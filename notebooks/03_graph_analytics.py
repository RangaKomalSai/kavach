# Databricks notebook source
# MAGIC %md
# MAGIC ## KAVACH — Graph Analytics (GraphFrames)
# MAGIC 
# MAGIC This notebook builds a **transaction network graph** from silver-layer UPI transactions
# MAGIC and computes structural features used for mule account detection:
# MAGIC 
# MAGIC - **In-degree / Out-degree**: How many unique senders/receivers each account has
# MAGIC - **PageRank**: Identifies accounts that receive money from many distinct sources (mule hubs)
# MAGIC - **Community Detection**: Label Propagation finds clusters of tightly-connected accounts (scam rings)
# MAGIC - **Triangle Count**: Measures local clustering — mule networks form dense triangles
# MAGIC 
# MAGIC **Output**: `gold_graph_features` and `gold_final_features` (ML-ready table joining account + graph features)

# COMMAND ----------
# DBTITLE 1,Install GraphFrames

# MAGIC %pip install graphframes
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
# DBTITLE 1,Configuration & Setup

dbutils.widgets.text("catalog", "kavach", "Catalog Name")
dbutils.widgets.text("schema", "digital_arrest", "Schema Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print(f"Target: {catalog}.{schema}")

# COMMAND ----------
# DBTITLE 1,Build Transaction Graph

from graphframes import GraphFrame
from pyspark.sql import functions as F

# Read silver transactions
txns = spark.table(f"{catalog}.{schema}.silver_transactions")

# Vertices: all unique UPI IDs (union of senders and receivers)
senders = txns.select(F.col("sender_upi_id").alias("id")).distinct()
receivers = txns.select(F.col("receiver_upi_id").alias("id")).distinct()
vertices = senders.union(receivers).distinct()

# Edges: aggregated per sender→receiver pair
edges = (
    txns.groupBy(
        F.col("sender_upi_id").alias("src"),
        F.col("receiver_upi_id").alias("dst"),
    )
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("txn_count"),
    )
)

# Build GraphFrame
g = GraphFrame(vertices, edges)

v_count = vertices.count()
e_count = edges.count()
print(f"Graph built: {v_count:,} vertices, {e_count:,} edges")

# COMMAND ----------
# DBTITLE 1,In-Degree & Out-Degree

# In-degree: how many unique sources send money TO this account
in_deg = g.inDegrees  # columns: id, inDegree

# Out-degree: how many unique destinations this account sends money TO
out_deg = g.outDegrees  # columns: id, outDegree

print(f"Accounts with inbound edges:  {in_deg.count():,}")
print(f"Accounts with outbound edges: {out_deg.count():,}")

display(in_deg.orderBy(F.desc("inDegree")).limit(5))

# COMMAND ----------
# DBTITLE 1,PageRank

# PageRank identifies accounts that receive money from many well-connected sources
# Mule accounts and hub accounts tend to have high PageRank
pr = g.pageRank(resetProbability=0.15, maxIter=10)

pagerank_df = pr.vertices.select(
    F.col("id"),
    F.col("pagerank"),
)

print("PageRank computed.")
display(pagerank_df.orderBy(F.desc("pagerank")).limit(5))

# COMMAND ----------
# DBTITLE 1,Community Detection (Label Propagation)

# Label Propagation finds clusters of tightly-connected accounts
# Scam rings (victim → multiple mules) will cluster into small communities
communities = g.labelPropagation(maxIter=5)

# Compute community sizes
community_sizes = (
    communities.groupBy("label")
    .agg(F.count("*").alias("community_size"))
)

# Join community size back to each vertex
community_df = (
    communities.select("id", "label")
    .join(community_sizes, on="label", how="left")
    .select(
        F.col("id"),
        F.col("label").alias("community_label"),
        F.col("community_size"),
    )
)

print(f"Communities detected: {community_sizes.count():,}")
print("\nCommunity size distribution:")
display(
    community_sizes
    .groupBy("community_size")
    .agg(F.count("*").alias("num_communities"))
    .orderBy("community_size")
)

# COMMAND ----------
# DBTITLE 1,Triangle Count

# Triangle count measures local clustering density
# Mule networks form dense triangles (victim→mule1, victim→mule2, mule1→mule2)
tri = g.triangleCount()

triangle_df = tri.select(
    F.col("id"),
    F.col("count").alias("triangle_count"),
)

print("Triangle count computed.")
display(triangle_df.orderBy(F.desc("triangle_count")).limit(5))

# COMMAND ----------
# DBTITLE 1,Combine All Graph Features → gold_graph_features

# Start from all vertices and left-join each feature set
graph_features = (
    vertices
    .join(in_deg, on="id", how="left")
    .join(out_deg, on="id", how="left")
    .join(pagerank_df, on="id", how="left")
    .join(community_df, on="id", how="left")
    .join(triangle_df, on="id", how="left")
)

# Add derived feature: in/out ratio (high ratio = receives from many, sends to few → mule signal)
graph_features = graph_features.withColumn(
    "in_out_ratio",
    F.when(F.col("outDegree") > 0, F.col("inDegree") / F.col("outDegree"))
     .otherwise(F.lit(0.0))
)

# Fill nulls with 0 for accounts with no edges in one direction
graph_features = graph_features.fillna(0)

# Save to Unity Catalog
graph_features.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_graph_features")

spark.sql(f"""
  COMMENT ON TABLE {catalog}.{schema}.gold_graph_features IS
  'Graph-derived structural features for each UPI account. Includes PageRank, degree centrality, community membership, triangle count, and in/out ratio. Built from silver_transactions using GraphFrames.'
""")

row_count = spark.table(f"{catalog}.{schema}.gold_graph_features").count()
print(f"gold_graph_features saved: {row_count:,} rows")

# COMMAND ----------
# DBTITLE 1,Create ML-Ready Table → gold_final_features

# Join account-level features with graph features to create the final ML input table
account_features = spark.table(f"{catalog}.{schema}.gold_account_features")
graph_features_table = spark.table(f"{catalog}.{schema}.gold_graph_features")

final_features = (
    account_features
    .join(graph_features_table, account_features["account_id"] == graph_features_table["id"], "left")
    .drop("id")  # redundant after join
)

final_features = final_features.fillna(0)

# Save
final_features.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_final_features")

spark.sql(f"""
  COMMENT ON TABLE {catalog}.{schema}.gold_final_features IS
  'ML-ready feature table joining account-level transaction aggregates (gold_account_features) with graph structural features (gold_graph_features). One row per receiver account. Label column: is_mule. Used for GBT mule detection model training.'
""")

row_count = spark.table(f"{catalog}.{schema}.gold_final_features").count()
print(f"gold_final_features saved: {row_count:,} rows")
print(f"Feature columns: {len(final_features.columns)}")
print(f"Columns: {final_features.columns}")

# COMMAND ----------
# DBTITLE 1,Analysis: Insights for Judges

from pyspark.sql import functions as F

final = spark.table(f"{catalog}.{schema}.gold_final_features")

# ── 1. Top 10 Accounts by PageRank (expect mules at top) ──
print("═" * 60)
print("  TOP 10 ACCOUNTS BY PAGERANK")
print("═" * 60)
display(
    final.select(
        "account_id", "pagerank", "inDegree", "total_inbound_volume",
        "pct_round_amounts", "is_mule"
    )
    .orderBy(F.desc("pagerank"))
    .limit(10)
)

# ── 2. Suspicious Communities: size 3-8 with high round-amount % ──
print("═" * 60)
print("  SUSPICIOUS COMMUNITIES (size 3-8, >50% round amounts)")
print("═" * 60)
display(
    final.filter(
        (F.col("community_size") >= 3) &
        (F.col("community_size") <= 8) &
        (F.col("pct_round_amounts") > 0.5)
    )
    .groupBy("community_label", "community_size")
    .agg(
        F.count("*").alias("accounts_in_community"),
        F.sum("total_inbound_volume").alias("total_volume"),
        F.avg("pct_round_amounts").alias("avg_pct_round"),
        F.max("is_mule").alias("has_mule"),
    )
    .orderBy(F.desc("total_volume"))
    .limit(15)
)

# ── 3. Mule vs Legitimate: average in/out ratio ──
print("═" * 60)
print("  MULE vs LEGITIMATE: STRUCTURAL COMPARISON")
print("═" * 60)
display(
    final.groupBy("is_mule")
    .agg(
        F.count("*").alias("count"),
        F.avg("in_out_ratio").alias("avg_in_out_ratio"),
        F.avg("pagerank").alias("avg_pagerank"),
        F.avg("inDegree").alias("avg_inDegree"),
        F.avg("triangle_count").alias("avg_triangles"),
        F.avg("total_inbound_volume").alias("avg_inbound_volume"),
        F.avg("pct_round_amounts").alias("avg_pct_round"),
    )
)
