# Databricks notebook source
# MAGIC %md
# MAGIC ## KAVACH — Graph Analytics (Pure PySpark)
# MAGIC
# MAGIC This notebook builds a **transaction network graph** from silver-layer UPI transactions
# MAGIC and computes structural features used for mule account detection:
# MAGIC
# MAGIC - **In-degree / Out-degree**: How many unique senders/receivers each account has
# MAGIC - **PageRank**: Identifies accounts that receive money from many distinct sources (mule hubs)
# MAGIC - **Community Detection**: Connected components finds clusters of tightly-connected accounts (scam rings)
# MAGIC - **Triangle Count**: Measures local clustering — mule networks form dense triangles
# MAGIC
# MAGIC **Implementation**: Pure PySpark (serverless-compatible, no GraphFrames dependency)
# MAGIC
# MAGIC **Output**: `gold_graph_features` and `gold_final_features` (ML-ready table joining account + graph features)

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

v_count = vertices.count()
e_count = edges.count()
print(f"Graph built: {v_count:,} vertices, {e_count:,} edges")
print("Using pure PySpark (serverless-compatible)")

# COMMAND ----------

# DBTITLE 1,In-Degree & Out-Degree
# In-degree: count unique senders TO each receiver
in_deg = (
    edges.groupBy(F.col("dst").alias("id"))
    .agg(F.count("*").alias("inDegree"))
)

# Out-degree: count unique receivers FROM each sender
out_deg = (
    edges.groupBy(F.col("src").alias("id"))
    .agg(F.count("*").alias("outDegree"))
)

print(f"Accounts with inbound edges:  {in_deg.count():,}")
print(f"Accounts with outbound edges: {out_deg.count():,}")

display(in_deg.orderBy(F.desc("inDegree")).limit(5))

# COMMAND ----------

# DBTITLE 1,PageRank
# Simplified PageRank using iterative joins (10 iterations)
# Formula: PR(node) = (1-d) + d * sum(PR(neighbor)/outDegree(neighbor))

damping = 0.85
max_iter = 10

# Initialize: all nodes start with rank = 1.0
pagerank_df = vertices.withColumn("pagerank", F.lit(1.0))

for i in range(max_iter):
    # Compute contribution each node sends to its neighbors
    contributions = (
        edges.alias("e")
        .join(pagerank_df.alias("pr"), F.col("e.src") == F.col("pr.id"))
        .join(out_deg.alias("od"), F.col("e.src") == F.col("od.id"))
        .select(
            F.col("e.dst").alias("id"),
            (F.col("pr.pagerank") / F.col("od.outDegree")).alias("contribution")
        )
    )
    
    # Sum contributions and apply damping
    new_ranks = (
        contributions.groupBy("id")
        .agg(F.sum("contribution").alias("sum_contrib"))
        .withColumn("pagerank", F.lit(1 - damping) + damping * F.col("sum_contrib"))
        .select("id", "pagerank")
    )
    
    # Merge back to all vertices (nodes with no inbound edges keep rank = 1-d)
    pagerank_df = (
        vertices
        .join(new_ranks, on="id", how="left")
        .withColumn("pagerank", F.coalesce(F.col("pagerank"), F.lit(1 - damping)))
        .select("id", "pagerank")
    )

print(f"PageRank computed ({max_iter} iterations).")
display(pagerank_df.orderBy(F.desc("pagerank")).limit(5))

# COMMAND ----------

# DBTITLE 1,Community Detection (Label Propagation)
# Connected Components: finds clusters of mutually reachable accounts
# More efficient than Label Propagation for serverless

# Create bidirectional edges for undirected graph
edges_undirected = (
    edges.select(F.col("src").alias("node1"), F.col("dst").alias("node2"))
    .union(
        edges.select(F.col("dst").alias("node1"), F.col("src").alias("node2"))
    )
).distinct()

# Initialize: each node is its own component
components = vertices.withColumn("label", F.col("id"))

# Iteratively propagate minimum label (simplified connected components)
max_iter = 10
for i in range(max_iter):
    # Each node adopts the minimum label of its neighbors
    neighbor_labels = (
        edges_undirected.alias("e")
        .join(components.alias("c"), F.col("e.node2") == F.col("c.id"))
        .groupBy(F.col("e.node1").alias("id"))
        .agg(F.min("c.label").alias("min_neighbor_label"))
    )
    
    components = (
        components.alias("comp")
        .join(neighbor_labels.alias("nl"), on="id", how="left")
        .withColumn(
            "label",
            F.least(
                F.col("comp.label"),
                F.coalesce(F.col("nl.min_neighbor_label"), F.col("comp.label"))
            )
        )
        .select("id", "label")
    )

# Compute community sizes
community_sizes = (
    components.groupBy("label")
    .agg(F.count("*").alias("community_size"))
)

community_df = (
    components
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
# Triangle count: A-B, B-C, C-A patterns
# Using self-joins to find cycles of length 3

# Find triangles: src→dst→third→src
triangles = (
    edges.alias("e1")
    .join(
        edges.alias("e2"),
        F.col("e1.dst") == F.col("e2.src")
    )
    .join(
        edges.alias("e3"),
        (F.col("e2.dst") == F.col("e3.src")) & (F.col("e3.dst") == F.col("e1.src"))
    )
    .select(
        F.least(F.col("e1.src"), F.col("e1.dst"), F.col("e2.dst")).alias("v1"),
        F.greatest(F.col("e1.src"), F.col("e1.dst"), F.col("e2.dst")).alias("v3"),
    )
    .distinct()
)

# Count triangles per vertex
# Each triangle appears 3 times (once per vertex), so count all occurrences
triangle_counts_v1 = triangles.groupBy(F.col("v1").alias("id")).agg(F.count("*").alias("count"))
triangle_counts_v3 = triangles.groupBy(F.col("v3").alias("id")).agg(F.count("*").alias("count"))

triangle_df = (
    triangle_counts_v1
    .union(triangle_counts_v3)
    .groupBy("id")
    .agg(F.sum("count").alias("triangle_count"))
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
