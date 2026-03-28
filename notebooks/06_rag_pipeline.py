# Databricks notebook source
# MAGIC %md
# MAGIC ## KAVACH — RAG Pipeline (Complaint Intelligence)
# MAGIC 
# MAGIC This notebook builds a **Retrieval-Augmented Generation** pipeline over complaint narratives
# MAGIC to enable semantic search across historical fraud cases:
# MAGIC 
# MAGIC 1. Load chunked complaints from `gold_complaints_chunked`
# MAGIC 2. Generate dense embeddings using `all-MiniLM-L6-v2` (384-dim)
# MAGIC 3. Build a FAISS Inner Product index for fast similarity search
# MAGIC 4. Save index + metadata to Unity Catalog Volume for serving
# MAGIC 5. Demo semantic queries for hackathon judges
# MAGIC 
# MAGIC **Use case**: An investigator types "CBI officer demanding money on video call" →
# MAGIC retrieves the 5 most similar past complaints with amounts, cities, and modus operandi.

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
# DBTITLE 1,Install Dependencies

# MAGIC %pip install sentence-transformers faiss-cpu
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
# DBTITLE 1,Reload Widgets After Restart

# Widgets need to be re-read after restartPython()
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------
# DBTITLE 1,Load Complaints

complaints = spark.table(f"{catalog}.{schema}.gold_complaints_chunked").toPandas()
print(f"{len(complaints)} complaints loaded")
print(f"Columns: {list(complaints.columns)}")
print(f"Avg narrative length: {complaints['narrative_length'].mean():.0f} chars")

# COMMAND ----------
# DBTITLE 1,Generate Embeddings (all-MiniLM-L6-v2)

from sentence_transformers import SentenceTransformer

model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
embeddings = model.encode(complaints["narrative"].tolist(), show_progress_bar=True)
print(f"Embeddings shape: {embeddings.shape}")

# COMMAND ----------
# DBTITLE 1,Build FAISS Index (Inner Product)

import faiss
import numpy as np

dimension = embeddings.shape[1]

# Normalize for cosine similarity via inner product
faiss.normalize_L2(embeddings)

# Build index
index = faiss.IndexFlatIP(dimension)
index.add(embeddings)

print(f"FAISS index built: {index.ntotal} vectors, dimension={dimension}")

# COMMAND ----------
# DBTITLE 1,Save Index & Metadata to Volume

import pickle

# Save FAISS index to local tmp, then copy to Volume
faiss.write_index(index, "/tmp/complaints.index")
dbutils.fs.cp(
    "file:/tmp/complaints.index",
    f"/Volumes/{catalog}/{schema}/data/vector_index/complaints.index"
)

# Save complaints DataFrame as pickle for metadata lookup
complaints.to_pickle("/tmp/complaints.pkl")
dbutils.fs.cp(
    "file:/tmp/complaints.pkl",
    f"/Volumes/{catalog}/{schema}/data/vector_index/complaints.pkl"
)

print(f"Saved to /Volumes/{catalog}/{schema}/data/vector_index/")
print(f"  complaints.index ({index.ntotal} vectors)")
print(f"  complaints.pkl   ({len(complaints)} rows)")

# COMMAND ----------
# DBTITLE 1,Search Function

def search_complaints(query, top_k=5):
    """Semantic search over complaint narratives using FAISS."""
    q = model.encode([query])
    faiss.normalize_L2(q)
    scores, indices = index.search(q, top_k)
    results = []
    for i, idx in enumerate(indices[0]):
        row = complaints.iloc[idx]
        results.append({
            "complaint_id": row["complaint_id"],
            "narrative": row["narrative"][:200],
            "similarity": round(float(scores[0][i]), 3),
            "amount_lost": row["amount_lost"],
            "city": row["victim_city"],
            "category": row["category"],
            "modus_operandi": row["modus_operandi"],
        })
    return results

print("search_complaints() ready.")

# COMMAND ----------
# DBTITLE 1,Demo: Semantic Search Queries

queries = [
    "CBI officer video call demanding money transfer",
    "ED officer threatened digital arrest via WhatsApp",
    "scammer asked to transfer money to multiple UPI accounts",
]

for q in queries:
    print(f"\n{'═' * 70}")
    print(f"  🔍 Query: {q}")
    print(f"{'═' * 70}")
    for r in search_complaints(q, top_k=3):
        print(f"  [{r['similarity']}] ₹{r['amount_lost']:,.0f} | {r['city']} | {r['category']}")
        print(f"           {r['narrative']}...")
        print()
