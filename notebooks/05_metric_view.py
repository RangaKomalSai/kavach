# Databricks notebook source
# MAGIC %md
# MAGIC ## KAVACH — Metric View (AI/BI Genie-Ready)
# MAGIC 
# MAGIC This notebook creates a **YAML Metric View** that defines reusable dimensions and measures
# MAGIC for the KAVACH fraud detection platform. Once created, this metric view can be:
# MAGIC 
# MAGIC - Queried by **Genie Spaces** using natural language (e.g. "show me scam volume by hour")
# MAGIC - Used in **Lakeview Dashboards** for drag-and-drop visualizations
# MAGIC - Referenced in SQL via `SELECT * FROM kavach_fraud_metrics`
# MAGIC 
# MAGIC **Source tables**: `silver_transactions` ⟕ `gold_account_features` ⟕ `gold_graph_features`

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
# DBTITLE 1,Create Metric View

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW ${catalog}.${schema}.kavach_fraud_metrics
# MAGIC WITH METRICS
# MAGIC LANGUAGE YAML
# MAGIC AS $$
# MAGIC   version: 1.1
# MAGIC
# MAGIC   source: >
# MAGIC     SELECT
# MAGIC       t.txn_id,
# MAGIC       t.created_at,
# MAGIC       t.amount,
# MAGIC       t.sender_upi_id,
# MAGIC       t.receiver_upi_id,
# MAGIC       t.is_round_amount,
# MAGIC       t.is_scam_episode,
# MAGIC       COALESCE(a.total_inbound_txns, 0) AS total_inbound_txns,
# MAGIC       COALESCE(a.pct_round_amounts, 0.0) AS pct_round_amounts,
# MAGIC       COALESCE(a.unique_senders, 0) AS unique_senders,
# MAGIC       COALESCE(a.is_mule, 0) AS is_mule,
# MAGIC       COALESCE(g.pagerank, 0.0) AS pagerank,
# MAGIC       COALESCE(g.community_size, 0) AS community_size,
# MAGIC       COALESCE(g.in_out_ratio, 0.0) AS in_out_ratio
# MAGIC     FROM ${catalog}.${schema}.silver_transactions t
# MAGIC     LEFT JOIN ${catalog}.${schema}.gold_account_features a
# MAGIC       ON t.receiver_upi_id = a.account_id
# MAGIC     LEFT JOIN ${catalog}.${schema}.gold_graph_features g
# MAGIC       ON t.receiver_upi_id = g.id
# MAGIC
# MAGIC   comment: >-
# MAGIC     KAVACH fraud detection metrics for digital arrest scam analysis.
# MAGIC     Transactions enriched with account features and graph metrics.
# MAGIC
# MAGIC   dimensions:
# MAGIC     - name: Transaction Date
# MAGIC       expr: DATE(created_at)
# MAGIC       display_name: Transaction Date
# MAGIC       synonyms: [date, day]
# MAGIC     - name: Hour of Day
# MAGIC       expr: HOUR(created_at)
# MAGIC       display_name: Hour of Day
# MAGIC       synonyms: [hour, time]
# MAGIC     - name: Is Scam
# MAGIC       expr: CASE WHEN is_scam_episode THEN 'Scam' ELSE 'Normal' END
# MAGIC       display_name: Transaction Type
# MAGIC       synonyms: [scam, fraud, type]
# MAGIC     - name: Amount Bucket
# MAGIC       expr: >
# MAGIC         CASE WHEN amount < 1000 THEN 'Under 1K'
# MAGIC              WHEN amount < 10000 THEN '1K-10K'
# MAGIC              WHEN amount < 50000 THEN '10K-50K'
# MAGIC              ELSE '50K+' END
# MAGIC       display_name: Amount Range
# MAGIC       synonyms: [amount range, bucket]
# MAGIC
# MAGIC   measures:
# MAGIC     - name: Total Transactions
# MAGIC       expr: COUNT(1)
# MAGIC       display_name: Total Transactions
# MAGIC       synonyms: [count, total]
# MAGIC     - name: Total Volume
# MAGIC       expr: SUM(amount)
# MAGIC       display_name: Total Volume (₹)
# MAGIC       format: {type: number, decimal_places: {type: exact, places: 0}}
# MAGIC       synonyms: [total amount, volume]
# MAGIC     - name: Flagged Accounts
# MAGIC       expr: COUNT(DISTINCT CASE WHEN is_mule = 1 THEN receiver_upi_id END)
# MAGIC       display_name: Flagged Mule Accounts
# MAGIC       synonyms: [mules, flagged]
# MAGIC     - name: Avg Transaction Amount
# MAGIC       expr: AVG(amount)
# MAGIC       display_name: Avg Amount (₹)
# MAGIC       format: {type: number, decimal_places: {type: exact, places: 0}}
# MAGIC     - name: Detection Rate
# MAGIC       expr: >
# MAGIC         COUNT(DISTINCT CASE WHEN is_mule = 1 THEN receiver_upi_id END) /
# MAGIC         NULLIF(COUNT(DISTINCT receiver_upi_id), 0)
# MAGIC       display_name: Detection Rate
# MAGIC       format: {type: percentage, decimal_places: {type: exact, places: 1}}
# MAGIC $$

# COMMAND ----------
# DBTITLE 1,Verify Metric View

# MAGIC %sql
# MAGIC SELECT * FROM ${catalog}.${schema}.kavach_fraud_metrics LIMIT 5
