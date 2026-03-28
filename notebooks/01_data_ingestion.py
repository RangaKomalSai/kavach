# Databricks notebook source
# MAGIC %md
# MAGIC ## KAVACH — Data Ingestion Pipeline (Bronze Layer)
# MAGIC 
# MAGIC This notebook ingests UPI **transactions**, **accounts**, **calls**, and **complaints** 
# MAGIC from CSV files in a Unity Catalog Volume using **Auto Loader** for incremental processing.
# MAGIC 
# MAGIC **Architecture:**
# MAGIC - **Source**: CSV files in `/Volumes/{catalog}/{schema}/data/`
# MAGIC - **Transactions table**: ~100,000 UPI transactions with scam episode labels
# MAGIC - **Accounts table**: ~10,000 user/merchant accounts with mule account flags
# MAGIC - **Calls table**: ~10,000 phone/video calls linked to scam episodes
# MAGIC - **Complaints table**: ~1,000 consumer complaint narratives with modus operandi
# MAGIC - Auto Loader handles schema inference, incremental file discovery, and exactly-once processing
# MAGIC 
# MAGIC **Configuration**: Use the widget inputs at the top of this notebook to set your catalog and schema.

# COMMAND ----------
# DBTITLE 1,Configuration & Setup

# Widget setup — configure your catalog and schema
dbutils.widgets.text("catalog", "kavach", "Catalog Name")
dbutils.widgets.text("schema", "digital_arrest", "Schema Name")

# Get configuration from widgets
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_path = f"/Volumes/{catalog}/{schema}/data"
checkpoint_base = f"{source_path}/_checkpoints"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
spark.sql(f"USE SCHEMA {schema}")

print(f"Target: {catalog}.{schema}")
print(f"Source: {source_path}")
print(f"Checkpoints: {checkpoint_base}")

# COMMAND ----------
# DBTITLE 1,Ingest Transactions with Auto Loader

from pyspark.sql import functions as F
from pyspark.sql.types import *

# --- Transactions Ingestion ---
txn_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", f"{checkpoint_base}/transactions/schema")
    .option("pathGlobFilter", "*transactions*.csv")
    .load(source_path)
)

# Flatten and transform
txn_transformed = txn_stream.select(
    F.col("txn_id"),
    F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss").alias("created_at"),
    F.col("sender_upi_id"),
    F.col("receiver_upi_id"),
    F.col("amount").cast("double"),
    F.col("txn_type"),
    F.col("sender_bank"),
    F.col("receiver_bank"),
    F.col("sender_device_id"),
    F.col("receiver_device_id"),
    F.col("receiver_account_age_days").cast("int"),
    F.col("is_first_txn_for_receiver").cast("int"),
    F.col("is_scam_episode").cast("int"),
    F.col("scam_episode_id"),
    F.col("_metadata.file_path").alias("source_file"),
)

# Write to Delta
query_txns = (
    txn_transformed.writeStream
    .option("checkpointLocation", f"{checkpoint_base}/transactions/checkpoint")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"{catalog}.{schema}.transactions")
)

query_txns.awaitTermination()
print(f"Transactions ingested into {catalog}.{schema}.transactions")

# COMMAND ----------
# DBTITLE 1,Ingest Accounts with Auto Loader

from pyspark.sql import functions as F

# --- Accounts Ingestion ---
acct_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", f"{checkpoint_base}/accounts/schema")
    .option("pathGlobFilter", "*accounts*.csv")
    .load(source_path)
)

# Flatten and transform
acct_transformed = acct_stream.select(
    F.col("account_id"),
    F.to_date("creation_date", "yyyy-MM-dd").alias("creation_date"),
    F.col("account_type"),
    F.col("bank_name"),
    F.col("is_dormant").cast("boolean"),
    F.col("days_since_last_activity").cast("int"),
    F.col("is_mule").cast("boolean"),
    F.col("_metadata.file_path").alias("source_file"),
)

# Write to Delta
query_accts = (
    acct_transformed.writeStream
    .option("checkpointLocation", f"{checkpoint_base}/accounts/checkpoint")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"{catalog}.{schema}.accounts")
)

query_accts.awaitTermination()
print(f"Accounts ingested into {catalog}.{schema}.accounts")

# COMMAND ----------
# DBTITLE 1,Ingest Calls with Auto Loader

from pyspark.sql import functions as F

# --- Calls Ingestion ---
calls_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", f"{checkpoint_base}/calls/schema")
    .option("pathGlobFilter", "*calls*.csv")
    .load(source_path)
)

# Flatten and transform
calls_transformed = calls_stream.select(
    F.col("call_id"),
    F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss").alias("created_at"),
    F.col("caller_number"),
    F.col("callee_number"),
    F.col("duration_seconds").cast("int"),
    F.col("call_type"),
    F.col("is_international").cast("boolean"),
    F.col("_metadata.file_path").alias("source_file"),
)

# Write to Delta
query_calls = (
    calls_transformed.writeStream
    .option("checkpointLocation", f"{checkpoint_base}/calls/checkpoint")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"{catalog}.{schema}.calls")
)

query_calls.awaitTermination()
print(f"Calls ingested into {catalog}.{schema}.calls")

# COMMAND ----------
# DBTITLE 1,Ingest Complaints with Auto Loader

from pyspark.sql import functions as F

# --- Complaints Ingestion ---
# Complaints CSV has a 'narrative' column with commas inside; handle with multiLine + escape
complaints_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .option("multiLine", "true")
    .option("escape", '"')
    .option("cloudFiles.schemaLocation", f"{checkpoint_base}/complaints/schema")
    .option("pathGlobFilter", "*complaints*.csv")
    .load(source_path)
)

# Flatten and transform
complaints_transformed = complaints_stream.select(
    F.col("complaint_id"),
    F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss").alias("created_at"),
    F.col("category"),
    F.col("narrative"),
    F.col("amount_lost").cast("double"),
    F.col("accused_upi_ids"),
    F.col("victim_city"),
    F.col("victim_age").cast("int"),
    F.col("modus_operandi"),
    F.col("status"),
    F.col("_metadata.file_path").alias("source_file"),
)

# Write to Delta
query_comp = (
    complaints_transformed.writeStream
    .option("checkpointLocation", f"{checkpoint_base}/complaints/checkpoint")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"{catalog}.{schema}.complaints")
)

query_comp.awaitTermination()
print(f"Complaints ingested into {catalog}.{schema}.complaints")

# COMMAND ----------
# DBTITLE 1,Establish PK-FK Constraints

# PK columns must be NOT NULL — set nullability first, then add constraints
for tbl, col in [
    ("transactions", "txn_id"),
    ("accounts", "account_id"),
    ("calls", "call_id"),
    ("complaints", "complaint_id"),
]:
    spark.sql(f"ALTER TABLE {catalog}.{schema}.{tbl} ALTER COLUMN {col} SET NOT NULL")

# Primary Keys
spark.sql(f"""
  ALTER TABLE {catalog}.{schema}.transactions
  ADD CONSTRAINT transactions_pk PRIMARY KEY (txn_id)
""")

spark.sql(f"""
  ALTER TABLE {catalog}.{schema}.accounts
  ADD CONSTRAINT accounts_pk PRIMARY KEY (account_id)
""")

spark.sql(f"""
  ALTER TABLE {catalog}.{schema}.calls
  ADD CONSTRAINT calls_pk PRIMARY KEY (call_id)
""")

spark.sql(f"""
  ALTER TABLE {catalog}.{schema}.complaints
  ADD CONSTRAINT complaints_pk PRIMARY KEY (complaint_id)
""")

print("PK constraints added successfully")

# COMMAND ----------
# DBTITLE 1,Enable Change Data Feed

# Enable CDF on all tables for incremental downstream reads via table_changes()
for tbl in ["transactions", "accounts", "calls", "complaints"]:
    spark.sql(f"ALTER TABLE {catalog}.{schema}.{tbl} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    print(f"Change Data Feed enabled on {catalog}.{schema}.{tbl}")

# COMMAND ----------
# DBTITLE 1,Add Table & Column Comments

# ── Table-level comments ──
spark.sql(f"""
  COMMENT ON TABLE {catalog}.{schema}.transactions IS
  'UPI transactions for digital arrest scam detection. Contains ~100K rows with scam episode labels. Each row is one UPI payment (P2P, P2M, collect, or autopay). Primary key: txn_id. Ingested incrementally via Auto Loader from CSV files.'
""")

spark.sql(f"""
  COMMENT ON TABLE {catalog}.{schema}.accounts IS
  'User and merchant bank accounts. Contains ~10K rows with ~3% flagged as mule accounts. Includes account age, dormancy status, and type. Primary key: account_id.'
""")

spark.sql(f"""
  COMMENT ON TABLE {catalog}.{schema}.calls IS
  'Phone and video call records. Contains ~10K rows. Scam-linked calls have durations of 30-120 minutes via video/WhatsApp, preceding fraudulent transactions. Primary key: call_id.'
""")

spark.sql(f"""
  COMMENT ON TABLE {catalog}.{schema}.complaints IS
  'Consumer complaint narratives filed by scam victims. Contains ~1K rows with 40% digital arrest scams and 60% other UPI fraud types. Narratives are in English with realistic detail. Primary key: complaint_id.'
""")

# ── Transactions column comments ──
txn_column_comments = {
    "txn_id":                     "Unique UPI transaction identifier (e.g. UPI842301548721). Primary key.",
    "created_at":                 "Timestamp (IST) when the transaction was initiated. Parsed from yyyy-MM-dd HH:mm:ss format.",
    "sender_upi_id":              "UPI Virtual Payment Address of the sender (e.g. rahulsharma42@sbi).",
    "receiver_upi_id":            "UPI Virtual Payment Address of the receiver/beneficiary.",
    "amount":                     "Transaction amount in Indian Rupees (INR). Scam transactions typically use round amounts from {25000, 50000, 100000, 200000, 500000}. Normal transactions follow a log-normal distribution centred at Rs 1800.",
    "txn_type":                   "Type of UPI transaction. Values: P2P (person-to-person), P2M (person-to-merchant), collect (collect request), autopay (standing instruction).",
    "sender_bank":                "Name of the sender bank (e.g. State Bank of India, HDFC Bank).",
    "receiver_bank":              "Name of the receiver bank.",
    "sender_device_id":           "16-character hex device fingerprint of the sender phone used for the transaction.",
    "receiver_device_id":         "16-character hex device fingerprint of the receiver phone.",
    "receiver_account_age_days":  "Age of the receiver account in days at the time of transaction. Mule accounts are typically 1-30 days old.",
    "is_first_txn_for_receiver":  "1 if this is the first transaction ever received by this receiver UPI ID in the scam episode, 0 otherwise.",
    "is_scam_episode":            "Binary label. 1 = transaction is part of a digital arrest scam episode, 0 = legitimate transaction.",
    "scam_episode_id":            "Identifier linking transactions to the same scam episode (e.g. EP0042). Empty string for legitimate transactions.",
    "source_file":                "Auto Loader metadata: full path of the source CSV file this record was ingested from.",
}

for col, comment in txn_column_comments.items():
    safe = comment.replace("'", "\\'")
    spark.sql(f"ALTER TABLE {catalog}.{schema}.transactions ALTER COLUMN {col} COMMENT '{safe}'")

# ── Accounts column comments ──
acct_column_comments = {
    "account_id":                 "Unique account identifier (e.g. ACC004521). Primary key.",
    "creation_date":              "Date when the bank account was originally created.",
    "account_type":               "Account classification. Values: personal or business.",
    "bank_name":                  "Name of the issuing bank (e.g. State Bank of India, HDFC Bank).",
    "is_dormant":                 "True if the account was inactive for an extended period. 30% of mule accounts are reactivated dormant accounts.",
    "days_since_last_activity":   "Number of days since the last transaction on this account. Mule accounts typically show 0-5 days (new) or 90-400 days (reactivated).",
    "is_mule":                    "True if this account is flagged as a mule/money laundering intermediary. ~3% of all accounts.",
    "source_file":                "Auto Loader metadata: full path of the source CSV file this record was ingested from.",
}

for col, comment in acct_column_comments.items():
    safe = comment.replace("'", "\\'")
    spark.sql(f"ALTER TABLE {catalog}.{schema}.accounts ALTER COLUMN {col} COMMENT '{safe}'")

# ── Calls column comments ──
calls_column_comments = {
    "call_id":             "Unique call record identifier (e.g. CALL001234). Primary key.",
    "created_at":          "Timestamp (IST) when the call was initiated.",
    "caller_number":       "Indian mobile number of the caller in +91XXXXXXXXXX format.",
    "callee_number":       "Indian mobile number of the person being called.",
    "duration_seconds":    "Duration of the call in seconds. Scam-linked calls are 1800-7200 seconds (30-120 minutes). Normal calls are 30-300 seconds.",
    "call_type":           "Type of call. Values: voice, video, whatsapp_video.",
    "is_international":    "True if the call originated from an international number. ~15% of scam calls are international.",
    "source_file":         "Auto Loader metadata: full path of the source CSV file this record was ingested from.",
}

for col, comment in calls_column_comments.items():
    safe = comment.replace("'", "\\'")
    spark.sql(f"ALTER TABLE {catalog}.{schema}.calls ALTER COLUMN {col} COMMENT '{safe}'")

# ── Complaints column comments ──
comp_column_comments = {
    "complaint_id":       "Unique complaint identifier (e.g. CMP00042). Primary key.",
    "created_at":         "Timestamp when the complaint was filed.",
    "category":           "Fraud category. Values: Digital Arrest Scam, KYC Fraud, QR Code Scam, Lottery/Prize Scam, Online Marketplace Fraud, Refund Fraud, Investment Fraud, Loan Fraud, Job Fraud, Impersonation Fraud.",
    "narrative":          "Free-text complaint narrative in English (100-300 words). Contains victim account of the fraud incident, written in a mix of formal and informal language.",
    "amount_lost":        "Total amount lost by the victim in Indian Rupees (INR).",
    "accused_upi_ids":    "Semicolon-separated list of UPI IDs of accused parties (mule accounts).",
    "victim_city":        "City of residence of the victim (e.g. Mumbai, Delhi, Bangalore).",
    "victim_age":         "Age of the victim at time of filing.",
    "modus_operandi":     "Machine-readable tag for the fraud method. Values: digital_arrest, fake_kyc_update, qr_code_scam, lottery_scam, olx_marketplace_fraud, fake_refund, investment_scam, loan_fraud, job_scam, impersonation_scam.",
    "status":             "Complaint processing status. Values: registered, under_investigation, resolved, escalated, closed.",
    "source_file":        "Auto Loader metadata: full path of the source CSV file this record was ingested from.",
}

for col, comment in comp_column_comments.items():
    safe = comment.replace("'", "\\'")
    spark.sql(f"ALTER TABLE {catalog}.{schema}.complaints ALTER COLUMN {col} COMMENT '{safe}'")

print(f"Table and column comments added for all 4 tables")

# COMMAND ----------
# DBTITLE 1,Verify Ingested Data

from pyspark.sql import functions as F

# Row counts
txn_count = spark.table(f"{catalog}.{schema}.transactions").count()
acct_count = spark.table(f"{catalog}.{schema}.accounts").count()
calls_count = spark.table(f"{catalog}.{schema}.calls").count()
comp_count = spark.table(f"{catalog}.{schema}.complaints").count()

print(f"Transactions: {txn_count:>10,} rows")
print(f"Accounts:     {acct_count:>10,} rows")
print(f"Calls:        {calls_count:>10,} rows")
print(f"Complaints:   {comp_count:>10,} rows")

scam_count = spark.table(f"{catalog}.{schema}.transactions").filter(F.col("is_scam_episode") == 1).count()
mule_count = spark.table(f"{catalog}.{schema}.accounts").filter(F.col("is_mule") == True).count()
print(f"\nScam transactions: {scam_count:,} ({scam_count/max(txn_count,1)*100:.1f}%)")
print(f"Mule accounts:     {mule_count:,} ({mule_count/max(acct_count,1)*100:.1f}%)")

# Show top 5 highest-value transactions
print("\n--- Top 5 Transactions by Amount ---")
display(
    spark.table(f"{catalog}.{schema}.transactions")
    .select("txn_id", "created_at", "sender_upi_id", "receiver_upi_id", "amount", "is_scam_episode", "scam_episode_id")
    .orderBy(F.desc("amount"))
    .limit(5)
)

# Show sample complaints
print("\n--- Sample Complaints ---")
display(
    spark.table(f"{catalog}.{schema}.complaints")
    .select("complaint_id", "category", "amount_lost", "victim_city", "modus_operandi")
    .orderBy(F.desc("amount_lost"))
    .limit(5)
)
