# Databricks notebook source
# MAGIC %md
# MAGIC ## KAVACH — Mule Detection Model Training (MLlib + MLflow)
# MAGIC 
# MAGIC This notebook trains **three classification models** to detect mule accounts
# MAGIC using the combined account + graph features from the gold layer:
# MAGIC 
# MAGIC | Model | Algorithm | Why |
# MAGIC |-------|-----------|-----|
# MAGIC | **GBT** | Gradient Boosted Trees | Handles imbalanced data well, captures non-linear interactions |
# MAGIC | **RF** | Random Forest | Robust ensemble baseline, less prone to overfitting |
# MAGIC | **LR** | Logistic Regression | Interpretable linear baseline for comparison |
# MAGIC 
# MAGIC **Input**: `gold_final_features` (account-level features + graph features, label = `is_mule`)  
# MAGIC **Output**: Best model saved to Unity Catalog Volume, all runs tracked in MLflow

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
# DBTITLE 1,Load Data & Class Distribution

from pyspark.sql import functions as F

df = spark.table(f"{catalog}.{schema}.gold_final_features")

total = df.count()
mule_count = df.filter(F.col("is_mule") == 1).count()
legit_count = total - mule_count
ratio = mule_count / max(legit_count, 1)

print(f"Total accounts:     {total:,}")
print(f"Mule accounts:      {mule_count:,}  ({mule_count/total*100:.1f}%)")
print(f"Legitimate accounts: {legit_count:,} ({legit_count/total*100:.1f}%)")
print(f"Class ratio (mule/legit): {ratio:.4f}")

display(df.groupBy("is_mule").count().orderBy("is_mule"))

# COMMAND ----------
# DBTITLE 1,Feature Assembly (VectorAssembler + StandardScaler)

from pyspark.ml.feature import VectorAssembler, StandardScaler

feature_columns = [
    "total_inbound_txns",
    "total_inbound_volume",
    "avg_inbound_amount",
    "max_inbound_amount",
    "pct_round_amounts",
    "account_age_at_first_inbound",
    "unique_senders",
    "active_days",
    "activity_span_hours",
    "inDegree",
    "outDegree",
    "pagerank",
    "community_size",
    "triangle_count",
    "in_out_ratio",
]

# Cast label to double (required by MLlib classifiers)
df = df.withColumn("label", F.col("is_mule").cast("double"))

# Fill any remaining nulls in feature columns
df = df.fillna(0, subset=feature_columns)

assembler = VectorAssembler(
    inputCols=feature_columns,
    outputCol="raw_features",
    handleInvalid="skip",
)

scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="features",
    withStd=True,
    withMean=True,
)

print(f"Feature columns ({len(feature_columns)}): {feature_columns}")

# COMMAND ----------
# DBTITLE 1,Train / Test Split (80/20)

train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

print(f"Training set: {train_df.count():,} rows  (mule: {train_df.filter(F.col('label')==1).count():,})")
print(f"Test set:     {test_df.count():,} rows  (mule: {test_df.filter(F.col('label')==1).count():,})")

# COMMAND ----------
# DBTITLE 1,MLflow Experiment Setup

import mlflow
import mlflow.spark

experiment_path = f"/Users/{spark.sql('SELECT current_user()').collect()[0][0]}/kavach-mule-detection"
mlflow.set_experiment(experiment_path)

print(f"MLflow experiment: {experiment_path}")

# COMMAND ----------
# DBTITLE 1,Helper: Evaluate & Log Model

from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

def evaluate_and_log(model_name, pipeline, train_df, test_df, params):
    """Fit pipeline, evaluate on test set, log everything to MLflow."""
    
    with mlflow.start_run(run_name=model_name):
        # Log parameters
        for k, v in params.items():
            mlflow.log_param(k, v)
        mlflow.log_param("num_features", len(feature_columns))
        mlflow.log_param("feature_columns", str(feature_columns))
        
        # Fit
        fitted_pipeline = pipeline.fit(train_df)
        predictions = fitted_pipeline.transform(test_df)
        
        # AUC
        auc_eval = BinaryClassificationEvaluator(
            labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
        )
        auc = auc_eval.evaluate(predictions)
        
        # F1
        f1_eval = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName="f1"
        )
        f1 = f1_eval.evaluate(predictions)
        
        # Precision
        prec_eval = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName="weightedPrecision"
        )
        precision = prec_eval.evaluate(predictions)
        
        # Recall
        rec_eval = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName="weightedRecall"
        )
        recall = rec_eval.evaluate(predictions)
        
        # Log metrics
        mlflow.log_metric("AUC", auc)
        mlflow.log_metric("F1", f1)
        mlflow.log_metric("Precision", precision)
        mlflow.log_metric("Recall", recall)
        
        # Log model
        mlflow.spark.log_model(fitted_pipeline, artifact_path="model")
        
        print(f"  {model_name}: AUC={auc:.4f}  F1={f1:.4f}  Precision={precision:.4f}  Recall={recall:.4f}")
        
        return fitted_pipeline, {"model": model_name, "AUC": auc, "F1": f1, "Precision": precision, "Recall": recall}

# COMMAND ----------
# DBTITLE 1,Model 1: Gradient Boosted Trees (GBT)

from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier

gbt = GBTClassifier(
    labelCol="label",
    featuresCol="features",
    maxIter=50,
    maxDepth=5,
    seed=42,
)

gbt_pipeline = Pipeline(stages=[assembler, scaler, gbt])

gbt_params = {"algorithm": "GBT", "maxIter": 50, "maxDepth": 5}
gbt_fitted, gbt_metrics = evaluate_and_log("GBT_Classifier", gbt_pipeline, train_df, test_df, gbt_params)

# COMMAND ----------
# DBTITLE 1,Model 2: Random Forest

from pyspark.ml.classification import RandomForestClassifier

rf = RandomForestClassifier(
    labelCol="label",
    featuresCol="features",
    numTrees=100,
    maxDepth=5,
    seed=42,
)

rf_pipeline = Pipeline(stages=[assembler, scaler, rf])

rf_params = {"algorithm": "RandomForest", "numTrees": 100, "maxDepth": 5}
rf_fitted, rf_metrics = evaluate_and_log("RF_Classifier", rf_pipeline, train_df, test_df, rf_params)

# COMMAND ----------
# DBTITLE 1,Model 3: Logistic Regression

from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression(
    labelCol="label",
    featuresCol="features",
    maxIter=100,
)

lr_pipeline = Pipeline(stages=[assembler, scaler, lr])

lr_params = {"algorithm": "LogisticRegression", "maxIter": 100}
lr_fitted, lr_metrics = evaluate_and_log("LR_Classifier", lr_pipeline, train_df, test_df, lr_params)

# COMMAND ----------
# DBTITLE 1,Model Comparison

import pandas as pd

comparison = pd.DataFrame([gbt_metrics, rf_metrics, lr_metrics])
comparison = comparison.sort_values("AUC", ascending=False).reset_index(drop=True)

print("═" * 70)
print("  MODEL COMPARISON — Mule Account Detection")
print("═" * 70)
display(spark.createDataFrame(comparison))

# Identify best model
best = comparison.iloc[0]
print(f"\n★ Best model: {best['model']} (AUC={best['AUC']:.4f}, F1={best['F1']:.4f})")

# COMMAND ----------
# DBTITLE 1,Save Best Model (GBT) to Volume

model_path = f"/Volumes/{catalog}/{schema}/data/models/gbt_best"

gbt_fitted.write().overwrite().save(model_path)

print(f"Best model saved to: {model_path}")

# COMMAND ----------
# DBTITLE 1,Feature Importances (GBT)

import pandas as pd

# Extract the GBT model from the pipeline (last stage)
gbt_model = gbt_fitted.stages[-1]
importances = gbt_model.featureImportances.toArray()

# Build feature importance DataFrame
fi_data = sorted(
    zip(feature_columns, importances),
    key=lambda x: x[1],
    reverse=True,
)
fi_df = pd.DataFrame(fi_data, columns=["feature_name", "importance"])
fi_df["importance_pct"] = (fi_df["importance"] * 100).round(2)

print("═" * 60)
print("  GBT FEATURE IMPORTANCES")
print("═" * 60)
display(spark.createDataFrame(fi_df))
