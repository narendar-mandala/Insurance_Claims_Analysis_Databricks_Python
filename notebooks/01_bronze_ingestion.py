# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze Layer — Raw Ingestion
# MAGIC
# MAGIC Reads the four source CSV files from the raw landing zone and writes them as
# MAGIC Delta tables in the **bronze** schema.  No business logic here — only schema
# MAGIC inference and audit metadata columns are added.
# MAGIC
# MAGIC **Source files expected in** `dbfs:/FileStore/insurance_claims/{env}/raw/`
# MAGIC | File | Bronze table |
# MAGIC |------|-------------|
# MAGIC | Claims_v6.csv | claims_raw |
# MAGIC | Policy_Holders_v6.csv | policy_holders_raw |
# MAGIC | Adjusters_v6.csv | adjusters_raw |
# MAGIC | Insurance_Claims_Analytics_Power_BI.csv | analytics_export_raw |

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

dbutils.widgets.text("catalog", "hive_metastore", "Catalog")
dbutils.widgets.text("env",     "dev",            "Environment")

CATALOG  = dbutils.widgets.get("catalog")
ENV      = dbutils.widgets.get("env")
BRONZE   = f"{CATALOG}.insurance_bronze_{ENV}"
RAW_PATH = "/Volumes/workspace/insurance_analytics/source-files"

INGESTION_TS = datetime.utcnow().isoformat()

# COMMAND ----------

def read_csv(filename: str):
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .load(f"{RAW_PATH}/{filename}")
        .withColumn("_source_file",    F.lit(filename))
        .withColumn("_ingested_at",    F.lit(INGESTION_TS))
        .withColumn("_ingestion_date", F.current_date())
    )

def write_bronze(df, table: str, pk_col: str):
    full_table = f"{BRONZE}.{table}"
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.enableChangeDataFeed", "true")
        .saveAsTable(full_table)
    )
    count = spark.table(full_table).count()
    print(f"  [{table}] {count:,} rows written → {full_table}")
    return count

# COMMAND ----------

# MAGIC %md ## 1. Claims

# COMMAND ----------

claims_df = read_csv("Claims_v6.csv")
claims_count = write_bronze(claims_df, "claims_raw", "ClaimID")

# COMMAND ----------

# MAGIC %md ## 2. Policy Holders

# COMMAND ----------

policy_df = read_csv("Policy_Holders_v6.csv")
policy_count = write_bronze(policy_df, "policy_holders_raw", "PolicyID")

# COMMAND ----------

# MAGIC %md ## 3. Adjusters

# COMMAND ----------

adj_df = read_csv("Adjusters_v6.csv")
adj_count = write_bronze(adj_df, "adjusters_raw", "AdjusterID")

# COMMAND ----------

# MAGIC %md ## 4. Pre-joined Analytics Export (source-of-truth cross-check)

# COMMAND ----------

analytics_df = read_csv("Insurance_Claims_Analytics_Power_BI.csv")
analytics_count = write_bronze(analytics_df, "analytics_export_raw", "ClaimID")

# COMMAND ----------

# MAGIC %md ## Ingestion Summary

# COMMAND ----------

summary = {
    "claims_raw":          claims_count,
    "policy_holders_raw":  policy_count,
    "adjusters_raw":       adj_count,
    "analytics_export_raw": analytics_count,
    "ingested_at":         INGESTION_TS,
}

for k, v in summary.items():
    print(f"  {k}: {v}")

dbutils.notebook.exit(summary)
