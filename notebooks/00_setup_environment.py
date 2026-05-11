# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup Environment
# MAGIC
# MAGIC Creates the three-layer database schema (bronze / silver / gold) and sets up
# MAGIC the shared configuration widget that every downstream notebook reads.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("env",     "dev",            "Environment (dev/prod)")

CATALOG = dbutils.widgets.get("catalog")
ENV     = dbutils.widgets.get("env")

BRONZE = f"{CATALOG}.insurance_bronze_{ENV}"
SILVER = f"{CATALOG}.insurance_silver_{ENV}"
GOLD   = f"{CATALOG}.insurance_gold_{ENV}"

# COMMAND ----------

for schema in [BRONZE, SILVER, GOLD]:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
    print(f"  schema ready: {schema}")

# COMMAND ----------

# Shared DBFS paths for raw CSV uploads
RAW_PATH = "/Volumes/workspace/insurance_analytics/source-files"
print(f"Raw data landing zone: {RAW_PATH}")

# COMMAND ----------

# Return values for job-task chaining
dbutils.notebook.exit({
    "catalog": CATALOG,
    "env":     ENV,
    "bronze":  BRONZE,
    "silver":  SILVER,
    "gold":    GOLD,
    "raw_path": RAW_PATH,
})
