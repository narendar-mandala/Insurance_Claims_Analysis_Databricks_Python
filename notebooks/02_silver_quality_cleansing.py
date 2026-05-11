# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver Layer — Data Quality & Cleansing
# MAGIC
# MAGIC This notebook reads from the **bronze** Delta tables, applies a comprehensive
# MAGIC suite of data-quality checks, resolves inconsistencies, and writes clean,
# MAGIC typed records to the **silver** layer.
# MAGIC
# MAGIC ### Quality checks performed
# MAGIC | # | Check | Resolution |
# MAGIC |---|-------|-----------|
# MAGIC | 1 | Null / blank mandatory fields | Flag; exclude from silver |
# MAGIC | 2 | Duplicate ClaimIDs | Keep latest by ingestion date |
# MAGIC | 3 | Claim date < policy start date | Flag as `dq_claim_before_policy` |
# MAGIC | 4 | Invalid AdjusterID (no adjuster record) | Flag as `dq_invalid_adjuster` |
# MAGIC | 5 | Invalid PolicyID (no policy record) | Flag as `dq_orphan_claim` |
# MAGIC | 6 | Negative or zero claim/premium amounts | Flag as `dq_invalid_amount` |
# MAGIC | 7 | Unrecognised ClaimStatus / ClaimType values | Flag as `dq_bad_enum` |
# MAGIC | 8 | Customer/Adjuster name normalisation | Strip professional titles |
# MAGIC | 9 | Claim amount > 3× median for same type | Flag as `dq_outlier_amount` |
# MAGIC | 10 | Future-dated claims (ClaimDate > today) | Flag as `dq_future_date` |

# COMMAND ----------

from pyspark.sql import functions as F, Window
from pyspark.sql.types import DoubleType, DateType, IntegerType
import re

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("env",     "dev",            "Environment")

CATALOG = dbutils.widgets.get("catalog")
ENV     = dbutils.widgets.get("env")
BRONZE  = f"{CATALOG}.insurance_bronze_{ENV}"
SILVER  = f"{CATALOG}.insurance_silver_{ENV}"

VALID_STATUSES = {"Paid", "In Review", "Closed", "Denied", "Open"}
VALID_TYPES    = {"Liability", "Theft", "Medical", "Fire", "Accident"}
VALID_POLICIES = {"Life", "Home", "Auto", "Health"}

# COMMAND ----------

# MAGIC %md ## Helper utilities

# COMMAND ----------

# Strips common professional titles from name strings
@F.udf("string")
def clean_name(name):
    if name is None:
        return None
    prefixes = r"^(Dr\.?|Mrs\.?|Mr\.?|Ms\.?|Prof\.?|Rev\.?)\s+"
    suffixes = r"\s+(Jr\.?|Sr\.?|II|III|IV|MD|PhD|DDS|MBA|Esq\.?)$"
    name = re.sub(prefixes, "", name.strip(), flags=re.IGNORECASE)
    name = re.sub(suffixes, "", name.strip(), flags=re.IGNORECASE)
    return name.strip()

def add_dq_flag(df, flag_col: str, condition):
    if flag_col not in df.columns:
        df = df.withColumn(flag_col, F.lit(False))
    return df.withColumn(flag_col, F.when(condition, F.lit(True)).otherwise(F.col(flag_col)))

# COMMAND ----------

# MAGIC %md ## 1. Load bronze tables

# COMMAND ----------

claims_raw    = spark.table(f"{BRONZE}.claims_raw")
policies_raw  = spark.table(f"{BRONZE}.policy_holders_raw")
adjusters_raw = spark.table(f"{BRONZE}.adjusters_raw")

# COMMAND ----------

# MAGIC %md ## 2. Clean Policy Holders

# COMMAND ----------

policies_silver = (
    policies_raw
    .withColumn("CustomerName",   clean_name(F.col("CustomerName")))
    .withColumn("PremiumAmount",  F.col("PremiumAmount").cast(DoubleType()))
    .withColumn("StartDate",      F.col("StartDate").cast(DateType()))
    .withColumn("PolicyType",     F.trim(F.col("PolicyType")))
    .withColumn("dq_invalid_premium",
                F.col("PremiumAmount").isNull() | (F.col("PremiumAmount") <= 0))
    .withColumn("dq_bad_policy_type",
                ~F.col("PolicyType").isin(list(VALID_POLICIES)))
    .withColumn("dq_null_fields",
                F.col("PolicyID").isNull() | F.col("CustomerName").isNull() | F.col("StartDate").isNull())
    .withColumn("_silver_processed_at", F.current_timestamp())
)

(
    policies_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER}.policy_holders_clean")
)
print(f"policy_holders_clean: {policies_silver.count():,} rows")

# COMMAND ----------

# MAGIC %md ## 3. Clean Adjusters

# COMMAND ----------

adjusters_silver = (
    adjusters_raw
    .withColumn("AdjusterName", clean_name(F.col("AdjusterName")))
    .withColumn("Region",       F.trim(F.initcap(F.col("Region"))))
    .withColumn("dq_null_fields",
                F.col("AdjusterID").isNull() | F.col("AdjusterName").isNull())
    .withColumn("_silver_processed_at", F.current_timestamp())
)

(
    adjusters_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER}.adjusters_clean")
)
print(f"adjusters_clean: {adjusters_silver.count():,} rows")

# COMMAND ----------

# MAGIC %md ## 4. Clean Claims — multi-pass DQ

# COMMAND ----------

claims = (
    claims_raw
    .withColumn("ClaimAmount", F.col("ClaimAmount").cast(DoubleType()))
    .withColumn("ClaimDate",   F.col("ClaimDate").cast(DateType()))
    .withColumn("ClaimStatus", F.trim(F.col("ClaimStatus")))
    .withColumn("ClaimType",   F.trim(F.col("ClaimType")))
    .withColumn("PolicyID",    F.col("PolicyID").cast(IntegerType()))
    .withColumn("AdjusterID",  F.col("AdjusterID").cast(IntegerType()))
)

# --- Check 1: Nulls in mandatory columns ---
claims = add_dq_flag(
    claims, "dq_null_fields",
    F.col("ClaimID").isNull() | F.col("PolicyID").isNull() |
    F.col("ClaimAmount").isNull() | F.col("ClaimDate").isNull()
)

# --- Check 2: Duplicate ClaimIDs (keep latest) ---
w_dedup = Window.partitionBy("ClaimID").orderBy(F.col("_ingested_at").desc())
claims = (
    claims
    .withColumn("_row_rank", F.row_number().over(w_dedup))
    .withColumn("dq_duplicate", F.col("_row_rank") > 1)
    .drop("_row_rank")
)

# --- Check 3: Orphan PolicyID (join-based — serverless compatible) ---
valid_pid_df = (
    policies_raw
    .select(F.col("PolicyID").cast(IntegerType()).alias("_valid_pid"))
    .distinct()
)
claims = (
    claims
    .join(valid_pid_df, claims.PolicyID == valid_pid_df._valid_pid, "left")
    .withColumn("dq_orphan_claim", F.col("_valid_pid").isNull())
    .drop("_valid_pid")
)

# --- Check 4: Invalid AdjusterID (join-based — serverless compatible) ---
valid_aid_df = (
    adjusters_raw
    .select(F.col("AdjusterID").cast(IntegerType()).alias("_valid_aid"))
    .distinct()
)
claims = (
    claims
    .join(valid_aid_df, claims.AdjusterID == valid_aid_df._valid_aid, "left")
    .withColumn("dq_invalid_adjuster", F.col("_valid_aid").isNull())
    .drop("_valid_aid")
)

# --- Check 5: Enum validation ---
claims = add_dq_flag(
    claims, "dq_bad_enum",
    ~F.col("ClaimStatus").isin(list(VALID_STATUSES)) |
    ~F.col("ClaimType").isin(list(VALID_TYPES))
)

# --- Check 6: Non-positive amounts ---
claims = add_dq_flag(
    claims, "dq_invalid_amount",
    F.col("ClaimAmount").isNull() | (F.col("ClaimAmount") <= 0)
)

# --- Check 7: Future-dated claims ---
claims = add_dq_flag(
    claims, "dq_future_date",
    F.col("ClaimDate") > F.current_date()
)

# --- Check 8: Claim before policy start date ---
policy_start_map = (
    policies_raw
    .select(
        F.col("PolicyID").cast(IntegerType()).alias("pid"),
        F.col("StartDate").cast(DateType()).alias("policy_start")
    )
)

claims = (
    claims
    .join(policy_start_map, claims.PolicyID == policy_start_map.pid, "left")
    .withColumn(
        "dq_claim_before_policy",
        F.col("policy_start").isNotNull() & (F.col("ClaimDate") < F.col("policy_start"))
    )
    .drop("pid", "policy_start")
)

# --- Check 9: Outlier amounts (> 3× median per claim type) ---
median_by_type = (
    claims
    .groupBy("ClaimType")
    .agg(F.percentile_approx("ClaimAmount", 0.5).alias("median_amount"))
)

claims = (
    claims
    .join(median_by_type, "ClaimType", "left")
    .withColumn("dq_outlier_amount", F.col("ClaimAmount") > 3 * F.col("median_amount"))
    .drop("median_amount")
)

# --- Composite DQ flag ---
dq_cols = [c for c in claims.columns if c.startswith("dq_")]
claims = claims.withColumn(
    "dq_has_any_issue",
    F.greatest(*[F.col(c).cast("boolean") for c in dq_cols])
)

claims = claims.withColumn("_silver_processed_at", F.current_timestamp())

# COMMAND ----------

# MAGIC %md ## 5. Write silver claims (all rows — DQ flags preserved)

# COMMAND ----------

(
    claims.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER}.claims_clean")
)

total     = claims.count()
dq_issues = claims.filter(F.col("dq_has_any_issue")).count()
clean     = total - dq_issues

print(f"claims_clean written: {total:,} total | {clean:,} clean | {dq_issues:,} with DQ flags")

# COMMAND ----------

# MAGIC %md ## 6. DQ Summary Report

# COMMAND ----------

print("\n=== Data Quality Issue Breakdown ===")
for col in dq_cols:
    cnt = claims.filter(F.col(col)).count()
    pct = 100 * cnt / total if total else 0
    print(f"  {col:<35} {cnt:>6,}  ({pct:.1f}%)")

# COMMAND ----------

# MAGIC %md ## 7. Quarantine table — rows with critical issues

# COMMAND ----------

critical_flags = ["dq_null_fields", "dq_orphan_claim", "dq_invalid_adjuster", "dq_bad_enum"]

quarantine = claims.filter(
    F.greatest(*[F.col(c).cast("boolean") for c in critical_flags])
)

(
    quarantine.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER}.claims_quarantine")
)
print(f"claims_quarantine: {quarantine.count():,} rows flagged for review")

# COMMAND ----------

dbutils.notebook.exit({
    "total_claims":   total,
    "clean_claims":   clean,
    "dq_flagged":     dq_issues,
    "quarantined":    quarantine.count(),
})
