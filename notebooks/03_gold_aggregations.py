# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold Layer — Aggregated Summary Tables
# MAGIC
# MAGIC Reads from the **silver** clean tables and produces seven leadership-ready
# MAGIC summary tables in the **gold** schema.  These tables feed directly into the
# MAGIC Power BI / Databricks SQL dashboard.
# MAGIC
# MAGIC | Gold table | Description |
# MAGIC |-----------|-------------|
# MAGIC | `claims_summary_by_status` | Count & $ by claim status |
# MAGIC | `claims_summary_by_type` | Count & $ by claim type |
# MAGIC | `monthly_claims_trend` | Month-over-month volume and $ trend |
# MAGIC | `adjuster_performance` | Per-adjuster KPIs (resolution rate, avg amount) |
# MAGIC | `regional_claims_summary` | Claims & premiums by geographic region |
# MAGIC | `policy_holder_analysis` | Policy-type profitability view (premiums vs claims) |
# MAGIC | `dq_scorecard` | Data-quality scorecard for ops monitoring |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("env",     "dev",            "Environment")

CATALOG = dbutils.widgets.get("catalog")
ENV     = dbutils.widgets.get("env")
SILVER  = f"{CATALOG}.insurance_silver_{ENV}"
GOLD    = f"{CATALOG}.insurance_gold_{ENV}"

# COMMAND ----------

claims    = spark.table(f"{SILVER}.claims_clean")
policies  = spark.table(f"{SILVER}.policy_holders_clean")
adjusters = spark.table(f"{SILVER}.adjusters_clean")

# Only use clean records for gold aggregations
clean_claims = claims.filter(~F.col("dq_has_any_issue"))

# Enriched claims: join policies and adjusters
enriched = (
    clean_claims
    .join(
        policies.select("PolicyID", "CustomerName", "PolicyType", "PremiumAmount", "StartDate"),
        "PolicyID", "left"
    )
    .join(
        adjusters.select(
            F.col("AdjusterID"),
            F.col("AdjusterName"),
            F.col("Region")
        ),
        "AdjusterID", "left"
    )
)

# COMMAND ----------

# MAGIC %md ## 1. Claims Summary by Status

# COMMAND ----------

claims_by_status = (
    enriched
    .groupBy("ClaimStatus")
    .agg(
        F.count("ClaimID").alias("total_claims"),
        F.round(F.sum("ClaimAmount"), 2).alias("total_claim_amount"),
        F.round(F.avg("ClaimAmount"), 2).alias("avg_claim_amount"),
        F.round(F.min("ClaimAmount"), 2).alias("min_claim_amount"),
        F.round(F.max("ClaimAmount"), 2).alias("max_claim_amount"),
    )
    .withColumn("pct_of_total_claims",
        F.round(100 * F.col("total_claims") / F.sum("total_claims").over(
            __import__("pyspark.sql", fromlist=["Window"]).Window.rowsBetween(
                __import__("pyspark.sql", fromlist=["Window"]).Window.unboundedPreceding,
                __import__("pyspark.sql", fromlist=["Window"]).Window.unboundedFollowing
            )
        ), 2))
    .withColumn("_gold_updated_at", F.current_timestamp())
    .orderBy(F.col("total_claims").desc())
)

(claims_by_status.write.format("delta").mode("overwrite")
 .option("overwriteSchema","true").saveAsTable(f"{GOLD}.claims_summary_by_status"))
print(f"claims_summary_by_status: {claims_by_status.count()} rows")
display(claims_by_status)

# COMMAND ----------

# MAGIC %md ## 2. Claims Summary by Type

# COMMAND ----------

claims_by_type = (
    enriched
    .groupBy("ClaimType")
    .agg(
        F.count("ClaimID").alias("total_claims"),
        F.round(F.sum("ClaimAmount"), 2).alias("total_claim_amount"),
        F.round(F.avg("ClaimAmount"), 2).alias("avg_claim_amount"),
        F.round(F.stddev("ClaimAmount"), 2).alias("stddev_claim_amount"),
        F.round(F.percentile_approx("ClaimAmount", 0.5), 2).alias("median_claim_amount"),
        F.countDistinct("PolicyID").alias("unique_policies_affected"),
    )
    .withColumn("_gold_updated_at", F.current_timestamp())
    .orderBy(F.col("total_claim_amount").desc())
)

(claims_by_type.write.format("delta").mode("overwrite")
 .option("overwriteSchema","true").saveAsTable(f"{GOLD}.claims_summary_by_type"))
print(f"claims_summary_by_type: {claims_by_type.count()} rows")
display(claims_by_type)

# COMMAND ----------

# MAGIC %md ## 3. Monthly Claims Trend

# COMMAND ----------

monthly_trend = (
    enriched
    .withColumn("claim_year",  F.year("ClaimDate"))
    .withColumn("claim_month", F.month("ClaimDate"))
    .withColumn("year_month",  F.date_format("ClaimDate", "yyyy-MM"))
    .groupBy("year_month", "claim_year", "claim_month")
    .agg(
        F.count("ClaimID").alias("total_claims"),
        F.round(F.sum("ClaimAmount"), 2).alias("total_claim_amount"),
        F.round(F.avg("ClaimAmount"), 2).alias("avg_claim_amount"),
        F.countDistinct("AdjusterID").alias("active_adjusters"),
        F.countDistinct("PolicyID").alias("unique_policies"),
    )
    .withColumn("_gold_updated_at", F.current_timestamp())
    .orderBy("year_month")
)

(monthly_trend.write.format("delta").mode("overwrite")
 .option("overwriteSchema","true").saveAsTable(f"{GOLD}.monthly_claims_trend"))
print(f"monthly_claims_trend: {monthly_trend.count()} rows")
display(monthly_trend)

# COMMAND ----------

# MAGIC %md ## 4. Adjuster Performance

# COMMAND ----------

from pyspark.sql import Window

w_all = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

adjuster_perf = (
    enriched
    .groupBy("AdjusterID", "AdjusterName", "Region")
    .agg(
        F.count("ClaimID").alias("total_claims_handled"),
        F.round(F.sum("ClaimAmount"), 2).alias("total_claim_amount"),
        F.round(F.avg("ClaimAmount"), 2).alias("avg_claim_amount"),
        F.round(
            100 * F.count(F.when(F.col("ClaimStatus") == "Paid", 1)) / F.count("ClaimID"),
            2
        ).alias("paid_rate_pct"),
        F.round(
            100 * F.count(F.when(F.col("ClaimStatus") == "Denied", 1)) / F.count("ClaimID"),
            2
        ).alias("denial_rate_pct"),
        F.round(
            100 * F.count(F.when(F.col("ClaimStatus").isin("Open","In Review"), 1)) / F.count("ClaimID"),
            2
        ).alias("open_rate_pct"),
        F.countDistinct("PolicyType").alias("policy_types_handled"),
    )
    .withColumn("rank_by_volume",
        F.rank().over(Window.orderBy(F.col("total_claims_handled").desc())))
    .withColumn("_gold_updated_at", F.current_timestamp())
    .orderBy(F.col("total_claims_handled").desc())
)

(adjuster_perf.write.format("delta").mode("overwrite")
 .option("overwriteSchema","true").saveAsTable(f"{GOLD}.adjuster_performance"))
print(f"adjuster_performance: {adjuster_perf.count()} rows")
display(adjuster_perf)

# COMMAND ----------

# MAGIC %md ## 5. Regional Claims Summary

# COMMAND ----------

regional_summary = (
    enriched
    .groupBy("Region")
    .agg(
        F.count("ClaimID").alias("total_claims"),
        F.round(F.sum("ClaimAmount"), 2).alias("total_claim_amount"),
        F.round(F.avg("ClaimAmount"), 2).alias("avg_claim_amount"),
        F.round(F.sum("PremiumAmount"), 2).alias("total_premium_collected"),
        F.countDistinct("AdjusterID").alias("adjusters_in_region"),
        F.countDistinct("PolicyID").alias("policies_served"),
        F.round(
            100 * F.count(F.when(F.col("ClaimStatus") == "Denied", 1)) / F.count("ClaimID"),
            2
        ).alias("denial_rate_pct"),
    )
    .withColumn("claims_to_premium_ratio",
        F.round(F.col("total_claim_amount") / F.col("total_premium_collected"), 4))
    .withColumn("_gold_updated_at", F.current_timestamp())
    .orderBy(F.col("total_claim_amount").desc())
)

(regional_summary.write.format("delta").mode("overwrite")
 .option("overwriteSchema","true").saveAsTable(f"{GOLD}.regional_claims_summary"))
print(f"regional_claims_summary: {regional_summary.count()} rows")
display(regional_summary)

# COMMAND ----------

# MAGIC %md ## 6. Policy Holder Analysis — Premium vs Claims (Profitability)

# COMMAND ----------

policy_analysis = (
    enriched
    .groupBy("PolicyType")
    .agg(
        F.countDistinct("PolicyID").alias("total_policies"),
        F.round(F.sum("PremiumAmount"), 2).alias("total_premiums_collected"),
        F.round(F.avg("PremiumAmount"), 2).alias("avg_premium"),
        F.count("ClaimID").alias("total_claims"),
        F.round(F.sum("ClaimAmount"), 2).alias("total_claims_paid"),
        F.round(F.avg("ClaimAmount"), 2).alias("avg_claim_amount"),
        F.round(
            100 * F.count(F.when(F.col("ClaimStatus") == "Denied", 1)) / F.count("ClaimID"),
            2
        ).alias("denial_rate_pct"),
    )
    .withColumn("loss_ratio",
        F.round(F.col("total_claims_paid") / F.col("total_premiums_collected"), 4))
    .withColumn("net_position",
        F.round(F.col("total_premiums_collected") - F.col("total_claims_paid"), 2))
    .withColumn("claims_per_policy",
        F.round(F.col("total_claims") / F.col("total_policies"), 2))
    .withColumn("_gold_updated_at", F.current_timestamp())
    .orderBy(F.col("loss_ratio").desc())
)

(policy_analysis.write.format("delta").mode("overwrite")
 .option("overwriteSchema","true").saveAsTable(f"{GOLD}.policy_holder_analysis"))
print(f"policy_holder_analysis: {policy_analysis.count()} rows")
display(policy_analysis)

# COMMAND ----------

# MAGIC %md ## 7. Data Quality Scorecard

# COMMAND ----------

dq_cols = [c for c in claims.columns if c.startswith("dq_")]
total_claims = claims.count()

dq_rows = []
for col in dq_cols:
    cnt = claims.filter(F.col(col)).count()
    dq_rows.append((col, cnt, round(100 * cnt / total_claims, 2) if total_claims else 0.0))

dq_scorecard = spark.createDataFrame(dq_rows, ["dq_check", "flagged_count", "flagged_pct"])
dq_scorecard = (
    dq_scorecard
    .withColumn("total_records", F.lit(total_claims))
    .withColumn("severity",
        F.when(F.col("flagged_pct") > 5, "HIGH")
         .when(F.col("flagged_pct") > 1, "MEDIUM")
         .otherwise("LOW"))
    .withColumn("_gold_updated_at", F.current_timestamp())
    .orderBy(F.col("flagged_pct").desc())
)

(dq_scorecard.write.format("delta").mode("overwrite")
 .option("overwriteSchema","true").saveAsTable(f"{GOLD}.dq_scorecard"))
print(f"dq_scorecard: {dq_scorecard.count()} checks logged")
display(dq_scorecard)

# COMMAND ----------

# MAGIC %md ## Gold Layer Summary

# COMMAND ----------

gold_tables = [
    "claims_summary_by_status",
    "claims_summary_by_type",
    "monthly_claims_trend",
    "adjuster_performance",
    "regional_claims_summary",
    "policy_holder_analysis",
    "dq_scorecard",
]

print("\n=== Gold Layer — Table Row Counts ===")
for t in gold_tables:
    cnt = spark.table(f"{GOLD}.{t}").count()
    print(f"  {t:<40} {cnt:>6} rows")

dbutils.notebook.exit({"gold_tables_created": len(gold_tables)})
