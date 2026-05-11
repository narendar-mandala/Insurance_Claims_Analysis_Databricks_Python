# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Analytics Dashboard — Executive Insights
# MAGIC
# MAGIC This notebook renders interactive Databricks visualisations directly from the
# MAGIC **gold** tables. It is designed for leadership review and can be published as a
# MAGIC **Databricks SQL Dashboard** or scheduled as a notebook job.
# MAGIC
# MAGIC ### Sections
# MAGIC 1. Executive KPI Summary
# MAGIC 2. Claims Volume & $ Trend (MoM)
# MAGIC 3. Claim Status Distribution
# MAGIC 4. Claims by Type — Volume vs Average Amount
# MAGIC 5. Regional Performance Heatmap
# MAGIC 6. Policy Profitability — Loss Ratio
# MAGIC 7. Adjuster Leaderboard
# MAGIC 8. Data Quality Health Check

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "hive_metastore", "Catalog")
dbutils.widgets.text("env",     "dev",            "Environment")

CATALOG = dbutils.widgets.get("catalog")
ENV     = dbutils.widgets.get("env")
GOLD    = f"{CATALOG}.insurance_gold_{ENV}"
SILVER  = f"{CATALOG}.insurance_silver_{ENV}"

# COMMAND ----------

# MAGIC %md ## 1. Executive KPI Summary

# COMMAND ----------

status_df    = spark.table(f"{GOLD}.claims_summary_by_status")
policy_df    = spark.table(f"{GOLD}.policy_holder_analysis")
regional_df  = spark.table(f"{GOLD}.regional_claims_summary")
dq_df        = spark.table(f"{GOLD}.dq_scorecard")

total_claims   = status_df.agg(F.sum("total_claims")).collect()[0][0]
total_paid_amt = status_df.filter(F.col("ClaimStatus") == "Paid").agg(F.sum("total_claim_amount")).collect()[0][0] or 0
total_premiums = policy_df.agg(F.sum("total_premiums_collected")).collect()[0][0] or 0
overall_loss   = round(total_paid_amt / total_premiums, 4) if total_premiums else 0
denial_rate    = status_df.filter(F.col("ClaimStatus") == "Denied").agg(F.sum("total_claims")).collect()[0][0] or 0

print("=" * 55)
print("          INSURANCE CLAIMS — EXECUTIVE KPIs")
print("=" * 55)
print(f"  Total Claims Processed    : {total_claims:>10,.0f}")
print(f"  Total Premiums Collected  : ${total_premiums:>10,.2f}")
print(f"  Total Claims Paid (Amt)   : ${total_paid_amt:>10,.2f}")
print(f"  Overall Loss Ratio        : {overall_loss:>10.2%}")
print(f"  Denied Claims             : {denial_rate:>10,.0f}  ({100*denial_rate/total_claims:.1f}%)")
print("=" * 55)

# COMMAND ----------

# MAGIC %md ## 2. Monthly Claims Trend

# COMMAND ----------

monthly = spark.table(f"{GOLD}.monthly_claims_trend")
display(monthly.select("year_month", "total_claims", "total_claim_amount", "avg_claim_amount").orderBy("year_month"))

# COMMAND ----------

# MAGIC %md ## 3. Claim Status Distribution

# COMMAND ----------

display(
    status_df
    .select("ClaimStatus", "total_claims", "total_claim_amount", "avg_claim_amount", "pct_of_total_claims")
    .orderBy(F.col("total_claims").desc())
)

# COMMAND ----------

# MAGIC %md ## 4. Claims by Type — Volume & Avg Amount

# COMMAND ----------

type_df = spark.table(f"{GOLD}.claims_summary_by_type")
display(
    type_df
    .select("ClaimType", "total_claims", "total_claim_amount",
            "avg_claim_amount", "median_claim_amount", "unique_policies_affected")
    .orderBy(F.col("total_claim_amount").desc())
)

# COMMAND ----------

# MAGIC %md ## 5. Regional Performance

# COMMAND ----------

display(
    regional_df
    .select("Region", "total_claims", "total_claim_amount",
            "total_premium_collected", "claims_to_premium_ratio",
            "denial_rate_pct", "adjusters_in_region")
    .orderBy(F.col("claims_to_premium_ratio").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC > **Insight:** Regions where `claims_to_premium_ratio > 1.0` are paying out more in
# MAGIC > claims than they collect in premiums — these are flagged for leadership review.

# COMMAND ----------

high_risk_regions = regional_df.filter(F.col("claims_to_premium_ratio") > 1.0)
if high_risk_regions.count() > 0:
    print("⚠  HIGH-RISK REGIONS (Loss Ratio > 100%):")
    display(high_risk_regions.select("Region", "claims_to_premium_ratio", "total_claim_amount", "total_premium_collected"))
else:
    print("✓  All regions within acceptable loss-ratio thresholds.")

# COMMAND ----------

# MAGIC %md ## 6. Policy Profitability — Loss Ratio by Policy Type

# COMMAND ----------

display(
    policy_df
    .select("PolicyType", "total_policies", "total_premiums_collected",
            "total_claims_paid", "loss_ratio", "net_position",
            "claims_per_policy", "denial_rate_pct")
    .orderBy(F.col("loss_ratio").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC > **Insight:** Loss ratio > 1.0 means the line of business is unprofitable at current premium levels.

# COMMAND ----------

# MAGIC %md ## 7. Adjuster Leaderboard

# COMMAND ----------

adj_df = spark.table(f"{GOLD}.adjuster_performance")
display(
    adj_df
    .select("rank_by_volume", "AdjusterName", "Region",
            "total_claims_handled", "avg_claim_amount",
            "paid_rate_pct", "denial_rate_pct", "open_rate_pct")
    .orderBy("rank_by_volume")
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC > **Insight:** Adjusters with a high `open_rate_pct` have a backlog of unresolved claims
# MAGIC > and may need capacity support.

# COMMAND ----------

backlog_alert = adj_df.filter(F.col("open_rate_pct") > 30)
if backlog_alert.count() > 0:
    print(f"⚠  {backlog_alert.count()} adjuster(s) have >30% open/in-review claims:")
    display(backlog_alert.select("AdjusterName", "Region", "open_rate_pct", "total_claims_handled"))

# COMMAND ----------

# MAGIC %md ## 8. Data Quality Health Check

# COMMAND ----------

display(
    dq_df
    .select("dq_check", "flagged_count", "flagged_pct", "severity")
    .orderBy(F.col("flagged_pct").desc())
)

# COMMAND ----------

critical_dq = dq_df.filter(F.col("severity") == "HIGH")
if critical_dq.count() > 0:
    print("⚠  HIGH-SEVERITY DATA QUALITY ISSUES DETECTED:")
    display(critical_dq)
else:
    print("✓  No high-severity data quality issues.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC *Dashboard generated by the Insurance Claims Analysis Pipeline.*
# MAGIC *Refresh by running the full pipeline: 00 → 01 → 02 → 03 → 04*
