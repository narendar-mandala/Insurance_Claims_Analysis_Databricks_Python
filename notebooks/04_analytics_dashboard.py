# Databricks notebook source
# MAGIC %md
# MAGIC # Insurance Claims — Executive Dashboard
# MAGIC
# MAGIC **Data story:** This dashboard tracks the health of our insurance portfolio across
# MAGIC five dimensions — claim volume, financial exposure, geographic risk, adjuster
# MAGIC productivity, and data quality.  Each section surfaces a specific question
# MAGIC leadership needs answered.
# MAGIC
# MAGIC > Run this notebook after the full pipeline (00 → 03) has completed.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("env",     "dev",       "Environment")

CATALOG = dbutils.widgets.get("catalog")
ENV     = dbutils.widgets.get("env")
GOLD    = f"{CATALOG}.insurance_gold_{ENV}"

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Chapter 1 · The Big Picture
# MAGIC *How large is our claims portfolio, and are we profitable overall?*

# COMMAND ----------

# MAGIC %md ### Key Performance Indicators

# COMMAND ----------

from pyspark.sql import functions as F

status_df  = spark.table(f"{GOLD}.claims_summary_by_status")
policy_df  = spark.table(f"{GOLD}.policy_holder_analysis")

total_claims    = status_df.agg(F.sum("total_claims")).collect()[0][0] or 0
total_premium   = policy_df.agg(F.sum("total_premiums_collected")).collect()[0][0] or 0
total_paid      = status_df.filter(F.col("ClaimStatus")=="Paid").agg(F.sum("total_claim_amount")).collect()[0][0] or 0
total_denied    = status_df.filter(F.col("ClaimStatus")=="Denied").agg(F.sum("total_claims")).collect()[0][0] or 0
total_open      = status_df.filter(F.col("ClaimStatus").isin("Open","In Review")).agg(F.sum("total_claims")).collect()[0][0] or 0
loss_ratio      = round(total_paid / total_premium, 4) if total_premium else 0
denial_rate_pct = round(100 * total_denied / total_claims, 1) if total_claims else 0
open_rate_pct   = round(100 * total_open   / total_claims, 1) if total_claims else 0

print("━" * 60)
print("       INSURANCE CLAIMS PORTFOLIO — EXECUTIVE KPIs")
print("━" * 60)
print(f"  Total Claims Filed          {total_claims:>10,.0f}")
print(f"  Total Premiums Collected   ${total_premium:>10,.2f}")
print(f"  Total Claims Paid Out      ${total_paid:>10,.2f}")
print(f"  Overall Loss Ratio          {loss_ratio:>10.2%}  {'⚠ ABOVE TARGET' if loss_ratio > 0.7 else '✓ Within range'}")
print(f"  Denial Rate                 {denial_rate_pct:>9.1f}%")
print(f"  Open / In-Review Rate       {open_rate_pct:>9.1f}%  {'⚠ High backlog' if open_rate_pct > 20 else '✓ Manageable'}")
print("━" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Chapter 2 · Claim Status — Where Do Claims End Up?
# MAGIC *Understanding the resolution funnel shows operational efficiency.*

# COMMAND ----------

# MAGIC %md ### Claim Count and Total Payout by Status

# COMMAND ----------

spark.sql(f"""
SELECT
    ClaimStatus                             AS `Claim Status`,
    total_claims                            AS `Number of Claims`,
    total_claim_amount                      AS `Total Payout ($)`,
    avg_claim_amount                        AS `Avg Claim ($)`,
    pct_of_total_claims                     AS `% of Total`
FROM {GOLD}.claims_summary_by_status
ORDER BY total_claims DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC > **Insight:** The ratio of *Paid* to *Denied* reveals our approval rate.
# MAGIC > A rising *In Review* share suggests adjuster capacity pressure.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Chapter 3 · Claim Types — What Are People Claiming?
# MAGIC *Which claim types carry the most financial exposure?*

# COMMAND ----------

# MAGIC %md ### Volume and Exposure by Claim Type

# COMMAND ----------

spark.sql(f"""
SELECT
    ClaimType                   AS `Claim Type`,
    total_claims                AS `Number of Claims`,
    total_claim_amount          AS `Total Exposure ($)`,
    avg_claim_amount            AS `Avg Claim ($)`,
    median_claim_amount         AS `Median Claim ($)`,
    unique_policies_affected    AS `Policies Affected`
FROM {GOLD}.claims_summary_by_type
ORDER BY total_claim_amount DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC > **Insight:** Claim types where *Avg* significantly exceeds *Median* indicate
# MAGIC > a small number of large outlier claims pulling up the average — a target
# MAGIC > for special investigation.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Chapter 4 · Profitability by Policy Type
# MAGIC *Which lines of business are making or losing money?*

# COMMAND ----------

# MAGIC %md ### Loss Ratio and Net Position by Policy Type

# COMMAND ----------

spark.sql(f"""
SELECT
    PolicyType                      AS `Policy Type`,
    total_policies                  AS `Policies`,
    total_premiums_collected        AS `Premiums Collected ($)`,
    total_claims_paid               AS `Claims Paid Out ($)`,
    ROUND(loss_ratio * 100, 1)      AS `Loss Ratio (%)`,
    net_position                    AS `Net Position ($)`,
    claims_per_policy               AS `Claims per Policy`,
    denial_rate_pct                 AS `Denial Rate (%)`
FROM {GOLD}.policy_holder_analysis
ORDER BY loss_ratio DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC > **Insight:** Loss Ratio > 100% means we pay out more than we collect in premiums
# MAGIC > for that product line — a direct signal to review pricing or underwriting criteria.
# MAGIC > Net Position tells leadership the dollar impact.

# COMMAND ----------

spark.sql(f"""
SELECT PolicyType AS `Policy Type`, ROUND(loss_ratio * 100, 1) AS `Loss Ratio (%)`
FROM {GOLD}.policy_holder_analysis
ORDER BY loss_ratio DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Chapter 5 · Geographic Risk
# MAGIC *Which regions are our biggest exposure — and are they covered by premiums?*

# COMMAND ----------

# MAGIC %md ### Regional Claims vs Premiums

# COMMAND ----------

spark.sql(f"""
SELECT
    Region                              AS `Region`,
    total_claims                        AS `Claims Filed`,
    total_claim_amount                  AS `Total Claims ($)`,
    total_premium_collected             AS `Premiums Collected ($)`,
    ROUND(claims_to_premium_ratio,3)    AS `Loss Ratio`,
    denial_rate_pct                     AS `Denial Rate (%)`,
    adjusters_in_region                 AS `Adjusters`,
    policies_served                     AS `Policies`
FROM {GOLD}.regional_claims_summary
ORDER BY claims_to_premium_ratio DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC > **Insight:** Regions where Loss Ratio > 1.0 are unprofitable —
# MAGIC > claims paid exceed premiums collected. These regions need pricing review
# MAGIC > or stricter underwriting controls.

# COMMAND ----------

spark.sql(f"""
SELECT Region AS `Region`, total_claim_amount AS `Claims ($)`, total_premium_collected AS `Premiums ($)`
FROM {GOLD}.regional_claims_summary
ORDER BY total_claim_amount DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Chapter 6 · Monthly Trend
# MAGIC *Is claims volume growing, shrinking, or seasonal?*

# COMMAND ----------

# MAGIC %md ### Month-over-Month Claims Volume and Payout

# COMMAND ----------

spark.sql(f"""
SELECT
    year_month          AS `Month`,
    total_claims        AS `Claims Filed`,
    total_claim_amount  AS `Total Payout ($)`,
    avg_claim_amount    AS `Avg Claim ($)`,
    active_adjusters    AS `Active Adjusters`,
    unique_policies     AS `Unique Policies`
FROM {GOLD}.monthly_claims_trend
ORDER BY year_month
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC > **Insight:** A rising trend in *Total Payout* without a matching rise in
# MAGIC > *Claims Filed* means individual claim sizes are growing — a severity signal.
# MAGIC > Peaks may indicate seasonal events (storms, accidents).

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Chapter 7 · Adjuster Performance
# MAGIC *Who is resolving claims efficiently, and who has a growing backlog?*

# COMMAND ----------

# MAGIC %md ### Top 20 Adjusters by Volume

# COMMAND ----------

spark.sql(f"""
SELECT
    rank_by_volume          AS `Rank`,
    AdjusterName            AS `Adjuster`,
    Region                  AS `Region`,
    total_claims_handled    AS `Claims Handled`,
    avg_claim_amount        AS `Avg Claim ($)`,
    paid_rate_pct           AS `Paid Rate (%)`,
    denial_rate_pct         AS `Denial Rate (%)`,
    open_rate_pct           AS `Open / In-Review (%)`
FROM {GOLD}.adjuster_performance
ORDER BY rank_by_volume
LIMIT 20
""").display()

# COMMAND ----------

# MAGIC %md ### Adjusters with High Open Claim Backlog (> 30%)

# COMMAND ----------

spark.sql(f"""
SELECT
    AdjusterName            AS `Adjuster`,
    Region                  AS `Region`,
    total_claims_handled    AS `Total Claims`,
    open_rate_pct           AS `Open Rate (%)`,
    paid_rate_pct           AS `Paid Rate (%)`
FROM {GOLD}.adjuster_performance
WHERE open_rate_pct > 30
ORDER BY open_rate_pct DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC > **Insight:** Adjusters with Open Rate > 30% are candidates for workload
# MAGIC > redistribution or coaching. Cross-reference with their region to see
# MAGIC > if this is a regional or individual issue.

# COMMAND ----------

# MAGIC %md ### Paid Rate vs Denial Rate by Region

# COMMAND ----------

spark.sql(f"""
SELECT
    Region                              AS `Region`,
    ROUND(AVG(paid_rate_pct),1)         AS `Avg Paid Rate (%)`,
    ROUND(AVG(denial_rate_pct),1)       AS `Avg Denial Rate (%)`,
    ROUND(AVG(open_rate_pct),1)         AS `Avg Open Rate (%)`,
    COUNT(*)                            AS `Adjusters`
FROM {GOLD}.adjuster_performance
GROUP BY Region
ORDER BY `Avg Paid Rate (%)` DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Chapter 8 · Data Quality Health Check
# MAGIC *How trustworthy is our underlying data?*

# COMMAND ----------

# MAGIC %md ### DQ Scorecard — Issues Found in Source Data

# COMMAND ----------

spark.sql(f"""
SELECT
    REPLACE(dq_check, 'dq_', '')    AS `Check`,
    flagged_count                   AS `Records Flagged`,
    flagged_pct                     AS `% of Total`,
    total_records                   AS `Total Records`,
    severity                        AS `Severity`
FROM {GOLD}.dq_scorecard
ORDER BY flagged_pct DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC > **Insight:** Any check with Severity = HIGH (> 5% flagged) needs immediate
# MAGIC > attention from the data engineering team. MEDIUM issues should be tracked
# MAGIC > and resolved within the sprint. LOW issues are logged for awareness.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary — Key Findings for Leadership

# COMMAND ----------

from pyspark.sql import functions as F

regional = spark.table(f"{GOLD}.regional_claims_summary")
adj_perf = spark.table(f"{GOLD}.adjuster_performance")
dq       = spark.table(f"{GOLD}.dq_scorecard")
policy   = spark.table(f"{GOLD}.policy_holder_analysis")

high_risk_regions  = regional.filter(F.col("claims_to_premium_ratio") > 1.0).count()
backlog_adjusters  = adj_perf.filter(F.col("open_rate_pct") > 30).count()
high_dq_issues     = dq.filter(F.col("severity") == "HIGH").count()
loss_making_lines  = policy.filter(F.col("loss_ratio") > 1.0).count()

print("━" * 60)
print("              FINDINGS REQUIRING ATTENTION")
print("━" * 60)
print(f"  Unprofitable regions (loss ratio > 100%)  : {high_risk_regions}")
print(f"  Loss-making policy lines                  : {loss_making_lines}")
print(f"  Adjusters with >30% open claim backlog    : {backlog_adjusters}")
print(f"  High-severity data quality issues         : {high_dq_issues}")
print("━" * 60)
print()
print("  Recommended actions:")
print("  1. Review pricing in high loss-ratio regions")
print("  2. Reassign open claims from overloaded adjusters")
print("  3. Investigate DQ issues flagged as HIGH severity")
print("  4. Schedule quarterly loss ratio review by policy type")
print("━" * 60)
