# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Dashboard Queries
# MAGIC
# MAGIC Each cell below is a standalone SQL query for a **Databricks SQL Dashboard** widget.
# MAGIC
# MAGIC ### How to use
# MAGIC 1. Go to **Databricks SQL** → **Dashboards** → **Create Dashboard**
# MAGIC 2. Add a widget → paste the SQL from each cell
# MAGIC 3. Choose the chart type shown in the comment above each query
# MAGIC 4. Set the widget title to match the `-- TITLE:` line

# COMMAND ----------

# MAGIC %md ## KPI Widgets (Counter chart type)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TITLE: Total Claims Filed
# MAGIC -- CHART: Counter
# MAGIC SELECT SUM(total_claims) AS `Total Claims`
# MAGIC FROM workspace.insurance_gold_dev.claims_summary_by_status

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TITLE: Overall Loss Ratio
# MAGIC -- CHART: Counter
# MAGIC SELECT CONCAT(ROUND(SUM(total_claims_paid) / SUM(total_premiums_collected) * 100, 1), '%') AS `Loss Ratio`
# MAGIC FROM workspace.insurance_gold_dev.policy_holder_analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TITLE: Total Premiums Collected
# MAGIC -- CHART: Counter
# MAGIC SELECT CONCAT('$', FORMAT_NUMBER(SUM(total_premiums_collected), 0)) AS `Total Premiums`
# MAGIC FROM workspace.insurance_gold_dev.policy_holder_analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TITLE: Claims Paid Out
# MAGIC -- CHART: Counter
# MAGIC SELECT CONCAT('$', FORMAT_NUMBER(SUM(total_claim_amount), 0)) AS `Claims Paid`
# MAGIC FROM workspace.insurance_gold_dev.claims_summary_by_status
# MAGIC WHERE ClaimStatus = 'Paid'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TITLE: Denial Rate
# MAGIC -- CHART: Counter
# MAGIC SELECT CONCAT(ROUND(100.0 * SUM(CASE WHEN ClaimStatus = 'Denied' THEN total_claims ELSE 0 END) / SUM(total_claims), 1), '%') AS `Denial Rate`
# MAGIC FROM workspace.insurance_gold_dev.claims_summary_by_status

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TITLE: Open Claim Backlog
# MAGIC -- CHART: Counter
# MAGIC SELECT SUM(total_claims) AS `Open Claims`
# MAGIC FROM workspace.insurance_gold_dev.claims_summary_by_status
# MAGIC WHERE ClaimStatus IN ('Open', 'In Review')

# COMMAND ----------

# MAGIC %md ## Bar Charts

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TITLE: Claims by Status
# MAGIC -- CHART: Bar — X: Claim Status, Y: Number of Claims
# MAGIC SELECT ClaimStatus AS `Claim Status`, total_claims AS `Claims`, total_claim_amount AS `Total Payout ($)`
# MAGIC FROM workspace.insurance_gold_dev.claims_summary_by_status
# MAGIC ORDER BY total_claims DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TITLE: Total Exposure by Claim Type
# MAGIC -- CHART: Bar — X: Claim Type, Y: Total Exposure ($)
# MAGIC SELECT ClaimType AS `Claim Type`, total_claim_amount AS `Total Exposure ($)`, avg_claim_amount AS `Avg Claim ($)`, total_claims AS `Count`
# MAGIC FROM workspace.insurance_gold_dev.claims_summary_by_type
# MAGIC ORDER BY total_claim_amount DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TITLE: Loss Ratio by Policy Type
# MAGIC -- CHART: Bar — X: Policy Type, Y: Loss Ratio (%) — add reference line at 100
# MAGIC SELECT
# MAGIC     PolicyType                          AS `Policy Type`,
# MAGIC     ROUND(loss_ratio * 100, 1)          AS `Loss Ratio (%)`,
# MAGIC     net_position                        AS `Net Position ($)`,
# MAGIC     total_premiums_collected            AS `Premiums ($)`,
# MAGIC     total_claims_paid                   AS `Claims Paid ($)`
# MAGIC FROM workspace.insurance_gold_dev.policy_holder_analysis
# MAGIC ORDER BY loss_ratio DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TITLE: Claims vs Premiums by Region
# MAGIC -- CHART: Grouped Bar — X: Region, Y: Claims ($) and Premiums ($)
# MAGIC SELECT
# MAGIC     Region                      AS `Region`,
# MAGIC     total_claim_amount          AS `Claims Paid ($)`,
# MAGIC     total_premium_collected     AS `Premiums Collected ($)`,
# MAGIC     ROUND(claims_to_premium_ratio * 100, 1) AS `Loss Ratio (%)`
# MAGIC FROM workspace.insurance_gold_dev.regional_claims_summary
# MAGIC ORDER BY claims_to_premium_ratio DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TITLE: Top 10 Adjusters by Claims Handled
# MAGIC -- CHART: Bar (horizontal) — X: Claims Handled, Y: Adjuster Name, Color: Region
# MAGIC SELECT AdjusterName AS `Adjuster`, Region, total_claims_handled AS `Claims Handled`, paid_rate_pct AS `Paid Rate (%)`, open_rate_pct AS `Open Rate (%)`
# MAGIC FROM workspace.insurance_gold_dev.adjuster_performance
# MAGIC ORDER BY total_claims_handled DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md ## Line Charts

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TITLE: Monthly Claims Volume Trend
# MAGIC -- CHART: Line — X: Month, Y: Claims Filed
# MAGIC SELECT year_month AS `Month`, total_claims AS `Claims Filed`, avg_claim_amount AS `Avg Claim ($)`
# MAGIC FROM workspace.insurance_gold_dev.monthly_claims_trend
# MAGIC ORDER BY year_month

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TITLE: Monthly Payout Trend
# MAGIC -- CHART: Line — X: Month, Y: Total Payout ($)
# MAGIC SELECT year_month AS `Month`, total_claim_amount AS `Total Payout ($)`, active_adjusters AS `Active Adjusters`
# MAGIC FROM workspace.insurance_gold_dev.monthly_claims_trend
# MAGIC ORDER BY year_month

# COMMAND ----------

# MAGIC %md ## Pie / Donut Charts

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TITLE: Claim Status Distribution
# MAGIC -- CHART: Pie/Donut — Label: Claim Status, Value: % of Total
# MAGIC SELECT ClaimStatus AS `Claim Status`, pct_of_total_claims AS `% of Total`, total_claims AS `Count`
# MAGIC FROM workspace.insurance_gold_dev.claims_summary_by_status
# MAGIC ORDER BY total_claims DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TITLE: Claims by Region (Share)
# MAGIC -- CHART: Pie/Donut — Label: Region, Value: Claims Filed
# MAGIC SELECT Region, total_claims AS `Claims Filed`, total_claim_amount AS `Total ($)`
# MAGIC FROM workspace.insurance_gold_dev.regional_claims_summary
# MAGIC ORDER BY total_claims DESC

# COMMAND ----------

# MAGIC %md ## Data Quality Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TITLE: Data Quality Scorecard
# MAGIC -- CHART: Table with conditional formatting on Severity
# MAGIC SELECT
# MAGIC     REPLACE(dq_check, 'dq_', '')    AS `Check`,
# MAGIC     flagged_count                   AS `Flagged Records`,
# MAGIC     flagged_pct                     AS `% Flagged`,
# MAGIC     severity                        AS `Severity`
# MAGIC FROM workspace.insurance_gold_dev.dq_scorecard
# MAGIC ORDER BY flagged_pct DESC

# COMMAND ----------

# MAGIC %md ## Adjuster Performance Heatmap Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TITLE: Adjuster Performance Summary
# MAGIC -- CHART: Table — sort by Open Rate descending to surface backlog risks first
# MAGIC SELECT
# MAGIC     AdjusterName        AS `Adjuster`,
# MAGIC     Region,
# MAGIC     total_claims_handled AS `Claims`,
# MAGIC     paid_rate_pct        AS `Paid %`,
# MAGIC     denial_rate_pct      AS `Denied %`,
# MAGIC     open_rate_pct        AS `Open %`,
# MAGIC     avg_claim_amount     AS `Avg Claim ($)`
# MAGIC FROM workspace.insurance_gold_dev.adjuster_performance
# MAGIC ORDER BY open_rate_pct DESC
