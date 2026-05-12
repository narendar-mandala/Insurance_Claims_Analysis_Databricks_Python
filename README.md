# Insurance Claims Analysis — Databricks Medallion Pipeline

An end-to-end data engineering and analytics project built on **Databricks** that
ingests raw insurance claims data, applies automated data-quality remediation, and
produces leadership-ready Gold aggregation tables for dashboard reporting.

---

## Table of Contents

1. [Business Context](#1-business-context)
2. [Architecture](#2-architecture)
3. [Data Model](#3-data-model)
4. [Data Quality Checks](#4-data-quality-checks)
5. [Gold Layer — Insights for Leadership](#5-gold-layer--insights-for-leadership)
6. [Project Structure](#6-project-structure)
7. [Setup & Deployment](#7-setup--deployment)
8. [Running the Pipeline](#8-running-the-pipeline)
9. [CI/CD with GitHub Actions](#9-cicd-with-github-actions)
10. [Connector Requirements](#10-connector-requirements)
11. [Dashboard Metrics Reference](#11-dashboard-metrics-reference)

---

## 1. Business Context

Insurance companies generate large volumes of claims data across multiple policy
types (Life, Home, Auto, Health), geographies, and adjusters.  Without automated
quality checks this data accumulates inconsistencies that distort KPIs and lead to
incorrect loss-ratio reporting.

This project solves three core problems:

| Problem | Solution |
|---------|----------|
| **Data Inconsistencies** — orphan claims, invalid enums, pre-policy claim dates, outlier amounts, duplicate claim IDs | Automated DQ flagging in the Silver layer with a quarantine table for ops review |
| **Scattered Reporting** — raw CSVs fed directly into Power BI without validation | Structured Gold Delta tables with guaranteed row-level quality |
| **Leadership Visibility** — no single view of loss ratio, adjuster performance, or regional risk | Seven aggregated Gold tables covering every key dimension |

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Source CSVs (DBFS landing zone)                            │
│  Claims · PolicyHolders · Adjusters · AnalyticsExport       │
└───────────────────────┬─────────────────────────────────────┘
                        │  Notebook 01
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  BRONZE  (insurance_bronze_{env})                           │
│  Raw Delta tables — schema inference + audit metadata        │
│  claims_raw · policy_holders_raw · adjusters_raw            │
│  analytics_export_raw                                       │
└───────────────────────┬─────────────────────────────────────┘
                        │  Notebook 02
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  SILVER  (insurance_silver_{env})                           │
│  Cleansed, typed, DQ-flagged Delta tables                   │
│  claims_clean · policy_holders_clean · adjusters_clean      │
│  claims_quarantine (critical issues isolated)               │
└───────────────────────┬─────────────────────────────────────┘
                        │  Notebook 03
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  GOLD  (insurance_gold_{env})                               │
│  Aggregated tables for dashboard & leadership reporting     │
│  7 summary tables — see Section 5                           │
└───────────────────────┬─────────────────────────────────────┘
                        │  Notebook 04
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  ANALYTICS DASHBOARD                                        │
│  Databricks notebook visualisations + Power BI              │
└─────────────────────────────────────────────────────────────┘
```

**Technology stack:**

| Layer | Technology |
|-------|-----------|
| Storage | Databricks DBFS / Delta Lake |
| Compute | Databricks (Spark 3.4, Scala 2.12) |
| Orchestration | Databricks Workflows (Asset Bundles) |
| CI/CD | GitHub Actions |
| Reporting | Databricks SQL Dashboard / Power BI |

---

## 3. Data Model

### Source Tables

| Table | Records | Key columns |
|-------|---------|-------------|
| **Claims** | ~5,000+ | ClaimID (UUID), PolicyID, ClaimType, ClaimAmount, ClaimStatus, ClaimDate, AdjusterID |
| **Policy Holders** | 1,000 | PolicyID (10001–11000), CustomerName, PolicyType, StartDate, PremiumAmount |
| **Adjusters** | 50 | AdjusterID (1–50), AdjusterName, Region |

### Relationships

```
Adjusters (1) ──< Claims (M) >── (M) Policy Holders
                      │
              ClaimStatus: Paid | In Review | Closed | Denied | Open
              ClaimType:   Liability | Theft | Medical | Fire | Accident
              PolicyType:  Life | Home | Auto | Health
              Region:      North | South | East | West | Central
```

---

## 4. Data Quality Checks

The Silver layer runs **10 automated checks** across every claim record.

| Flag | Description | Severity |
|------|-------------|----------|
| `dq_null_fields` | Missing ClaimID, PolicyID, Amount, or Date | Critical |
| `dq_duplicate` | Same ClaimID appears more than once | Critical |
| `dq_orphan_claim` | PolicyID not found in Policy Holders table | Critical |
| `dq_invalid_adjuster` | AdjusterID not found in Adjusters table | Critical |
| `dq_bad_enum` | ClaimStatus or ClaimType is not a recognised value | Critical |
| `dq_invalid_amount` | ClaimAmount is null, zero, or negative | High |
| `dq_future_date` | ClaimDate is after today | High |
| `dq_claim_before_policy` | ClaimDate is before the policy's StartDate | High |
| `dq_outlier_amount` | ClaimAmount > 3× median for the same ClaimType | Medium |

Records with any **Critical** flag are moved to the `claims_quarantine` table for
manual review. All other records are written to `claims_clean` with their flags
intact so analysts can filter as needed.

**Name normalisation** strips professional titles (Dr., Mrs., PhD, MD, DDS, etc.)
from both `CustomerName` and `AdjusterName` to ensure consistent joins and grouping.

---

## 5. Gold Layer — Insights for Leadership

### claims_summary_by_status
How claims are distributed across the five lifecycle statuses.  Drives the
**Claim Resolution Rate** and **Open Backlog** KPIs.

### claims_summary_by_type
Volume, total payout, average, and median by claim type (Liability, Theft,
Medical, Fire, Accident). Helps underwriting identify which claim types drive
the most exposure.

### monthly_claims_trend
Month-over-month count and dollar volume from 2020 through present. Used for
trend lines and seasonality analysis.

### adjuster_performance
Per-adjuster KPIs including paid rate %, denial rate %, open rate %, and volume
rank.  Adjusters with >30% open/in-review claims are surfaced as a backlog alert.

### regional_claims_summary
Claims volume, total payout, premium collections, and the **Claims-to-Premium
Ratio** for each of the five regions.  Regions with a ratio > 1.0 are paying
out more than they collect — a direct profitability signal.

### policy_holder_analysis
Loss ratio and net position by policy type.  A loss ratio > 1.0 means the
product line is unprofitable at current premium levels.

### dq_scorecard
Operational scorecard showing every DQ check, the number and percentage of
flagged records, and a severity rating (HIGH / MEDIUM / LOW).  Refreshed on
every pipeline run.

---

## 6. Project Structure

```
Insurance_Claims_Analysis_Databricks_Python/
│
├── notebooks/
│   ├── 00_setup_environment.py       # Creates schemas & landing zone
│   ├── 01_bronze_ingestion.py        # CSV → Bronze Delta tables
│   ├── 02_silver_quality_cleansing.py # DQ checks + cleansing → Silver
│   ├── 03_gold_aggregations.py       # Business aggregations → Gold
│   └── 04_analytics_dashboard.py     # Executive visualisations
│
├── data/                             # Source CSV files (upload to DBFS)
│   ├── Claims_v6.csv
│   ├── Policy_Holders_v6.csv
│   ├── Adjusters_v6.csv
│   └── Insurance_Claims_Analytics_Power_BI.csv
│
├── config/
│   └── pipeline_config.yml           # Shared configuration
│
├── .github/
│   └── workflows/
│       └── deploy.yml                # GitHub Actions CI/CD
│
├── databricks.yml                    # Databricks Asset Bundle definition
└── README.md
```

---

## 7. Setup & Deployment

### Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Python | 3.8+ | [python.org](https://python.org) |
| Databricks CLI | ≥ 0.18 | `pip install databricks-cli` |
| Git | any | [git-scm.com](https://git-scm.com) |

### Step 1 — Authenticate the Databricks CLI

```bash
databricks configure --token
# Host:  https://dbc-67b2619d-a17e.cloud.databricks.com
# Token: <your Personal Access Token>
```

Generate a PAT in Databricks: **User Settings → Access Tokens → Generate New Token**

### Step 2 — Upload source CSV files to DBFS

```bash
# From the repo root
databricks fs cp data/Claims_v6.csv              dbfs:/FileStore/insurance_claims/dev/raw/
databricks fs cp data/Policy_Holders_v6.csv      dbfs:/FileStore/insurance_claims/dev/raw/
databricks fs cp data/Adjusters_v6.csv           dbfs:/FileStore/insurance_claims/dev/raw/
databricks fs cp data/Insurance_Claims_Analytics_Power_BI.csv dbfs:/FileStore/insurance_claims/dev/raw/
```

### Step 3 — Deploy the Asset Bundle

```bash
# Validate
databricks bundle validate

# Deploy to dev
databricks bundle deploy --target dev

# Deploy to prod
databricks bundle deploy --target prod
```

### Step 4 — Run the pipeline manually (optional)

```bash
databricks bundle run insurance_claims_pipeline --target dev
```

---

## 8. Running the Pipeline

The pipeline is a five-step Databricks Workflow:

```
00_setup_environment
        ↓
01_bronze_ingestion
        ↓
02_silver_quality_cleansing
        ↓
03_gold_aggregations
        ↓
04_analytics_dashboard
```

**Scheduled:** Daily at 06:00 UTC (configurable in `databricks.yml`).

**Manual trigger:** Databricks Workspace → Workflows → *Insurance Claims Analysis Pipeline* → **Run Now**

---

## 9. CI/CD with GitHub Actions

The workflow in `.github/workflows/deploy.yml` automates deployments:

| Git event | Action |
|-----------|--------|
| Push to `develop` | Deploy → dev |
| Push to `main` | Deploy → prod + trigger pipeline run |
| Pull Request to `main` | Validate bundle only |
| Manual dispatch | Deploy to chosen target |

### Required GitHub Secrets

Go to **GitHub repo → Settings → Secrets and Variables → Actions** and add:

| Secret | Value |
|--------|-------|
| `DATABRICKS_HOST` | `https://dbc-67b2619d-a17e.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | XXXXXXX |

---

## 10. Connector Requirements

To make the full workflow automatic (Git push → Databricks deploy → pipeline run),
you need **two** things configured:

### A. Databricks ↔ GitHub (native Git integration)

1. In Databricks: **User Settings → Linked Accounts → Link GitHub**
2. This lets you use **Databricks Repos** to sync notebooks from GitHub automatically.
3. Notebooks can then be opened, edited, and committed directly in the Databricks UI.

### B. GitHub Actions Service Account (for CI/CD push)

1. Create a Databricks Service Principal (**Admin Console → Service Principals**)
2. Generate an OAuth token for it
3. Store as `DATABRICKS_TOKEN` in GitHub Secrets
4. The Actions workflow uses this to deploy the bundle on every push to `main`

### Optional: Unity Catalog

If your workspace has **Unity Catalog** enabled, update `databricks.yml` to use your
Unity Catalog name instead of `hive_metastore` and enable three-level namespace
(`catalog.schema.table`).

---

## 11. Dashboard Metrics Reference

| KPI | Formula | Target |
|-----|---------|--------|
| **Loss Ratio** | Total Claims Paid ÷ Total Premiums | < 1.0 |
| **Denial Rate** | Denied Claims ÷ Total Claims | Monitor |
| **Open Backlog %** | (Open + In Review) ÷ Total Claims | < 20% |
| **Adjuster Open Rate** | Open claims per adjuster ÷ their total | < 30% |
| **Regional Exposure** | Regional claims ÷ regional premiums | < 1.0 |
| **DQ Pass Rate** | Clean records ÷ Total records | > 95% |

---

*Built with Databricks · Delta Lake · Python · GitHub Actions*
