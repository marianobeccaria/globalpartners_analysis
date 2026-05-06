# GlobalPartners — Solution Design Document

> **Version:** 1.1 | **Status:** Updated with SME Feedback | **Architecture:** AWS Medallion (Bronze / Silver / Gold)
> 
> **Prepared by:** Data Engineering Team | **Primary Goal:** Customer Lifetime Value (CLV) — Daily

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Project Scope](#2-project-scope)
3. [Source Data](#3-source-data)
4. [EDA Findings & Data Quality](#4-eda-findings--data-quality)
5. [Pipeline Architecture](#5-pipeline-architecture)
6. [Gold Layer Data Model](#6-gold-layer-data-model)
7. [Technology Stack — Why Each Tool Was Chosen](#7-technology-stack--why-each-tool-was-chosen)
8. [SME Confirmed Assumptions](#8-sme-confirmed-assumptions)
9. [Remaining Follow-Up](#9-remaining-follow-up)

---

## 1. Executive Summary

GlobalPartners requires a unified data platform to analyze customer behavior, spending patterns, and overall business performance across all restaurant locations and ordering platforms.

The **primary deliverable** is a daily-updated Customer Lifetime Value (CLV) model that tracks how each customer's value evolves over time. **Secondary deliverables** include RFM (Recency, Frequency, Monetary) segmentation, churn indicators, sales trend monitoring, loyalty program analysis, location performance ranking, and discount effectiveness. Results will be displayed an interactive Streamlit dashboard.

The entire pipeline runs on AWS using PySpark for all transformation logic, following a Medallion Architecture (Bronze -> Silver -> Gold) orchestrated by AWS Glue Workflow with chained Triggers.

---

## 2. Project Scope

### 2.1 Objectives

**Primary**
- Calculate and update Customer Lifetime Value (CLV) daily per customer
- Classify customers into High / Medium / Low CLV tiers (top 20% / mid 60% / bottom 20%)

**Secondary**
- Segment customers using RFM (Recency, Frequency, Monetary) logic
- Identify customers at risk of churn based on inactivity thresholds (> 45 days)
- Surface sales trends and seasonal patterns by location and category
- Compare loyalty member vs. non-member spending and engagement
- Rank location performance by revenue, order volume, and average order value
- Measure discount and promotion effectiveness on revenue impact. Profitability is out of scope until cost or margin data is provided.

### 2.2 Constraints

| Constraint | Detail |
|---|---|
| Cloud Platform | AWS only — no Snowflake, DBT, or external tool licenses |
| Transformation Engine | All logic implemented in PySpark |
| Data Source | SQL Server via JDBC (CSVs used for current development phase) |
| Orchestration | AWS Glue Workflow with Triggers — no Step Functions |
| Scheduling | Daily pipeline run at 2:00 AM UTC |
| Encryption | SSE-S3 at rest, TLS in transit |

---

## 3. Source Data

All the information in this section was obtained by performing a preliminar data analysys and the sripts can be found here: [preliminary_data_analysis.py](../src/exploratory_data_analysis.py)

### 3.1 Tables

| Table | Records | Description |
|---|---|---|
| `order_items` | 203,519 | Core transactional data - customer, order, item, price, loyalty flag, timestamp, restaurant |
| `order_item_options` | 193,017 | Add-ons, customizations, and modifiers per line item. `option_price < 0` indicates a discount |
| `date_dim` | 365 | Calendar dimension table - currently covers 2023 only (see Section 5 for remediation) |

### 3.2 Key Relationships

- `order_items` - `order_item_options` joined on `(order_id, lineitem_id)`
- `order_items` - `date_dim` joined on `order_date = date_key`

### 3.3 Key Columns

**order_items**

| Column | Type | Description |
|---|---|---|
| `user_id` | String | Unique customer identifier |
| `order_id` | String | Unique order identifier |
| `restaurant_id` | String | Unique restaurant identifier |
| `app_name` | String | Ordering platform (e.g. Alltown Fresh) |
| `creation_time_utc` | Timestamp | ISO8601 timestamp of order placement |
| `lineitem_id` | String | Unique identifier for item within an order |
| `item_price` | Decimal | Unit price of the item |
| `item_quantity` | Integer | Quantity ordered |
| `is_loyalty` | Boolean | Loyalty membership flag |
| `currency` | String | Transaction currency (all USD) |

**order_item_options**

| Column | Type | Description |
|---|---|---|
| `order_id` | String | Links to parent order |
| `lineitem_id` | String | Links to parent line item |
| `option_group_name` | String | Group category (e.g. Size, Toppings) |
| `option_name` | String | Selected customization |
| `option_price` | Decimal | Price of the option (negative = discount) |
| `option_quantity` | Integer | Number of times option was added |

---

## 4. EDA Findings & Data Quality

A full Exploratory Data Analysis was conducted prior to architecture design. All findings below directly inform Silver layer transformation logic.

| # | Finding | Status | Action in Silver Layer |
|---|---|---|---|
| 1 | Zero nulls across all three tables | ✅ Clean | No action required |
| 2 | Zero duplicates in `order_items` and `date_dim` | ✅ Clean | No action required |
| 3 | 2,299 exact duplicate rows in `order_item_options` | ⚠️ Issue | `dropDuplicates()` in Bronze → Silver job |
| 4 | 51 rows with same `option_name` but different `option_group_name` | ✅ Valid | Keep — legitimate entries under different menu groups |
| 5 | Many `order_items` rows do not have matching `order_item_options` rows; 15 option rows are orphaned | ✅ Confirmed | Treat options as optional. Preserve all valid order item rows using a left join from items to aggregated options; ignore orphan option rows. |
| 6 | `date_dim` only covers 2023; orders span 2020–2024 | ✅ Approved | Regenerate `date_dim` programmatically for 2020–2024 in pipeline. Dates outside 2023 are treated as non-holidays for now. |
| 7 | `date_dim.date_key` format is `DD-MM-YYYY` vs order_date `YYYY-MM-DD` | ❌ Critical | Reformat `date_key` to `YYYY-MM-DD` before joining |
| 8 | 826 rows from `Alltown Fresh - DEVELOPMENT` platform | ⚠️ Issue | Filter out test orders in Silver layer |
| 9 | No negative `option_price` values found (0 discounts detected) | ✅ Approved Assumption | Use proxy promotional signals from available data: zero-price items and below-category-average prices. |
| 10 | `item_price` max = $5,000; `item_quantity` max = 500 | ✅ Approved Assumption | Keep bulk/catering-looking orders in CLV, but flag them separately for transparency and dashboard filtering. |
| 11 | Single currency (USD) | ✅ Clean | No FX conversion required |
| 12 | Loyalty split: 23% members, 77% non-members | ✅ Info | Baseline for loyalty program analysis |
| 13 | 3 ordering platforms; 28 restaurant locations | ✅ Info | All locations rankable; DEVELOPMENT platform filtered |

---

## 5. Pipeline Architecture

### 5.1 Overview — AWS Medallion Architecture

```
SQL Server/RDS via Glue JDBC (Production) / CSVs in S3 (Current Build)
        │
        ▼
-------------------------------
│  AWS Glue Spark Job         │  ingestion_job
│  Trigger 1: Scheduled Daily │  
-------------------------------
             │ Raw Parquet
             ▼
-------------------------------
│  S3 Bronze Layer            │  Raw — untouched
│  Partitioned by             │  ingestion_date
-------------------------------
             │ On SUCCESS ▶ Trigger 2
             ▼
-------------------------------
│  AWS Glue Spark Job         │  bronze_to_silver_job
│  (PySpark)                  │  Clean, join, enrich
-------------------------------
             │ Cleaned Parquet
             ▼
-------------------------------
│  S3 Silver Layer            │  orders_enriched
│  Partitioned by year/month  │
-------------------------------
             │ On SUCCESS ▶ Trigger 3
             ▼
-------------------------------
│  AWS Glue Spark Job         │  silver_to_gold_job
│  (PySpark)                  │  Compute all metrics
-------------------------------
             │ Metric Tables Parquet
             ▼
-------------------------------
│  S3 Gold Layer              │  7 business metric tables
-------------------------------
             │ boto3 / pandas
             ▼
-------------------------------
│  Streamlit Dashboard        │  Interactive visualizations
-------------------------------

-------------------------------------------------------------
Orchestration : AWS Glue Workflow + Triggers
Monitoring    : CloudWatch Logs + Alarms per Glue job
Access Control: IAM Role (least-privilege) on all Glue jobs
-------------------------------------------------------------
```

> 📎 Full architecture diagram available in `docs/globalpartners_architecture.drawio`

### 5.2 S3 Bucket Structure

```
s3://globalpartners-dea/
│
├── bronze/
│   ├── order_items/
│   │   └── ingestion_date=YYYY-MM-DD/
│   ├── order_item_options/
│   │   └── ingestion_date=YYYY-MM-DD/
│   └── date_dim/
│       └── ingestion_date=YYYY-MM-DD/
│
├── silver/
│   └── orders_enriched/
│       └── year=YYYY/month=MM/
│
└── gold/
    ├── customer_clv_daily/
    ├── customer_rfm_segments/
    ├── customer_churn_indicators/
    ├── sales_trends/
    ├── loyalty_comparison/
    ├── location_performance/
    └── discount_effectiveness/
```

All layers use **Parquet** format with **SSE-S3 encryption at rest**.

### 5.3 Glue Jobs Detail

#### Job 1 — `ingestion_job` (Glue Spark)

- Supports two source modes: CSV files in S3 for the current build, or SQL Server/RDS through a Glue JDBC connection for production-style ingestion
- Reads `order_items`, `order_item_options`, and `date_dim`
- Writes raw Parquet files to Bronze, partitioned by `ingestion_date`
- **No transformations** — Bronze is an exact copy of the source

#### Job 2 — `bronze_to_silver_job` (PySpark)

- Reads all three Bronze tables
- Drops 2,299 exact duplicate rows from `order_item_options` via `dropDuplicates()`
- Filters out 826 `Alltown Fresh - DEVELOPMENT` test orders
- Parses `creation_time_utc` as ISO8601 timestamps (`format='ISO8601'`)
- Reformats `date_dim.date_key` from `DD-MM-YYYY` to `YYYY-MM-DD`
- Regenerates `date_dim` to cover full 2020–2024 range programmatically; dates outside the provided 2023 holiday calendar are treated as non-holidays
- Aggregates option revenue to the line-item grain and left joins options to `order_items`, preserving item rows without modifiers/options
- Ignores orphan option rows that do not match an order item
- Joins enriched table with `date_dim` on `order_date = date_key`
- Computes `gross_revenue = (item_price × item_quantity) + (option_price × option_quantity)`
- Adds bulk/catering indicator fields so high-value or high-quantity orders remain in CLV but can be reported separately
- Writes `orders_enriched` Parquet to Silver, partitioned by `year/month`

#### Job 3 — `silver_to_gold_job` (PySpark)

- Reads Silver `orders_enriched` table
- Computes six core business metric tables (see Section 6)
- Carries bulk/catering flags into the CLV table using `bulk_orders_to_date` and `has_bulk_order_candidate`
- Writes each metric as a separate Parquet table to Gold layer

#### Job 4 — `discount_effectiveness_job` (PySpark)

- Reads Silver `orders_enriched` table
- Builds the `discount_effectiveness` Gold table separately
- Uses SME-approved proxy promotional signals because no explicit discount code, coupon, promotion table, cost, or margin data is available
- Flags orders with zero-price items and items priced more than one standard deviation below their category average
- Reports estimated revenue impact only; profitability is not calculated

### 5.4 Orchestration — Glue Workflow

| Trigger | Condition | Action |
|---|---|---|
| Trigger 1 | Scheduled — daily 2:00 AM UTC | Start `ingestion_job` |
| Trigger 2 | On **SUCCESS** of `ingestion_job` | Start `bronze_to_silver_job` |
| Trigger 3 | On **SUCCESS** of `bronze_to_silver_job` | Start `silver_to_gold_job` |

> Trigger chaining is the **failure reload mechanism** — if any job fails, downstream jobs do not run, ensuring Gold is never populated with bad or incomplete data. Failed jobs can be manually re-triggered from the Glue console after the root cause is resolved.

### 5.5 Production Requirements Coverage

| Requirement | Implementation |
|---|---|
| Scheduling | Glue Workflow Trigger — daily at 2:00 AM UTC |
| Encryption at rest | S3 SSE-S3 on all Bronze / Silver / Gold layers |
| Encryption in transit | TLS enforced on all JDBC connections to SQL Server |
| Failure reload | Trigger chaining — downstream jobs only fire on upstream success |
| Idempotency | Partitioning by `ingestion_date` - reruns overwrite only that partition |
| Monitoring | CloudWatch Logs per Glue job; CloudWatch Alarms on failure -> SNS |
| Access control | Least-privilege IAM role attached to all Glue jobs |

---

## 6. Gold Layer Data Model

### 6.1 Primary — `customer_clv_daily`

This the main deliverable. Tracks how each customer's cumulative lifetime value evolves day by day, enabling time-series analysis of revenue contribution and CLV tier transitions.

| Column | Type | Description |
|---|---|---|
| `user_id` | String | Unique customer identifier |
| `snapshot_date` | Date | The date for this CLV snapshot |
| `total_revenue_to_date` | Decimal | Cumulative gross revenue from this customer up to `snapshot_date` |
| `orders_to_date` | Integer | Total number of orders placed up to `snapshot_date` |
| `avg_order_value` | Decimal | `total_revenue_to_date / orders_to_date` |
| `clv_tier` | String | `High` (top 20%) / `Medium` (mid 60%) / `Low` (bottom 20%) |
| `is_loyalty` | Boolean | Loyalty membership flag as of `snapshot_date` |
| `bulk_orders_to_date` | Integer | Number of bulk/catering candidate orders included in CLV through `snapshot_date` |
| `has_bulk_order_candidate` | Boolean | True when the customer has at least one flagged bulk/catering candidate order |

### 6.2 Secondary Metric Tables

| Table | Key Columns |
|---|---|
| `customer_rfm_segments` | `user_id`, `recency_days`, `frequency`, `monetary`, `rfm_segment` (VIP / New / Churn Risk) |
| `customer_churn_indicators` | `user_id`, `days_since_last_order`, `avg_order_gap_days`, `spend_change_pct`, `churn_risk_flag` |
| `sales_trends` | `date`, `week`, `month`, `restaurant_id`, `item_category`, `total_revenue`, `order_count` |
| `loyalty_comparison` | `is_loyalty`, `avg_clv`, `avg_order_value`, `repeat_order_rate`, `total_customers` |
| `location_performance` | `restaurant_id`, `total_revenue`, `avg_order_value`, `orders_per_day`, `revenue_rank` |
| `discount_effectiveness` | `order_id`, `is_promotional_order`, `order_type`, `free_item_count`, `below_avg_price_item_count`, `total_estimated_discount`; uses proxy promotional signals and reports estimated revenue impact only |

---

## 7. Technology Stack — Why Each Tool Was Chosen

| Tool | Justification |
|---|---|
| **AWS Glue Spark** | PySpark required by client. Native AWS service. Used for ingestion and transformations so the pipeline can support both CSV and SQL Server/RDS JDBC sources. |
| **AWS Glue Workflow** | Native orchestration within Glue. Avoids Step Functions. Trigger chaining provides built-in failure isolation. |
| **Amazon RDS for SQL Server + Glue JDBC Connection** | Provides the target production source pattern requested by the SME while keeping the current CSV build available as the default source mode. |
| **Amazon S3 + Parquet** | Cost-effective, durable object storage. Parquet is columnar - fast for PySpark aggregations. Partition pruning reduces scan costs. |
| **Medallion Architecture** | Bronze preserves raw source fidelity. Silver enforces data quality. Gold serves business metrics cleanly. |
| **CloudWatch** | Native AWS monitoring. Alarms on job failure ensure pipeline issues are caught immediately. |
| **Streamlit** | Python-native dashboard framework. Reads Gold Parquet via `boto3`/`pandas`. No additional AWS service required for visualization. |
| **IAM Least Privilege** | Each Glue job uses a scoped IAM role with only the S3 prefixes and Glue actions it needs. Reduces any credential issue. |

---

## 8. Confirmed Assumptions

The following assumptions were reviewed and are reflected in the current pipeline implementation:

| # | Confirmed Assumption | Implementation |
|---|---|---|
| 1 | Generate a full 2020–2024 `date_dim` because the provided calendar only covers 2023. | Silver regenerates the date dimension for 2020–2024. Holiday flags and names are preserved from the provided 2023 data. Dates outside 2023 are treated as non-holidays until a complete holiday calendar is provided. |
| 2 | Use available-data proxy signals for discount and promotion analysis. | `discount_effectiveness_job` flags zero-price items and below-category-average priced items as proxy promotional signals. |
| 3 | Profitability cannot be calculated from the current source files. | Discount effectiveness focuses on estimated revenue impact only because product cost, margin, coupon, and promotion-code data are not available. |
| 4 | Keep bulk/catering-looking orders in CLV, but flag them separately. | Silver adds bulk/catering candidate flags. Gold CLV carries `bulk_orders_to_date` and `has_bulk_order_candidate` so analysts can identify customers whose CLV includes these orders. |
| 5 | Treat item options/modifiers as optional. | Silver preserves order item rows even when no matching option rows exist. Missing option revenue is treated as zero. |
| 6 | Document assumptions and limitations clearly in the dashboard and project documentation. | Discount proxy logic, missing profitability data, incomplete holiday coverage, and bulk/catering flags are documented here and surfaced in the dashboard methodology notes. |

---

## 9. Remaining Follow-Up

The default build uses CSV files uploaded to S3 as the source dataset. CDK now includes optional SQL Server/RDS and Glue JDBC infrastructure for the target production architecture:

```
SQL Server / RDS -> Glue JDBC Connection -> S3 Bronze -> Silver -> Gold -> Streamlit Dashboard
```

Remaining follow-up items:

- Update the Draw.io architecture diagram to show the SQL Server/RDS to Glue JDBC target flow and the current CSV-to-S3 development source path.
- Load the source CSV data into the SQL Server/RDS tables before running the pipeline in `SOURCE_MODE=jdbc`.
- Replace proxy promotional logic with actual discount, coupon, promotion, cost, or margin data if those sources become available.
- Expand holiday enrichment beyond 2023 if a complete 2020–2024 holiday calendar is provided.

---


```
 
```

---

*GlobalPartners Solution Design Document v1.1 — Confidential — For SME Review*
