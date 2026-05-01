# silver_to_gold_job.py
# GlobalPartners Business Insights — Silver to Gold Transformation Job
# Purpose: Read the clean Silver orders_enriched table and compute all
#          7 business metric tables, writing each to the Gold layer.
#
# Layer:   Silver (orders_enriched) → Gold (7 metric tables)
# Trigger: Glue Workflow Trigger 3 — runs on success of bronze_to_silver_job
#
# Input:   s3://<bucket>/silver/orders_enriched/
# Output:  s3://<bucket>/gold/customer_clv_daily/
#          s3://<bucket>/gold/customer_rfm_segments/
#          s3://<bucket>/gold/customer_churn_indicators/
#          s3://<bucket>/gold/sales_trends/
#          s3://<bucket>/gold/loyalty_comparison/
#          s3://<bucket>/gold/location_performance/

import sys
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    datediff,
    dense_rank,
    lit,
    max as _max,
    min as _min,
    month,
    ntile,
    percent_rank,
    round as _round,
    sum as _sum,
    to_date,
    when,
    year,
)
from pyspark.sql.window import Window


# ── Job Initialization ─────────────────────────────────────────────────────────
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_BUCKET",        # e.g. mbeccaria-dea-globalpartners
        "SILVER_PREFIX",    # e.g. silver
        "GOLD_PREFIX",      # e.g. gold
        "CHURN_DAYS",       # inactivity threshold e.g. 45
        "RFM_MONTHS",       # lookback window for RFM e.g. 6
    ],
)

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket        = args["S3_BUCKET"]
silver_prefix = args["SILVER_PREFIX"]
gold_prefix   = args["GOLD_PREFIX"]
churn_days    = int(args["CHURN_DAYS"])
rfm_months    = int(args["RFM_MONTHS"])

SILVER_BASE = f"s3://{bucket}/{silver_prefix}"
GOLD_BASE   = f"s3://{bucket}/{gold_prefix}"

# Reference date — "today" from the pipeline's perspective.
# Used consistently across ALL metrics so every calculation
# uses the same point in time within a single pipeline run.
reference_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# Reusable Spark date column for datediff() calculations
ref_date_col = to_date(lit(reference_date))

print(f"{'='*60}")
print(f"silver_to_gold_job started")
print(f"Silver source   : {SILVER_BASE}")
print(f"Gold output     : {GOLD_BASE}")
print(f"Reference date  : {reference_date}")
print(f"Churn threshold : {churn_days} days")
print(f"RFM lookback    : {rfm_months} months")
print(f"{'='*60}")


# ── Helper: write Gold table ───────────────────────────────────────────────────
# All Gold tables are written as Parquet in full overwrite mode.
# Gold tables are fully recomputed on every pipeline run —
# no partitioning needed since the whole table is replaced.
def write_to_gold(df, table_name):
    output_path = f"{GOLD_BASE}/{table_name}"
    row_count = df.count()
    print(f"\n  Writing {table_name}:")
    print(f"    Rows        : {row_count:,}")
    print(f"    Output path : {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    print(f"    {table_name} written successfully")
    return row_count


# ══════════════════════════════════════════════════════════════
# STEP 1 — READ SILVER
# Read the full orders_enriched table from Silver.
# This is the single source of truth for all Gold metrics.
# ══════════════════════════════════════════════════════════════
print("\n── STEP 1: Reading Silver orders_enriched ──")

df = spark.read.parquet(f"{SILVER_BASE}/orders_enriched/")

# Cast order_date to DateType for accurate date arithmetic
df = df.withColumn("order_date", to_date(col("order_date")))

total_rows = df.count()
print(f"  Total rows loaded : {total_rows:,}")
print(f"  Schema:")
df.printSchema()


# ══════════════════════════════════════════════════════════════
# METRIC 1 — customer_clv_daily (PRIMARY METRIC)
#
# Goal: Show how each customer's cumulative lifetime value
#       grows day by day over their entire order history.
#
# Approach:
#   1. Aggregate gross_revenue per customer per day
#   2. Use a window function (running sum) to accumulate revenue
#      from the customer's first order up to each snapshot date
#   3. Compute orders_to_date the same way (running count)
#   4. Tag each snapshot with a CLV tier based on the
#      customer's FINAL total revenue (top 20% = High, etc.)
# ══════════════════════════════════════════════════════════════
print("\n── METRIC 1: customer_clv_daily ──")

# Step 1a: Aggregate to one row per customer per day
df_daily = df.groupBy("user_id", "order_date", "is_loyalty") \
    .agg(
        _sum("gross_revenue").alias("daily_revenue"),
        countDistinct("order_id").alias("daily_orders"),
    )

# Step 1b: Define a window per customer ordered by date
# unboundedPreceding = from the customer's very first row
# currentRow         = up to and including today's row
# Together they create a cumulative/running sum per customer
clv_window = Window \
    .partitionBy("user_id") \
    .orderBy("order_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Step 1c: Apply running sum and running count over the window
df_clv = df_daily \
    .withColumn(
        "total_revenue_to_date",
        _round(_sum("daily_revenue").over(clv_window), 2)
    ) \
    .withColumn(
        "orders_to_date",
        _sum("daily_orders").over(clv_window).cast("integer")
    ) \
    .withColumn(
        "avg_order_value",
        _round(col("total_revenue_to_date") / col("orders_to_date"), 2)
    ) \
    .withColumnRenamed("order_date", "snapshot_date")

# Step 1d: Compute each customer's FINAL total revenue
# Used to assign CLV tier based on lifetime totals, not daily snapshots
final_revenue_window = Window.partitionBy("user_id")

df_clv = df_clv.withColumn(
    "final_total_revenue",
    _max("total_revenue_to_date").over(final_revenue_window)
)

# Step 1e: Assign CLV tier using percent_rank on final revenue
# percent_rank() scores each customer 0.0 to 1.0 vs all others
# > 0.80 = top 20%    -> High
# > 0.20 = middle 60% -> Medium
# ≤ 0.20 = bottom 20% -> Low
tier_window = Window.orderBy("final_total_revenue")

df_clv = df_clv \
    .withColumn("revenue_percentile", percent_rank().over(tier_window)) \
    .withColumn(
        "clv_tier",
        when(col("revenue_percentile") > 0.80, lit("High"))
        .when(col("revenue_percentile") > 0.20, lit("Medium"))
        .otherwise(lit("Low"))
    ) \
    .drop("final_total_revenue", "revenue_percentile")

# Select final columns for Gold
df_clv_final = df_clv.select(
    "user_id",
    "snapshot_date",
    "daily_revenue",
    "total_revenue_to_date",
    "orders_to_date",
    "avg_order_value",
    "clv_tier",
    "is_loyalty",
)

write_to_gold(df_clv_final, "customer_clv_daily")


# ══════════════════════════════════════════════════════════════
# METRIC 2 — customer_rfm_segments
#
# Goal: Classify customers into behavioral segments using
#       Recency, Frequency, and Monetary metrics.
#
# Definitions (all within the last RFM_MONTHS months):
#   Recency   = days since last order (lower = more recent = better)
#   Frequency = number of distinct orders in lookback window
#   Monetary  = total spend in lookback window
#
# Segments:
#   VIP          = high R, high F, high M
#   New Customer = high R (recent), low F (few orders)
#   Churn Risk   = low R (inactive), low F
#   Regular      = everyone else
# ══════════════════════════════════════════════════════════════
print("\n── METRIC 2: customer_rfm_segments ──")

# Filter to RFM lookback window (last N months from reference date)
# rfm_months * 30 approximates months as 30 days each
# e.g. rfm_months=6 → keep orders from last 180 days only
df_rfm_window = df.filter(
    datediff(ref_date_col, col("order_date")) <= (rfm_months * 30)
)

print(f"  RFM window rows : {df_rfm_window.count():,} (last {rfm_months} months)")

# Aggregate RFM metrics — one row per customer
df_rfm = df_rfm_window.groupBy("user_id") \
    .agg(
        # Days since their most recent order
        datediff(ref_date_col, _max("order_date")).alias("recency_days"),
        # Distinct orders in the window (not row count — Silver has many rows per order)
        countDistinct("order_id").alias("frequency"),
        # Total spend in the window
        _round(_sum("gross_revenue"), 2).alias("monetary"),
    )

# Score each metric into quartiles using ntile(4)
# ntile(4) splits all customers into 4 equal buckets: 1=worst, 4=best
rfm_window  = Window.orderBy("recency_days")
freq_window = Window.orderBy("frequency")
mon_window  = Window.orderBy("monetary")

df_rfm = df_rfm \
    .withColumn(
        # Recency: fewer days = more recent = better
        # ntile(4) gives 4 to highest recency_days (worst) so we reverse:
        # lit(5) - ntile(4) → lowest days gets score 4 (best)
        "r_score", lit(5) - ntile(4).over(rfm_window)
    ) \
    .withColumn(
        # Frequency: more orders = better = higher ntile score
        "f_score", ntile(4).over(freq_window)
    ) \
    .withColumn(
        # Monetary: more spend = better = higher ntile score
        "m_score", ntile(4).over(mon_window)
    ) \
    .withColumn(
        # Combined RFM score: ranges from 3 (worst) to 12 (best)
        "rfm_score",
        col("r_score") + col("f_score") + col("m_score")
    )

# Assign segments based on score combinations
# Conditions evaluated top to bottom — first match wins
df_rfm = df_rfm.withColumn(
    "rfm_segment",
    when(
        # All three scores high → best customers
        (col("r_score") >= 3) & (col("f_score") >= 3) & (col("m_score") >= 3),
        lit("VIP")
    ).when(
        # Recent but low frequency → ordered recently but not often
        (col("r_score") >= 3) & (col("f_score") <= 2),
        lit("New Customer")
    ).when(
        # Not recent AND low frequency → at risk of churning
        (col("r_score") <= 2) & (col("f_score") <= 2),
        lit("Churn Risk")
    ).otherwise(
        # Everyone else — active but not top tier
        lit("Regular")
    )
)

df_rfm_final = df_rfm.select(
    "user_id",
    "recency_days",
    "frequency",
    "monetary",
    "r_score",
    "f_score",
    "m_score",
    "rfm_score",
    "rfm_segment",
)

write_to_gold(df_rfm_final, "customer_rfm_segments")


# ══════════════════════════════════════════════════════════════
# METRIC 3 — customer_churn_indicators
#
# Goal: Build an activity profile per customer that marketing
#       can use to identify at-risk customers without ML.
#
# Indicators:
#   days_since_last_order  = how long since last order
#   avg_order_gap_days     = average days between orders
#   total_orders           = lifetime order count
#   total_spend            = lifetime gross revenue
#   spend_last_90          = spend in last 90 days
#   spend_prior_90         = spend in prior 90 days (days 91-180)
#   spend_change_pct       = % change last 90 vs prior 90
#   churn_risk_flag        = True if inactive > CHURN_DAYS threshold
# ══════════════════════════════════════════════════════════════
print("\n── METRIC 3: customer_churn_indicators ──")

# Base lifetime metrics per customer
df_churn_base = df.groupBy("user_id") \
    .agg(
        datediff(ref_date_col, _max("order_date")).alias("days_since_last_order"),
        countDistinct("order_id").alias("total_orders"),
        _round(_sum("gross_revenue"), 2).alias("total_spend"),
        _min("order_date").alias("first_order_date"),
        _max("order_date").alias("last_order_date"),
    )

# Average gap between orders:
# total days active / (total orders - 1)
# e.g. first order Jan 1, last order Mar 31 = 90 days active
#      if 4 orders placed → 3 gaps → avg gap = 90/3 = 30 days
# Customers with only 1 order get null (no gap exists)
df_churn_base = df_churn_base \
    .withColumn(
        "days_active",
        datediff(col("last_order_date"), col("first_order_date"))
    ) \
    .withColumn(
        "avg_order_gap_days",
        when(
            col("total_orders") > 1,
            _round(col("days_active") / (col("total_orders") - 1), 1)
        ).otherwise(lit(None))
    )

# Spend in last 90 days (days 0-90 from reference date)
df_recent = df.filter(
    datediff(ref_date_col, col("order_date")).between(0, 90)
).groupBy("user_id") \
    .agg(_round(_sum("gross_revenue"), 2).alias("spend_last_90"))

# Spend in prior 90 days (days 91-180 from reference date)
df_prior = df.filter(
    datediff(ref_date_col, col("order_date")).between(91, 180)
).groupBy("user_id") \
    .agg(_round(_sum("gross_revenue"), 2).alias("spend_prior_90"))

# Join spend windows back to base metrics
df_churn = df_churn_base \
    .join(df_recent, on="user_id", how="left") \
    .join(df_prior,  on="user_id", how="left")

# Spend change %: ((last_90 - prior_90) / prior_90) * 100
# +25.0 = spent 25% more recently (good sign)
# -40.0 = spent 40% less recently (churn signal)
# null  = no spend in either window (customer inactive in last 6 months)
df_churn = df_churn.withColumn(
    "spend_change_pct",
    when(
        (col("spend_prior_90").isNotNull()) & (col("spend_prior_90") > 0),
        _round(
            ((col("spend_last_90") - col("spend_prior_90")) / col("spend_prior_90")) * 100,
            2
        )
    ).otherwise(lit(None))
)

# Flag customers who haven't ordered in more than CHURN_DAYS days
df_churn = df_churn.withColumn(
    "churn_risk_flag",
    when(col("days_since_last_order") > churn_days, lit(True))
    .otherwise(lit(False))
)

df_churn_final = df_churn.select(
    "user_id",
    "days_since_last_order",
    "avg_order_gap_days",
    "total_orders",
    "total_spend",
    "spend_last_90",
    "spend_prior_90",
    "spend_change_pct",
    "churn_risk_flag",
    "first_order_date",
    "last_order_date",
)

write_to_gold(df_churn_final, "customer_churn_indicators")


# ══════════════════════════════════════════════════════════════
# METRIC 4 — sales_trends
#
# Goal: Generate time-based revenue summaries for trend analysis.
#       One row per (date × restaurant × category) combination.
#       Dashboard can aggregate further without recomputing.
# ══════════════════════════════════════════════════════════════
print("\n── METRIC 4: sales_trends ──")

df_trends = df.groupBy(
    "order_date",
    "year",
    "month",
    "week",
    "day_of_week",
    "is_weekend",
    "is_holiday",
    "holiday_name",
    "restaurant_id",
    "item_category",
) \
.agg(
    _round(_sum("gross_revenue"), 2).alias("total_revenue"),
    countDistinct("order_id").alias("order_count"),
    countDistinct("user_id").alias("unique_customers"),
    _round(avg("gross_revenue"), 2).alias("avg_item_revenue"),
)

write_to_gold(df_trends, "sales_trends")


# ══════════════════════════════════════════════════════════════
# METRIC 5 — loyalty_comparison
#
# Goal: Compare KPIs between loyalty members and non-members.
#
# Two-step aggregation to avoid double-counting:
#   Step 1: compute per-customer totals (each customer = 1 row)
#   Step 2: average across customers within each loyalty group
# ══════════════════════════════════════════════════════════════
print("\n── METRIC 5: loyalty_comparison ──")

# Step 1: per-customer totals
df_per_customer = df.groupBy("user_id", "is_loyalty") \
    .agg(
        _round(_sum("gross_revenue"), 2).alias("customer_total_spend"),
        countDistinct("order_id").alias("customer_order_count"),
    ) \
    .withColumn(
        "customer_avg_order_value",
        _round(col("customer_total_spend") / col("customer_order_count"), 2)
    )

# Step 2: average across customers per loyalty group
df_loyalty = df_per_customer.groupBy("is_loyalty") \
    .agg(
        count("user_id").alias("total_customers"),
        _round(avg("customer_total_spend"), 2).alias("avg_clv"),
        _round(avg("customer_order_count"), 2).alias("avg_orders_per_customer"),
        _round(avg("customer_avg_order_value"), 2).alias("avg_order_value"),
        _round(
            # Repeat purchase rate = % of customers with more than 1 order
            (count(when(col("customer_order_count") > 1, True)) / count("user_id")) * 100,
            2
        ).alias("repeat_purchase_rate_pct"),
    )

write_to_gold(df_loyalty, "loyalty_comparison")


# ══════════════════════════════════════════════════════════════
# METRIC 6 — location_performance
#
# Goal: Rank all 28 restaurant locations by revenue and surface
#       operational metrics to identify top and bottom performers.
# ══════════════════════════════════════════════════════════════
print("\n── METRIC 6: location_performance ──")

df_location = df.groupBy("restaurant_id") \
    .agg(
        _round(_sum("gross_revenue"), 2).alias("total_revenue"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("user_id").alias("unique_customers"),
        _round(avg("gross_revenue"), 2).alias("avg_item_revenue"),
        _round(
            _sum("gross_revenue") / countDistinct("order_date"),
            2
        ).alias("avg_daily_revenue"),
    ) \
    .withColumn(
        "avg_order_value",
        _round(col("total_revenue") / col("total_orders"), 2)
    )

# Rank locations by total revenue — highest revenue = rank 1
# dense_rank() ensures no rank numbers are skipped on ties
rank_window = Window.orderBy(col("total_revenue").desc())

df_location = df_location \
    .withColumn("revenue_rank", dense_rank().over(rank_window)) \
    .withColumn(
        "performance_tier",
        when(col("revenue_rank") <= 5,  lit("Top Performer"))
        .when(col("revenue_rank") >= 24, lit("Needs Attention"))
        .otherwise(lit("Mid Tier"))
    )

df_location_final = df_location.select(
    "revenue_rank",
    "restaurant_id",
    "total_revenue",
    "total_orders",
    "unique_customers",
    "avg_order_value",
    "avg_daily_revenue",
    "performance_tier",
)

write_to_gold(df_location_final, "location_performance")


# ══════════════════════════════════════════════════════════════
# JOB SUMMARY
# ══════════════════════════════════════════════════════════════
print(f"\n{'='*60}")
print(f"silver_to_gold_job completed successfully")
print(f"Reference date  : {reference_date}")
print(f"Metrics written to: {GOLD_BASE}")
print(f"  customer_clv_daily")
print(f"  customer_rfm_segments")
print(f"  customer_churn_indicators")
print(f"  sales_trends")
print(f"  loyalty_comparison")
print(f"  location_performance")
print(f"  ⏸  discount_effectiveness — pending SME clarification")
print(f"{'='*60}")

job.commit()