# discount_effectiveness_job.py
# GlobalPartners Business Insights — Discount Effectiveness Job
# Purpose: Since no explicit discount codes exist in the dataset,
#          this job identifies promotional pricing signals using
#          two proxy approaches:
#
#   Option 1 — Zero-price items (item_price = 0.00)
#              These represent complimentary items or promotional giveaways
#
#   Option 2 — Below-average price items
#              Items priced more than 1 std deviation below their
#              category average — signals potential promotional pricing
#
# Layer:   Silver (orders_enriched) → Gold (discount_effectiveness)
# Trigger: Glue Workflow Trigger 4 — runs on success of silver_to_gold_job
#
# Input:   s3://<bucket>/silver/orders_enriched/
# Output:  s3://<bucket>/gold/discount_effectiveness/

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
    lit,
    round as _round,
    stddev,
    sum as _sum,
    when,
    month,
    year,
)
from pyspark.sql.window import Window


# ── Job Initialization ─────────────────────────────────────────────────────────
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_BUCKET",       # e.g. dea-globalpartners
        "SILVER_PREFIX",   # e.g. silver
        "GOLD_PREFIX",     # e.g. gold
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

SILVER_BASE = f"s3://{bucket}/{silver_prefix}"
GOLD_BASE   = f"s3://{bucket}/{gold_prefix}"

print(f"{'='*60}")
print(f"discount_effectiveness_job started")
print(f"Silver source  : {SILVER_BASE}")
print(f"Gold output    : {GOLD_BASE}")
print(f"{'='*60}")


# ══════════════════════════════════════════════════════════════
# STEP 1 — READ SILVER
# ══════════════════════════════════════════════════════════════
print("\n── STEP 1: Reading Silver orders_enriched ──")

df = spark.read.parquet(f"{SILVER_BASE}/orders_enriched/")
df = df.withColumn("year",  year(col("order_date"))) \
       .withColumn("month", month(col("order_date")))

total_rows = df.count()
print(f"  Total rows loaded : {total_rows:,}")


# ══════════════════════════════════════════════════════════════
# STEP 2 — COMPUTE CATEGORY PRICE STATISTICS
# Purpose: Calculate avg and stddev of item_price per category.
#          Used to identify items priced anomalously low
#          (Option 2 — below-average price signal).
#
# We use a window function so every row keeps all its columns
# while also having the category-level stats attached.
# This avoids a separate join step.
# ══════════════════════════════════════════════════════════════
print("\n── STEP 2: Computing category price statistics ──")

# Only include rows where item_price > 0 in the stats calculation
# Zero-price items are already captured by Option 1 and should
# not drag down the category average
df_priced = df.filter(col("item_price") > 0)

category_window = Window.partitionBy("item_category")

df = df \
    .withColumn(
        "category_avg_price",
        _round(avg("item_price").over(category_window), 4)
    ) \
    .withColumn(
        "category_stddev_price",
        _round(stddev("item_price").over(category_window), 4)
    )

# Print category stats for verification
category_stats = df.groupBy("item_category").agg(
    _round(avg("item_price"), 2).alias("avg_price"),
    _round(stddev("item_price"), 2).alias("stddev_price"),
    count("item_price").alias("item_count"),
).orderBy("avg_price", ascending=False)

print(f"  Category price stats:")
category_stats.show(20, truncate=False)


# ══════════════════════════════════════════════════════════════
# STEP 3 — APPLY PROMOTIONAL SIGNALS AT ITEM LEVEL
#
# Signal 1 — Free item flag:
#   item_price = 0.00 → this item was given for free
#
# Signal 2 — Below average price flag:
#   item_price < (category_avg - 1 * category_stddev)
#   i.e. priced more than 1 standard deviation below category avg
#   Note: only applies to items where item_price > 0
#         (free items are already captured by Signal 1)
#
# estimated_discount_per_item:
#   For free items: the category avg price (what it would have cost)
#   For below-avg items: (category_avg - item_price) × item_quantity
#   For full price items: 0
# ══════════════════════════════════════════════════════════════
print("\n── STEP 3: Applying promotional signals at item level ──")

df = df \
    .withColumn(
        "is_free_item",
        when(col("item_price") == 0, lit(True)).otherwise(lit(False))
    ) \
    .withColumn(
        "below_avg_threshold",
        col("category_avg_price") - col("category_stddev_price")
    ) \
    .withColumn(
        "is_below_avg_price",
        when(
            (col("item_price") > 0) &
            (col("item_price") < col("below_avg_threshold")),
            lit(True)
        ).otherwise(lit(False))
    ) \
    .withColumn(
        "estimated_discount_per_item",
        when(
            col("is_free_item"),
            # Free item: estimated loss = category avg × quantity
            _round(col("category_avg_price") * col("item_quantity"), 2)
        ).when(
            col("is_below_avg_price"),
            # Below avg: estimated discount = price gap × quantity
            _round(
                (col("category_avg_price") - col("item_price")) * col("item_quantity"),
                2
            )
        ).otherwise(lit(0.0))
    )

free_items       = df.filter(col("is_free_item")).count()
below_avg_items  = df.filter(col("is_below_avg_price")).count()
print(f"  Free items detected        : {free_items:,}")
print(f"  Below-avg items detected   : {below_avg_items:,}")


# ══════════════════════════════════════════════════════════════
# STEP 4 — AGGREGATE TO ORDER LEVEL
# Purpose: Collapse item-level signals to one row per order.
#          This is the final grain for the Gold table —
#          one row per order with all promotional signals summarized.
# ══════════════════════════════════════════════════════════════
print("\n── STEP 4: Aggregating to order level ──")

df_orders = df.groupBy(
    "order_id",
    "user_id",
    "restaurant_id",
    "order_date",
    "year",
    "month",
    "is_loyalty",
) \
.agg(
    # Revenue metrics
    _round(_sum("gross_revenue"), 2).alias("gross_revenue"),
    count("lineitem_id").alias("total_line_items"),
    countDistinct("item_name").alias("unique_items"),

    # Option 1 — Free item signals
    _sum(when(col("is_free_item"), col("item_quantity")).otherwise(0))
     .alias("free_item_count"),
    _round(_sum(
        when(col("is_free_item"), col("estimated_discount_per_item"))
        .otherwise(0)
    ), 2).alias("free_item_revenue_loss"),

    # Option 2 — Below average price signals
    _sum(when(col("is_below_avg_price"), col("item_quantity")).otherwise(0))
     .alias("below_avg_price_item_count"),
    _round(_sum(
        when(col("is_below_avg_price"), col("estimated_discount_per_item"))
        .otherwise(0)
    ), 2).alias("below_avg_discount_amt"),

    # Combined estimated discount
    _round(_sum("estimated_discount_per_item"), 2).alias("total_estimated_discount"),
)

# ── Derive order-level boolean flags and type ──────────────────────────────────
df_orders = df_orders \
    .withColumn(
        "has_free_item",
        when(col("free_item_count") > 0, lit(True)).otherwise(lit(False))
    ) \
    .withColumn(
        "has_below_avg_item",
        when(col("below_avg_price_item_count") > 0, lit(True)).otherwise(lit(False))
    ) \
    .withColumn(
        "is_promotional_order",
        when(
            (col("has_free_item")) | (col("has_below_avg_item")),
            lit(True)
        ).otherwise(lit(False))
    ) \
    .withColumn(
        # Human readable label for dashboard filtering
        "order_type",
        when(
            col("has_free_item") & col("has_below_avg_item"),
            lit("free_item+below_avg")
        ).when(
            col("has_free_item"),
            lit("free_item")
        ).when(
            col("has_below_avg_item"),
            lit("below_avg_price")
        ).otherwise(lit("full_price"))
    )

# ── Print summary stats ────────────────────────────────────────────────────────
total_orders       = df_orders.count()
promotional_orders = df_orders.filter(col("is_promotional_order")).count()
promo_pct          = promotional_orders / total_orders * 100 if total_orders > 0 else 0
total_discount     = df_orders.agg(_sum("total_estimated_discount")).collect()[0][0]

print(f"  Total orders              : {total_orders:,}")
print(f"  Promotional orders        : {promotional_orders:,} ({promo_pct:.1f}%)")
print(f"  Total estimated discount  : ${total_discount:,.2f}")

order_type_counts = df_orders.groupBy("order_type").count().orderBy("count", ascending=False)
print(f"\n  Order type breakdown:")
order_type_counts.show()


# ══════════════════════════════════════════════════════════════
# STEP 5 — SELECT FINAL COLUMNS AND WRITE TO GOLD
# ══════════════════════════════════════════════════════════════
print("\n── STEP 5: Writing discount_effectiveness to Gold ──")

df_final = df_orders.select(
    "order_id",
    "user_id",
    "restaurant_id",
    "order_date",
    "year",
    "month",
    "is_loyalty",
    "gross_revenue",
    "total_line_items",
    "unique_items",
    "has_free_item",
    "free_item_count",
    "free_item_revenue_loss",
    "has_below_avg_item",
    "below_avg_price_item_count",
    "below_avg_discount_amt",
    "total_estimated_discount",
    "is_promotional_order",
    "order_type",
)

output_path = f"{GOLD_BASE}/discount_effectiveness"

df_final.write \
    .mode("overwrite") \
    .parquet(output_path)

print(f"  discount_effectiveness written to: {output_path}")
print(f"  Total rows written: {df_final.count():,}")


# ── Job Summary ────────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"discount_effectiveness_job completed successfully")
print(f"Output : {output_path}")
print(f"  Total orders              : {total_orders:,}")
print(f"  Promotional orders        : {promotional_orders:,} ({promo_pct:.1f}%)")
print(f"  Estimated discount total  : ${total_discount:,.2f}")
print(f"  Order types breakdown shown above")
print(f"{'='*60}")

job.commit()

