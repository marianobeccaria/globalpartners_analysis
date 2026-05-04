# exploratory_data_analysis.py
# GlobalPartners Business Insights - Step 1: Exploratory Data Analysis
# Purpose: Understand schema, data quality, relationships, and key patterns
# across order_items, order_item_options, and date_dim tables.

import pandas as pd
import numpy as np
import os

# ============================================================
# CONFIGURATION — update this path to match your local folder
# ============================================================
DATA_DIR = "data/"  # relative path to your csv files

ORDER_ITEMS_FILE        = os.path.join("..", DATA_DIR, "order_items.csv")
ORDER_ITEM_OPTIONS_FILE = os.path.join("..", DATA_DIR, "order_item_options.csv")
DATE_DIM_FILE           = os.path.join("..", DATA_DIR, "date_dim.csv")


# %% ============================================================
# CELL 1 — Load Raw Data
# Purpose: Load all three CSVs into dataframes.
#          Always load once into _raw and work from copies.
# ============================================================

df_items_raw    = pd.read_csv(ORDER_ITEMS_FILE)
df_options_raw  = pd.read_csv(ORDER_ITEM_OPTIONS_FILE)
df_date_raw     = pd.read_csv(DATE_DIM_FILE)

# Normalize all column names to lowercase
df_items_raw.columns   = df_items_raw.columns.str.lower()
df_options_raw.columns = df_options_raw.columns.str.lower()
df_date_raw.columns    = df_date_raw.columns.str.lower()

print("Files loaded successfully.")
print(f"   order_items        : {df_items_raw.shape[0]:,} rows x {df_items_raw.shape[1]} cols")
print(f"   order_item_options : {df_options_raw.shape[0]:,} rows x {df_options_raw.shape[1]} cols")
print(f"   date_dim           : {df_date_raw.shape[0]:,} rows x {df_date_raw.shape[1]} cols")


# %% ============================================================
# CELL 2 — Schema Inspection
# Purpose: Check column names, data types, and a sample of rows
#          for each table. First step to understand what we have.
# ============================================================

df_items   = df_items_raw.copy()
df_options = df_options_raw.copy()
df_date    = df_date_raw.copy()

print("=" * 60)
print("ORDER ITEMS — Schema")
print("=" * 60)
print(df_items.dtypes)
print("\nSample rows:")
print(df_items.head(3).to_string())

print("\n" + "=" * 60)
print("ORDER ITEM OPTIONS — Schema")
print("=" * 60)
print(df_options.dtypes)
print("\nSample rows:")
print(df_options.head(3).to_string())

print("\n" + "=" * 60)
print("DATE DIM — Schema")
print("=" * 60)
print(df_date.dtypes)
print("\nSample rows:")
print(df_date.head(3).to_string())


# %% ============================================================
# CELL 3 — Null Value Analysis
# Purpose: Identify missing values per column.
#          Nulls in key columns like user_id, order_id, or item_price
#          would break CLV and RFM calculations downstream.
# ============================================================

def null_report(df, name):
    null_counts = df.isnull().sum()
    null_pct    = (null_counts / len(df) * 100).round(2)
    report = pd.DataFrame({"null_count": null_counts, "null_%": null_pct})
    report = report[report["null_count"] > 0].sort_values("null_%", ascending=False)
    print(f"\n{'=' * 60}")
    print(f"NULL REPORT — {name} ({len(df):,} rows)")
    print(f"{'=' * 60}")
    if report.empty:
        print("  No nulls found.")
    else:
        print(report.to_string())

null_report(df_items,   "order_items")
null_report(df_options, "order_item_options")
null_report(df_date,    "date_dim")


# %% ============================================================
# CELL 4 — Duplicate Analysis
# Purpose: Detect fully duplicated rows and duplicate primary keys.
#          Duplicates silently inflate revenue totals and customer counts.
# ============================================================

print("=" * 60)
print("DUPLICATE ANALYSIS")
print("=" * 60)

# Full row duplicates
for df, name in [(df_items, "order_items"), (df_options, "order_item_options"), (df_date, "date_dim")]:
    dups = df.duplicated().sum()
    print(f"  {name}: {dups:,} fully duplicated rows")

# Key-level duplicates
print()
lineitem_dups = df_items.duplicated(subset=["order_id", "lineitem_id"]).sum()
print(f"  order_items — duplicate (order_id, lineitem_id) pairs : {lineitem_dups:,}")

option_dups = df_options.duplicated(subset=["order_id", "lineitem_id", "option_name"]).sum()
print(f"  order_item_options — duplicate (order_id, lineitem_id, option_name) : {option_dups:,}")

date_dups = df_date.duplicated(subset=["date_key"]).sum()
print(f"  date_dim — duplicate date_key values : {date_dups:,}")


# %% ============================================================
# CELL 5 — Cardinality & Unique Value Counts
# Purpose: Understand the scale of key dimensions.
#          How many customers, orders, restaurants, platforms?
# ============================================================

print("=" * 60)
print("CARDINALITY — order_items")
print("=" * 60)

key_cols = ["user_id", "order_id", "restaurant_id", "app_name", "currency",
            "item_category", "item_name", "is_loyalty"]

for col in key_cols:
    if col in df_items.columns:
        print(f"  {col:25s}: {df_items[col].nunique():,} unique values")


# %% ============================================================
# CELL 6 — Numeric Column Statistics
# Purpose: Check value ranges for price, quantity columns.
#          Negative item_price or zero quantities are red flags.
#          Negative option_price is expected (discounts).
# ============================================================

print("=" * 60)
print("NUMERIC STATS — order_items")
print("=" * 60)
num_cols_items = ["item_price", "item_quantity"]
print(df_items[num_cols_items].describe().round(4).to_string())

print("\n" + "=" * 60)
print("NUMERIC STATS — order_item_options")
print("=" * 60)
num_cols_options = ["option_price", "option_quantity"]
print(df_options[num_cols_options].describe().round(4).to_string())

# Flag negative item prices (unexpected)
neg_prices = df_items[df_items["item_price"] < 0]
print(f"\n  Negative item_price rows : {len(neg_prices):,}")

# Flag negative option prices (expected — these are discounts)
neg_option_prices = df_options[df_options["option_price"] < 0]
print(f"  ℹNegative option_price rows (discounts) : {len(neg_option_prices):,}")


# %% ============================================================
# CELL 7 — Date Range Analysis
# Purpose: Understand the time coverage of the dataset.
#          Needed to set correct RFM reference date and
#          validate date_dim coverage matches order data.
# ============================================================

df_items["creation_time_utc"] = pd.to_datetime(df_items["creation_time_utc"], format='ISO8601')

print("=" * 60)
print("DATE RANGE ANALYSIS")
print("=" * 60)
print(f"  order_items date range   : {df_items['creation_time_utc'].min()} → {df_items['creation_time_utc'].max()}")
print(f"  date_dim date range      : {df_date['date_key'].min()} → {df_date['date_key'].max()}")
print(f"  Total days in order data : {(df_items['creation_time_utc'].max() - df_items['creation_time_utc'].min()).days:,}")

# %% ============================================================
# CELL 8 — Join Quality: order_items, order_item_options
# Purpose: Verify how well the two transactional tables join.
#          Unmatched records = options with no parent order (data issue)
#          or orders with no options (expected for simple items).
# ============================================================

print("=" * 60)
print("JOIN QUALITY — order_items ↔ order_item_options")
print("=" * 60)

items_keys   = set(zip(df_items["order_id"], df_items["lineitem_id"]))
options_keys = set(zip(df_options["order_id"], df_options["lineitem_id"]))

matched     = options_keys & items_keys
unmatched   = options_keys - items_keys
match_rate  = len(matched) / len(options_keys) * 100

print(f"  order_item_options keys            : {len(options_keys):,}")
print(f"  Matched to order_items             : {len(matched):,}")
print(f"  Unmatched (orphan options)         : {len(unmatched):,}")
print(f"  Match rate                         : {match_rate:.2f}%")


# %% ============================================================
# CELL 9 — Join Quality: order_items ↔ date_dim
# Purpose: Check whether every order date exists in date_dim.
#          Missing dates would cause silent drops in time-based aggregations.
# ============================================================

print("=" * 60)
print("JOIN QUALITY — order_items ↔ date_dim")
print("=" * 60)

df_items["order_date"] = df_items["creation_time_utc"].dt.date.astype(str)
df_date["date_key"]    = df_date["date_key"].astype(str)

order_dates  = set(df_items["order_date"].unique())
dim_dates    = set(df_date["date_key"].unique())

matched_dates   = order_dates & dim_dates
unmatched_dates = order_dates - dim_dates
date_match_rate = len(matched_dates) / len(order_dates) * 100

print(f"  Unique order dates                 : {len(order_dates):,}")
print(f"  Matched to date_dim                : {len(matched_dates):,}")
print(f"  Unmatched (missing in date_dim)    : {len(unmatched_dates):,}")
print(f"  Match rate                         : {date_match_rate:.2f}%")

if unmatched_dates:
    print(f"\n  Sample unmatched dates: {sorted(list(unmatched_dates))[:5]}")


# %% ============================================================
# CELL 10 — Loyalty Flag Distribution
# Purpose: Understand the split between loyalty and non-loyalty customers.
#          This directly affects the Loyalty Program Impact metric (Step 5).
# ============================================================

print("=" * 60)
print("LOYALTY FLAG DISTRIBUTION — order_items")
print("=" * 60)

loyalty_dist = df_items["is_loyalty"].value_counts(dropna=False)
loyalty_pct  = df_items["is_loyalty"].value_counts(normalize=True, dropna=False) * 100

summary = pd.DataFrame({"count": loyalty_dist, "%": loyalty_pct.round(2)})
print(summary.to_string())


# %% ============================================================
# CELL 11 — Top Categories, Platforms, and Restaurants
# Purpose: Quick look at key dimensions to spot dominant values,
#          data entry inconsistencies, or unexpected categories.
# ============================================================

print("=" * 60)
print("TOP 10 — item_category")
print("=" * 60)
print(df_items["item_category"].value_counts().head(10).to_string())

print("\n" + "=" * 60)
print("TOP 10 — app_name (ordering platform)")
print("=" * 60)
print(df_items["app_name"].value_counts().head(10).to_string())

print("\n" + "=" * 60)
print("TOP 10 — restaurant_id (by order volume)")
print("=" * 60)
print(df_items["restaurant_id"].value_counts().head(10).to_string())


# %% ============================================================
# CELL 12 — EDA Summary
# Purpose: Print a consolidated findings summary to guide
#          architecture decisions and data model design.
# ============================================================

print("=" * 60)
print("EDA SUMMARY")
print("=" * 60)
print(f"""
TABLE SIZES
  order_items        : {len(df_items):,} rows
  order_item_options : {len(df_options):,} rows
  date_dim           : {len(df_date):,} rows

KEY DIMENSIONS
  Unique customers   : {df_items['user_id'].nunique():,}
  Unique orders      : {df_items['order_id'].nunique():,}
  Unique restaurants : {df_items['restaurant_id'].nunique():,}
  Unique platforms   : {df_items['app_name'].nunique():,}

DATE COVERAGE
  Orders span        : {df_items['creation_time_utc'].min().date()} → {df_items['creation_time_utc'].max().date()}

LOYALTY SPLIT
  Loyalty members    : {(df_items['is_loyalty'] == True).sum():,} rows
  Non-members        : {(df_items['is_loyalty'] == False).sum():,} rows

DISCOUNTS DETECTED
  option_price < 0   : {len(neg_option_prices):,} rows in order_item_options
""")
print("EDA complete. Review findings above before proceeding to architecture design.")

# %%
