# data_loader.py
# GlobalPartners Dashboard — Data Loader
# Purpose: Read Gold Parquet tables from S3 into pandas DataFrames.
#          Uses st.cache_data so each table is only fetched once
#          per session — subsequent page loads are instant.

import boto3
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import io
import os
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

BUCKET      = os.getenv("S3_BUCKET_NAME", "mbeccaria-dea-globalpartners")
GOLD_PREFIX = "gold"


def _read_parquet_from_s3(table_name: str) -> pd.DataFrame:
    """Read all Parquet part files from a Gold table prefix into one DataFrame."""
    s3     = boto3.client("s3")
    prefix = f"{GOLD_PREFIX}/{table_name}/"

    # List all parquet files under the prefix
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    if "Contents" not in response:
        return pd.DataFrame()

    parts = []
    for obj in response["Contents"]:
        key = obj["Key"]
        if not key.endswith(".parquet"):
            continue
        # Read each part file into memory
        buf      = io.BytesIO()
        s3.download_fileobj(BUCKET, key, buf)
        buf.seek(0)
        parts.append(pq.read_table(buf).to_pandas())

    if not parts:
        return pd.DataFrame()

    return pd.concat(parts, ignore_index=True)


# ── Cached loaders — one per Gold table ───────────────────────────────────────
# st.cache_data caches the result for the duration of the session.
# ttl=3600 means data refreshes every hour if the app stays running.

@st.cache_data(ttl=3600, show_spinner="Loading CLV data...")
def load_clv() -> pd.DataFrame:
    df = _read_parquet_from_s3("customer_clv_daily")
    if not df.empty:
        df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
    return df


@st.cache_data(ttl=3600, show_spinner="Loading RFM segments...")
def load_rfm() -> pd.DataFrame:
    return _read_parquet_from_s3("customer_rfm_segments")


@st.cache_data(ttl=3600, show_spinner="Loading churn indicators...")
def load_churn() -> pd.DataFrame:
    df = _read_parquet_from_s3("customer_churn_indicators")
    if not df.empty:
        df["first_order_date"] = pd.to_datetime(df["first_order_date"])
        df["last_order_date"]  = pd.to_datetime(df["last_order_date"])
    return df


@st.cache_data(ttl=3600, show_spinner="Loading sales trends...")
def load_sales_trends() -> pd.DataFrame:
    df = _read_parquet_from_s3("sales_trends")
    if not df.empty:
        df["order_date"] = pd.to_datetime(df["order_date"])
    return df


@st.cache_data(ttl=3600, show_spinner="Loading loyalty data...")
def load_loyalty() -> pd.DataFrame:
    return _read_parquet_from_s3("loyalty_comparison")


@st.cache_data(ttl=3600, show_spinner="Loading location data...")
def load_locations() -> pd.DataFrame:
    return _read_parquet_from_s3("location_performance")

@st.cache_data(ttl=3600, show_spinner="Loading discount data...")
def load_discount() -> pd.DataFrame:
    df = _read_parquet_from_s3("discount_effectiveness")
    if not df.empty:
        df["order_date"] = pd.to_datetime(df["order_date"])
    return df