# pages/1_CLV.py
# GlobalPartners Dashboard — Customer Lifetime Value Page
# Purpose: Show how each customer's cumulative revenue evolves over time,
#          CLV tier distribution, and top customer performance.

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.data_loader import load_clv

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="CLV | GlobalPartners",
    page_icon="💎",
    layout="wide",
)

st.title("💎 Customer Lifetime Value")
st.caption("How much total revenue does each customer generate over their relationship with us?")
st.divider()

# ── Load data ──────────────────────────────────────────────────────────────────
df = load_clv()

if df.empty:
    st.error("No CLV data available. Please run the pipeline first.")
    st.stop()

# ── Precompute: each customer's latest snapshot ────────────────────────────────
# Takes the most recent snapshot per customer to get their
# current CLV tier and total revenue — used in KPI cards and tables
latest = df.sort_values("snapshot_date").groupby("user_id").last().reset_index()

# ── Sidebar filters ────────────────────────────────────────────────────────────
st.sidebar.header("Filters")

tiers = ["All"] + sorted(latest["clv_tier"].unique().tolist())
selected_tier = st.sidebar.selectbox("CLV Tier", tiers)

loyalty_filter = st.sidebar.radio(
    "Loyalty Status",
    ["All", "Loyalty Members", "Non-Members"]
)

date_range = st.sidebar.date_input(
    "Date Range",
    value=[df["snapshot_date"].min(), df["snapshot_date"].max()],
    min_value=df["snapshot_date"].min(),
    max_value=df["snapshot_date"].max(),
)

# ── Apply filters ──────────────────────────────────────────────────────────────
df_filtered  = df.copy()
lat_filtered = latest.copy()

if selected_tier != "All":
    lat_filtered = lat_filtered[lat_filtered["clv_tier"] == selected_tier]
    df_filtered  = df_filtered[df_filtered["user_id"].isin(lat_filtered["user_id"])]

if loyalty_filter == "Loyalty Members":
    lat_filtered = lat_filtered[lat_filtered["is_loyalty"] == True]
    df_filtered  = df_filtered[df_filtered["user_id"].isin(lat_filtered["user_id"])]
elif loyalty_filter == "Non-Members":
    lat_filtered = lat_filtered[lat_filtered["is_loyalty"] == False]
    df_filtered  = df_filtered[df_filtered["user_id"].isin(lat_filtered["user_id"])]

if len(date_range) == 2:
    df_filtered = df_filtered[
        (df_filtered["snapshot_date"] >= pd.Timestamp(date_range[0])) &
        (df_filtered["snapshot_date"] <= pd.Timestamp(date_range[1]))
    ]

# ── KPI Cards ─────────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Customers",
        f"{lat_filtered['user_id'].nunique():,}"
    )
with col2:
    avg_clv = lat_filtered["total_revenue_to_date"].mean()
    st.metric("Avg CLV", f"${avg_clv:,.2f}")
with col3:
    median_clv = lat_filtered["total_revenue_to_date"].median()
    st.metric("Median CLV", f"${median_clv:,.2f}")
with col4:
    top10_revenue = lat_filtered.nlargest(10, "total_revenue_to_date")["total_revenue_to_date"].sum()
    st.metric("Top 10 Customers Revenue", f"${top10_revenue:,.0f}")

st.divider()

# ── Row 1: CLV Tier Donut + Avg CLV by Tier ───────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("CLV Tier Breakdown")
    tier_counts = lat_filtered["clv_tier"].value_counts()
    fig_donut = go.Figure(go.Pie(
        labels=tier_counts.index,
        values=tier_counts.values,
        hole=0.45,
        marker_colors=["#FFD700", "#C0C0C0", "#CD7F32"],
        textinfo="label+percent",
    ))
    fig_donut.update_layout(
        height=320,
        margin=dict(t=10, b=10, l=10, r=10),
        showlegend=False,
    )
    st.plotly_chart(fig_donut, use_container_width=True)

with col_right:
    st.subheader("Avg CLV by Tier")
    tier_avg = lat_filtered.groupby("clv_tier")["total_revenue_to_date"].mean().reset_index()
    tier_avg.columns = ["CLV Tier", "Avg Revenue"]
    tier_avg = tier_avg.sort_values("Avg Revenue", ascending=True)

    color_map = {"High": "#FFD700", "Medium": "#C0C0C0", "Low": "#CD7F32"}
    fig_bar = go.Figure(go.Bar(
        x=tier_avg["Avg Revenue"],
        y=tier_avg["CLV Tier"],
        orientation="h",
        marker_color=[color_map.get(t, "#3498db") for t in tier_avg["CLV Tier"]],
        text=[f"${v:,.2f}" for v in tier_avg["Avg Revenue"]],
        textposition="outside",
    ))
    fig_bar.update_layout(
        height=320,
        margin=dict(t=10, b=10, l=10, r=80),
        xaxis_title="Avg Lifetime Revenue ($)",
        yaxis_title="",
        xaxis_tickprefix="$",
        xaxis_tickformat=",.0f",
    )
    st.plotly_chart(fig_bar, use_container_width=True)

st.divider()

# ── Row 2: CLV Growth Over Time ────────────────────────────────────────────────
st.subheader("📈 Average CLV Growth Over Time")
st.caption("Average cumulative revenue per customer — shows how the customer base's value builds day by day.")

daily_avg = df_filtered.groupby("snapshot_date")["total_revenue_to_date"].mean().reset_index()
daily_avg.columns = ["Date", "Avg CLV"]

fig_line = go.Figure(go.Scatter(
    x=daily_avg["Date"],
    y=daily_avg["Avg CLV"],
    mode="lines",
    line=dict(color="#2E75B6", width=2),
    fill="tozeroy",
    fillcolor="rgba(46,117,182,0.1)",
))
fig_line.update_layout(
    height=300,
    margin=dict(t=10, b=30, l=40, r=20),
    xaxis_title="",
    yaxis_title="Avg CLV ($)",
    yaxis_tickprefix="$",
    yaxis_tickformat=",.0f",
)
st.plotly_chart(fig_line, use_container_width=True)

st.divider()

# ── Row 3: Loyalty vs Non-Loyalty CLV ─────────────────────────────────────────
col_loy, col_top = st.columns(2)

with col_loy:
    st.subheader("👥 CLV: Loyalty vs Non-Members")
    loyalty_avg = lat_filtered.groupby("is_loyalty")["total_revenue_to_date"].mean().reset_index()
    loyalty_avg["Group"] = loyalty_avg["is_loyalty"].map({True: "Loyalty Members", False: "Non-Members"})

    fig_loy = go.Figure(go.Bar(
        x=loyalty_avg["Group"],
        y=loyalty_avg["total_revenue_to_date"],
        marker_color=["#2ecc71", "#95a5a6"],
        text=[f"${v:,.2f}" for v in loyalty_avg["total_revenue_to_date"]],
        textposition="outside",
    ))
    fig_loy.update_layout(
        height=300,
        margin=dict(t=10, b=10, l=10, r=10),
        yaxis_title="Avg CLV ($)",
        yaxis_tickprefix="$",
        yaxis_tickformat=",.0f",
        xaxis_title="",
        showlegend=False,
    )
    st.plotly_chart(fig_loy, use_container_width=True)

with col_top:
    st.subheader("🏆 Top 10 Customers by CLV")
    top10 = lat_filtered.nlargest(10, "total_revenue_to_date")[
        ["user_id", "total_revenue_to_date", "clv_tier", "is_loyalty"]
    ].copy()
    top10.columns = ["Customer ID", "Total Revenue", "CLV Tier", "Loyalty Member"]
    top10["Total Revenue"] = top10["Total Revenue"].apply(lambda x: f"${x:,.2f}")
    top10["Customer ID"] = top10["Customer ID"].astype(str).str[:12] + "..."
    st.dataframe(top10, width='stretch', hide_index=True)
    
