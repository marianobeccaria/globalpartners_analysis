# pages/4_Sales_Trends.py
# GlobalPartners Dashboard — Sales Trends Page
# Purpose: Visualize revenue trends over time broken down by
#          location, category, and calendar dimensions.
#          Helps operations and finance identify peak periods,
#          seasonal patterns, and underperforming time windows.

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.data_loader import load_sales_trends

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Sales Trends | GlobalPartners",
    page_icon="📈",
    layout="wide",
)

st.title("📈 Sales Trends & Seasonality")
st.caption(
    "Revenue patterns over time broken down by location, category, "
    "and calendar dimensions. Use this page to identify peak periods, "
    "seasonal patterns, and plan staffing and inventory."
)
st.divider()

# ── Load data ──────────────────────────────────────────────────────────────────
df = load_sales_trends()
print(df.head(10))

if df.empty:
    st.error("No sales trend data available. Please run the pipeline first.")
    st.stop()

# ── Sidebar filters ────────────────────────────────────────────────────────────
st.sidebar.header("Filters")

# Date range
date_min = df["order_date"].min()
date_max = df["order_date"].max()
date_range = st.sidebar.date_input(
    "Date Range",
    value=[date_min, date_max],
    min_value=date_min,
    max_value=date_max,
)

# Granularity selector
granularity = st.sidebar.radio(
    "Time Granularity",
    ["Daily", "Weekly", "Monthly"],
    index=2,  # default to Monthly
)

# Category filter
all_categories = sorted(df["item_category"].dropna().unique().tolist())
selected_categories = st.sidebar.multiselect(
    "Item Categories",
    options=all_categories,
    default=all_categories,
)

# Location filter
all_locations = sorted(df["restaurant_id"].unique().tolist())
selected_locations = st.sidebar.multiselect(
    "Restaurants",
    options=all_locations,
    default=all_locations,
    format_func=lambda x: f"Restaurant {str(x)[:8]}...",
)

# Weekend / holiday toggles
show_weekends  = st.sidebar.checkbox("Highlight Weekends", value=True)
show_holidays  = st.sidebar.checkbox("Highlight Holidays", value=True)

# ── Apply filters ──────────────────────────────────────────────────────────────
df_filtered = df.copy()

if len(date_range) == 2:
    df_filtered = df_filtered[
        (df_filtered["order_date"] >= pd.Timestamp(date_range[0])) &
        (df_filtered["order_date"] <= pd.Timestamp(date_range[1]))
    ]

if selected_categories:
    df_filtered = df_filtered[df_filtered["item_category"].isin(selected_categories)]

if selected_locations:
    df_filtered = df_filtered[df_filtered["restaurant_id"].isin(selected_locations)]

# ── KPI Cards ──────────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

total_revenue  = df_filtered["total_revenue"].sum()
total_orders   = df_filtered["order_count"].sum()
total_days     = df_filtered["order_date"].nunique()
avg_daily_rev  = total_revenue / total_days if total_days > 0 else 0
weekend_rev    = df_filtered[df_filtered["is_weekend"] == True]["total_revenue"].sum()
weekend_pct    = weekend_rev / total_revenue * 100 if total_revenue > 0 else 0

with col1:
    st.metric("Total Revenue", f"${total_revenue:,.0f}")
with col2:
    st.metric("Total Orders", f"{total_orders:,}")
with col3:
    st.metric("Avg Daily Revenue", f"${avg_daily_rev:,.0f}")
with col4:
    st.metric(
        "Weekend Revenue Share",
        f"{weekend_pct:.1f}%",
        help="% of total revenue generated on weekends."
    )

st.divider()

# ── Row 1: Revenue Over Time ───────────────────────────────────────────────────
st.subheader("💰 Revenue Over Time")

# Aggregate based on selected granularity
if granularity == "Daily":
    time_df = df_filtered.groupby("order_date").agg(
        total_revenue=("total_revenue", "sum"),
        order_count=("order_count", "sum"),
    ).reset_index()
    time_df["period"] = time_df["order_date"]

elif granularity == "Weekly":
    df_filtered["week_start"] = df_filtered["order_date"] - pd.to_timedelta(
        df_filtered["order_date"].dt.dayofweek, unit="D"
    )
    time_df = df_filtered.groupby("week_start").agg(
        total_revenue=("total_revenue", "sum"),
        order_count=("order_count", "sum"),
    ).reset_index()
    time_df["period"] = time_df["week_start"]

else:  # Monthly
    df_filtered["month_start"] = df_filtered["order_date"].dt.to_period("M").dt.to_timestamp()
    time_df = df_filtered.groupby("month_start").agg(
        total_revenue=("total_revenue", "sum"),
        order_count=("order_count", "sum"),
    ).reset_index()
    time_df["period"] = time_df["month_start"]

fig_trend = go.Figure()

# Revenue area line
fig_trend.add_trace(go.Scatter(
    x=time_df["period"],
    y=time_df["total_revenue"],
    mode="lines",
    name="Revenue",
    line=dict(color="#2E75B6", width=2),
    fill="tozeroy",
    fillcolor="rgba(46,117,182,0.1)",
    hovertemplate="<b>%{x}</b><br>Revenue: $%{y:,.0f}<extra></extra>",
))

fig_trend.update_layout(
    height=320,
    margin=dict(t=10, b=30, l=40, r=20),
    xaxis_title="",
    yaxis_title="Revenue ($)",
    yaxis_tickprefix="$",
    yaxis_tickformat=",.0f",
    hovermode="x unified",
)
st.plotly_chart(fig_trend, width='stretch')

st.divider()

# ── Row 2: Revenue by Category + Day of Week ──────────────────────────────────
col_cat, col_dow = st.columns(2)

with col_cat:
    st.subheader("Revenue by Category")
    st.caption("Which menu categories drive the most revenue?")

    cat_df = df_filtered.groupby("item_category")["total_revenue"].sum().reset_index()
    cat_df = cat_df.sort_values("total_revenue", ascending=True)
    cat_df["pct"] = (cat_df["total_revenue"] / cat_df["total_revenue"].sum() * 100).round(1)

    fig_cat = go.Figure(go.Bar(
        x=cat_df["total_revenue"],
        y=cat_df["item_category"],
        orientation="h",
        marker_color="#2E75B6",
        text=[f"${v:,.0f} ({p}%)" for v, p in zip(cat_df["total_revenue"], cat_df["pct"])],
        textposition="outside",
        hovertemplate="<b>%{y}</b><br>Revenue: $%{x:,.0f}<extra></extra>",
    ))
    fig_cat.update_layout(
        height=400,
        margin=dict(t=10, b=10, l=10, r=120),
        xaxis_title="Total Revenue ($)",
        yaxis_title="",
        xaxis_tickprefix="$",
        xaxis_tickformat=",.0f",
    )
    st.plotly_chart(fig_cat, width='stretch')

with col_dow:
    st.subheader("Revenue by Day of Week")
    st.caption("Which days generate the most revenue?")

    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    dow_df = df_filtered.groupby("day_of_week")["total_revenue"].sum().reset_index()
    dow_df["day_of_week"] = pd.Categorical(
        dow_df["day_of_week"],
        categories=day_order,
        ordered=True,
    )
    dow_df = dow_df.sort_values("day_of_week")

    # Color weekends differently
    bar_colors = [
        "#e74c3c" if d in ["Saturday", "Sunday"] else "#2E75B6"
        for d in dow_df["day_of_week"]
    ]

    fig_dow = go.Figure(go.Bar(
        x=dow_df["day_of_week"],
        y=dow_df["total_revenue"],
        marker_color=bar_colors,
        text=[f"${v:,.0f}" for v in dow_df["total_revenue"]],
        textposition="outside",
        hovertemplate="<b>%{x}</b><br>Revenue: $%{y:,.0f}<extra></extra>",
    ))
    fig_dow.update_layout(
        height=400,
        margin=dict(t=10, b=10, l=10, r=10),
        xaxis_title="",
        yaxis_title="Total Revenue ($)",
        yaxis_tickprefix="$",
        yaxis_tickformat=",.0f",
        showlegend=False,
    )
    # Add annotation explaining red bars
    fig_dow.add_annotation(
        text="🔴 Weekend",
        xref="paper", yref="paper",
        x=1, y=1,
        showarrow=False,
        font=dict(color="#e74c3c", size=11),
    )
    st.plotly_chart(fig_dow, width='stretch')

st.divider()

# ── Row 3: Monthly Heatmap ─────────────────────────────────────────────────────
st.subheader("📅 Monthly Revenue Heatmap")
st.caption(
    "Revenue by month and year. "
    "Darker = higher revenue. Reveals seasonality patterns across years."
)

# Pivot: rows = month name, columns = year
heatmap_df = df_filtered.copy()
heatmap_df["year"]       = heatmap_df["order_date"].dt.year
heatmap_df["month_name"] = heatmap_df["order_date"].dt.strftime("%b")
heatmap_df["month_num"]  = heatmap_df["order_date"].dt.month

monthly_pivot = heatmap_df.groupby(["month_num", "month_name", "year"])["total_revenue"] \
    .sum().reset_index()

pivot_table = monthly_pivot.pivot_table(
    index="month_name",
    columns="year",
    values="total_revenue",
    aggfunc="sum",
).fillna(0)

# Sort rows by month number
month_order = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
pivot_table = pivot_table.reindex(
    [m for m in month_order if m in pivot_table.index]
)

fig_heat = go.Figure(go.Heatmap(
    z=pivot_table.values,
    x=[str(c) for c in pivot_table.columns],
    y=pivot_table.index,
    colorscale="Blues",
    text=[[f"${v:,.0f}" for v in row] for row in pivot_table.values],
    texttemplate="%{text}",
    hovertemplate="<b>%{y} %{x}</b><br>Revenue: $%{z:,.0f}<extra></extra>",
))
fig_heat.update_layout(
    height=380,
    margin=dict(t=10, b=10, l=60, r=20),
    xaxis_title="Year",
    yaxis_title="Month",
)
st.plotly_chart(fig_heat, width='stretch')

st.divider()

# ── Row 4: Holiday Impact ──────────────────────────────────────────────────────
st.subheader("🎉 Holiday vs Regular Day Revenue")
st.caption("Do holidays drive more or less revenue than regular days?")

holiday_df = df_filtered.groupby("is_holiday").agg(
    total_revenue=("total_revenue", "sum"),
    avg_daily_revenue=("total_revenue", "mean"),
    order_count=("order_count", "sum"),
    days=("order_date", "nunique"),
).reset_index()

holiday_df["label"] = holiday_df["is_holiday"].map(
    {True: "Holiday", False: "Regular Day"}
)
holiday_df["avg_daily_revenue"] = holiday_df["avg_daily_revenue"].round(2)

col_h1, col_h2 = st.columns(2)

with col_h1:
    fig_hol = go.Figure(go.Bar(
        x=holiday_df["label"],
        y=holiday_df["avg_daily_revenue"],
        marker_color=["#f39c12", "#2E75B6"],
        text=[f"${v:,.0f}" for v in holiday_df["avg_daily_revenue"]],
        textposition="outside",
    ))
    fig_hol.update_layout(
        height=300,
        margin=dict(t=40, b=10, l=10, r=10),
        xaxis_title="",
        yaxis_title="Avg Daily Revenue ($)",
        yaxis_tickprefix="$",
        yaxis_tickformat=",.0f",
        showlegend=False,
        title="Avg Daily Revenue: Holiday vs Regular",
    )
    st.plotly_chart(fig_hol, width='stretch')

with col_h2:
    # Named holidays table
    if show_holidays:
        holiday_named = df_filtered[
            (df_filtered["is_holiday"] == True) &
            (df_filtered["holiday_name"].notna()) &
            (df_filtered["holiday_name"] != "")
        ].groupby("holiday_name").agg(
            total_revenue=("total_revenue", "sum"),
            order_count=("order_count", "sum"),
        ).reset_index().sort_values("total_revenue", ascending=False)

        holiday_named.columns = ["Holiday", "Total Revenue", "Orders"]
        holiday_named["Total Revenue"] = holiday_named["Total Revenue"].apply(
            lambda x: f"${x:,.0f}"
        )

        st.markdown("**Revenue by Named Holiday**")
        if holiday_named.empty:
            st.info("No named holiday data available in the selected range.")
        else:
            st.dataframe(
                holiday_named,
                width='stretch',
                hide_index=True,
            )
