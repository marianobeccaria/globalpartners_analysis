# pages/3_Churn_Risk.py
# GlobalPartners Dashboard — Churn Risk Page
# Purpose: Surface customers at risk of churning based on
#          inactivity thresholds and spend trend indicators.
#          Helps marketing prioritize re-engagement campaigns.

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.data_loader import load_churn

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Churn Risk | GlobalPartners",
    page_icon="⚠️",
    layout="wide",
)

st.title("⚠️ Churn Risk Indicators")
st.caption(
    "Customers flagged as at-risk based on days since last order, "
    "order frequency, and spend trend. Use this page to prioritize "
    "re-engagement campaigns before customers are lost."
)
st.divider()

# ── Load data ──────────────────────────────────────────────────────────────────
df = load_churn()

if df.empty:
    st.error("No churn data available. Please run the pipeline first.")
    st.stop()

# ── Sidebar filters ────────────────────────────────────────────────────────────
st.sidebar.header("Filters")

churn_threshold = st.sidebar.slider(
    "Churn Threshold (days inactive)",
    min_value=15,
    max_value=180,
    value=45,
    step=5,
    help="Customers inactive beyond this threshold are flagged as at-risk."
)

min_orders = st.sidebar.number_input(
    "Min Lifetime Orders",
    min_value=1,
    max_value=50,
    value=1,
    help="Filter to customers with at least this many lifetime orders."
)

show_at_risk_only = st.sidebar.checkbox(
    "Show at-risk customers only",
    value=False,
)

# ── Apply dynamic churn flag based on sidebar threshold ───────────────────────
# The pipeline computed churn_risk_flag at 45 days.
# Here we recompute it dynamically based on the slider
# so the business user can explore different thresholds interactively.
df_filtered = df.copy()
df_filtered["churn_risk_flag"] = df_filtered["days_since_last_order"] > churn_threshold
df_filtered = df_filtered[df_filtered["total_orders"] >= min_orders]

if show_at_risk_only:
    df_filtered = df_filtered[df_filtered["churn_risk_flag"] == True]

# ── KPI Cards ──────────────────────────────────────────────────────────────────
total        = len(df_filtered)
at_risk      = df_filtered["churn_risk_flag"].sum()
at_risk_pct  = at_risk / total * 100 if total > 0 else 0
safe         = total - at_risk
avg_gap      = df_filtered["avg_order_gap_days"].median()
avg_inactive = df_filtered[df_filtered["churn_risk_flag"]]["days_since_last_order"].mean()

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Customers", f"{total:,}")
with col2:
    st.metric(
        "At-Risk Customers",
        f"{at_risk:,}",
        delta=f"{at_risk_pct:.1f}% of base",
        delta_color="inverse",
    )
with col3:
    st.metric("Safe Customers", f"{safe:,}")
with col4:
    st.metric(
        "Avg Days Inactive (at-risk)",
        f"{avg_inactive:.0f} days" if not pd.isna(avg_inactive) else "N/A"
    )

st.divider()

# ── Row 1: At-Risk vs Safe Donut + Days Since Last Order Distribution ──────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("At-Risk vs Safe Customers")
    st.caption(f"Based on {churn_threshold}-day inactivity threshold.")

    fig_donut = go.Figure(go.Pie(
        labels=["At-Risk", "Safe"],
        values=[at_risk, safe],
        hole=0.45,
        marker_colors=["#e74c3c", "#2ecc71"],
        textinfo="label+percent+value",
    ))
    fig_donut.update_layout(
        height=300,
        margin=dict(t=10, b=10, l=10, r=10),
        showlegend=False,
    )
    st.plotly_chart(fig_donut, use_container_width=True)

with col_right:
    st.subheader("Days Since Last Order")
    st.caption("Distribution of customer inactivity. Red line = churn threshold.")

    fig_hist = px.histogram(
        df_filtered,
        x="days_since_last_order",
        color="churn_risk_flag",
        color_discrete_map={True: "#e74c3c", False: "#2ecc71"},
        nbins=40,
        barmode="overlay",
        opacity=0.7,
        labels={"churn_risk_flag": "At Risk", "days_since_last_order": "Days Inactive"},
    )
    # Add vertical line at churn threshold
    fig_hist.add_vline(
        x=churn_threshold,
        line_dash="dash",
        line_color="#c0392b",
        annotation_text=f"Threshold: {churn_threshold}d",
        annotation_position="top right",
    )
    fig_hist.update_layout(
        height=300,
        margin=dict(t=10, b=10, l=10, r=10),
        xaxis_title="Days Since Last Order",
        yaxis_title="Customers",
        legend_title="At Risk",
    )
    st.plotly_chart(fig_hist, use_container_width=True)

st.divider()

# ── Row 2: Spend Change % + Avg Order Gap Distribution ────────────────────────
col_spend, col_gap = st.columns(2)

with col_spend:
    st.subheader("Spend Trend — Last 90 vs Prior 90 Days")
    st.caption(
        "% change in spend between the last 90 days and the prior 90 days. "
        "Negative = spending less recently — early churn signal."
    )

    # Only customers who were active in both windows have spend_change_pct
    df_spend = df_filtered[df_filtered["spend_change_pct"].notna()].copy()

    if df_spend.empty:
        st.info("No spend trend data available for the selected filters.")
    else:
        # Bin spend change into buckets for readability
        bins   = [-200, -50, -10, 10, 50, 200]
        labels = ["< -50%", "-50% to -10%", "-10% to +10%", "+10% to +50%", "> +50%"]

        df_spend["spend_bucket"] = pd.cut(
            df_spend["spend_change_pct"],
            bins=bins,
            labels=labels,
        )

        bucket_counts = df_spend["spend_bucket"].value_counts().reindex(labels).fillna(0)

        colors = ["#c0392b", "#e74c3c", "#95a5a6", "#27ae60", "#2ecc71"]

        fig_spend = go.Figure(go.Bar(
            x=bucket_counts.index,
            y=bucket_counts.values,
            marker_color=colors,
            text=bucket_counts.values.astype(int),
            textposition="outside",
        ))
        fig_spend.update_layout(
            height=300,
            margin=dict(t=10, b=10, l=10, r=10),
            xaxis_title="Spend Change %",
            yaxis_title="Customers",
            showlegend=False,
        )
        st.plotly_chart(fig_spend, use_container_width=True)
        st.caption(
            f"Showing {len(df_spend):,} customers active in both 90-day windows. "
            f"{total - len(df_spend):,} customers had no activity in one or both windows."
        )

with col_gap:
    st.subheader("Avg Order Gap Distribution")
    st.caption(
        "How many days on average between orders. "
        "Customers with long gaps who recently went quiet are highest priority."
    )

    df_gap = df_filtered[
        df_filtered["avg_order_gap_days"].notna() &
        (df_filtered["avg_order_gap_days"] <= df_filtered["avg_order_gap_days"].quantile(0.95))
    ]

    fig_gap = px.histogram(
        df_gap,
        x="avg_order_gap_days",
        color="churn_risk_flag",
        color_discrete_map={True: "#e74c3c", False: "#2ecc71"},
        nbins=40,
        barmode="overlay",
        opacity=0.6,
        marginal="rug",
        labels={
            "avg_order_gap_days": "Avg Days Between Orders",
            "churn_risk_flag": "At Risk",
        },
    )
    fig_gap.update_layout(
        height=300,
        margin=dict(t=10, b=10, l=10, r=10),
        xaxis_title="Avg Days Between Orders",
        yaxis_title="Customers",
        legend_title="At Risk",
    )
    st.plotly_chart(fig_gap, use_container_width=True)

    # fig_gap = px.box(
    #     df_gap,
    #     x="churn_risk_flag",
    #     y="avg_order_gap_days",
    #     color="churn_risk_flag",
    #     color_discrete_map={True: "#e74c3c", False: "#2ecc71"},
    #     points="outliers",
    #     labels={
    #         "churn_risk_flag": "At Risk",
    #         "avg_order_gap_days": "Avg Days Between Orders",
    #     },
    # )
    # fig_gap.update_layout(
    #     xaxis_ticktext=["Safe", "At-Risk"],
    #     xaxis_tickvals=[False, True],
    #     showlegend=False,
    # )
    # st.plotly_chart(fig_gap, use_container_width=True)

st.divider()

# ── Row 3: Lifetime Value of At-Risk Customers ────────────────────────────────
st.subheader("💰 Revenue at Stake — At-Risk Customer Value")
st.caption(
    "Total lifetime spend of customers currently flagged as at-risk. "
    "This is the revenue the business could lose without re-engagement."
)

col_rev1, col_rev2, col_rev3 = st.columns(3)

at_risk_df   = df_filtered[df_filtered["churn_risk_flag"] == True]
safe_df      = df_filtered[df_filtered["churn_risk_flag"] == False]

with col_rev1:
    at_risk_revenue = at_risk_df["total_spend"].sum()
    st.metric(
        "Total Spend — At-Risk Customers",
        f"${at_risk_revenue:,.0f}",
        help="Lifetime revenue from customers currently flagged as at-risk."
    )
with col_rev2:
    avg_at_risk_spend = at_risk_df["total_spend"].mean()
    st.metric("Avg Spend per At-Risk Customer", f"${avg_at_risk_spend:,.2f}")
with col_rev3:
    avg_safe_spend = safe_df["total_spend"].mean()
    delta = avg_at_risk_spend - avg_safe_spend
    st.metric(
        "At-Risk vs Safe Avg Spend",
        f"${avg_at_risk_spend:,.2f}",
        delta=f"${delta:+,.2f} vs safe customers",
        delta_color="normal",
    )

# ── Spend distribution: at-risk vs safe ───────────────────────────────────────
spend_cap = df_filtered["total_spend"].quantile(0.95)
df_spend_plot = df_filtered[df_filtered["total_spend"] <= spend_cap]

fig_spend_box = px.box(
    df_spend_plot,
    x="churn_risk_flag",
    y="total_spend",
    color="churn_risk_flag",
    color_discrete_map={True: "#e74c3c", False: "#2ecc71"},
    points="outliers",
    labels={
        "churn_risk_flag": "At Risk",
        "total_spend": "Lifetime Spend ($)",
    },
)
fig_spend_box.update_layout(
    height=300,
    margin=dict(t=10, b=10, l=10, r=10),
    xaxis_ticktext=["Safe", "At-Risk"],
    xaxis_tickvals=[False, True],
    xaxis_title="",
    yaxis_title="Lifetime Spend ($)",
    showlegend=False,
)
st.plotly_chart(fig_spend_box, use_container_width=True)

st.divider()

# ── Row 4: At-Risk Customer Table ──────────────────────────────────────────────
st.subheader("📋 At-Risk Customer Detail")
st.caption(
    "Top at-risk customers ranked by lifetime spend — "
    "highest value customers to prioritize for re-engagement."
)

at_risk_table = at_risk_df.sort_values("total_spend", ascending=False)[[
    "user_id",
    "days_since_last_order",
    "total_orders",
    "total_spend",
    "avg_order_gap_days",
    "spend_change_pct",
]].copy()

at_risk_table.columns = [
    "Customer ID",
    "Days Inactive",
    "Total Orders",
    "Lifetime Spend ($)",
    "Avg Order Gap (days)",
    "Spend Change %",
]

at_risk_table["Customer ID"]       = at_risk_table["Customer ID"].astype(str).str[:12] + "..."
at_risk_table["Lifetime Spend ($)"] = at_risk_table["Lifetime Spend ($)"].apply(lambda x: f"${x:,.2f}")
at_risk_table["Spend Change %"]     = at_risk_table["Spend Change %"].apply(
    lambda x: f"{x:+.1f}%" if pd.notna(x) else "N/A"
)
at_risk_table["Avg Order Gap (days)"] = at_risk_table["Avg Order Gap (days)"].apply(
    lambda x: f"{x:.1f}" if pd.notna(x) else "N/A"
)

st.dataframe(
    at_risk_table.head(50),
    width='stretch',
    hide_index=True,
)
st.caption(f"Showing top 50 of {len(at_risk_df):,} at-risk customers by lifetime spend.")
