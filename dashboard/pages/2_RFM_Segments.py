# pages/2_RFM_Segments.py
# GlobalPartners Dashboard — RFM Segments Page
# Purpose: Visualize customer behavioral segments based on
#          Recency, Frequency, and Monetary scores.
#          Helps marketing identify which customers to target
#          and with what type of campaign.

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.data_loader import load_rfm

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Recency Frequency Monetary Segments | GlobalPartners",
    page_icon="🎯",
    layout="wide",
)

st.title("🎯 Customer Segmentation — RFM Analysis")
st.caption(
    "Customers grouped by Recency (how recently they ordered), "
    "Frequency (how often), and Monetary (how much they spend). "
    "Use these segments to target the right customers with the right message."
)
st.divider()

# ── Load data ──────────────────────────────────────────────────────────────────
df = load_rfm()

if df.empty:
    st.error("No RFM data available. Please run the pipeline first.")
    st.stop()

# ── Sidebar filters ────────────────────────────────────────────────────────────
st.sidebar.header("Filters")

segments = ["All"] + sorted(df["rfm_segment"].unique().tolist())
selected_segment = st.sidebar.selectbox("Segment", segments)

score_range = st.sidebar.slider(
    "RFM Score Range",
    min_value=int(df["rfm_score"].min()),
    max_value=int(df["rfm_score"].max()),
    value=(int(df["rfm_score"].min()), int(df["rfm_score"].max())),
)

recency_max = st.sidebar.slider(
    "Max Recency (days since last order)",
    min_value=int(df["recency_days"].min()),
    max_value=int(df["recency_days"].max()),
    value=int(df["recency_days"].max()),
)

# ── Apply filters ──────────────────────────────────────────────────────────────
df_filtered = df.copy()

if selected_segment != "All":
    df_filtered = df_filtered[df_filtered["rfm_segment"] == selected_segment]

df_filtered = df_filtered[
    (df_filtered["rfm_score"] >= score_range[0]) &
    (df_filtered["rfm_score"] <= score_range[1])
]

df_filtered = df_filtered[df_filtered["recency_days"] <= recency_max]

# ── KPI Cards ──────────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

seg_counts = df_filtered["rfm_segment"].value_counts()

with col1:
    st.metric("Total Customers", f"{len(df_filtered):,}")
with col2:
    vip = seg_counts.get("VIP", 0)
    vip_pct = vip / len(df_filtered) * 100 if len(df_filtered) > 0 else 0
    st.metric("VIP Customers", f"{vip:,}", delta=f"{vip_pct:.1f}% of filtered")
with col3:
    churn = seg_counts.get("Churn Risk", 0)
    churn_pct = churn / len(df_filtered) * 100 if len(df_filtered) > 0 else 0
    st.metric("Churn Risk", f"{churn:,}", delta=f"{churn_pct:.1f}% of filtered", delta_color="inverse")
with col4:
    avg_monetary = df_filtered["monetary"].mean()
    st.metric("Avg Spend (6 months)", f"${avg_monetary:,.2f}")

st.divider()

# ── Row 1: Segment Distribution + Segment Profiles ────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Segment Distribution")

    # Color map consistent across all charts
    color_map = {
        "VIP":          "#2ecc71",
        "Regular":      "#3498db",
        "New Customer": "#f39c12",
        "Churn Risk":   "#e74c3c",
    }

    seg_df = df_filtered["rfm_segment"].value_counts().reset_index()
    seg_df.columns = ["Segment", "Count"]

    fig_bar = go.Figure(go.Bar(
        x=seg_df["Segment"],
        y=seg_df["Count"],
        marker_color=[color_map.get(s, "#95a5a6") for s in seg_df["Segment"]],
        text=seg_df["Count"],
        textposition="outside",
    ))
    fig_bar.update_layout(
        height=340,
        margin=dict(t=10, b=10, l=10, r=10),
        xaxis_title="",
        yaxis_title="Number of Customers",
        showlegend=False,
    )
    st.plotly_chart(fig_bar, width='stretch')

with col_right:
    st.subheader("Segment Profiles — Avg RFM Metrics")
    st.caption("Average Recency, Frequency, and Monetary value per segment.")

    # Aggregate avg metrics per segment
    profile = df_filtered.groupby("rfm_segment").agg(
        Avg_Recency=("recency_days", "mean"),
        Avg_Frequency=("frequency", "mean"),
        Avg_Monetary=("monetary", "mean"),
        Customers=("user_id", "count"),
    ).reset_index()
    profile.columns = ["Segment", "Avg Recency (days)", "Avg Frequency", "Avg Spend ($)", "Customers"]
    profile["Avg Recency (days)"] = profile["Avg Recency (days)"].round(1)
    profile["Avg Frequency"]      = profile["Avg Frequency"].round(1)
    profile["Avg Spend ($)"]      = profile["Avg Spend ($)"].round(2)
    profile = profile.sort_values("Avg Spend ($)", ascending=False)

    st.dataframe(
        profile,
        width='stretch',
        hide_index=True,
        height=340,
    )

st.divider()

# ── Row 2: RFM Score Distribution + Scatter ───────────────────────────────────
col_hist, col_scatter = st.columns(2)

with col_hist:
    st.subheader("RFM Score Distribution by Segment")
    st.caption(
        "Score ranges from 3 (worst) to 12 (best). "
        "Box shows the middle 50% of customers. "
        "Line inside = median score."
    )

    fig_box = px.box(
        df_filtered,
        x="rfm_segment",
        y="rfm_score",
        color="rfm_segment",
        color_discrete_map=color_map,
        points="outliers",    # show only outlier dots, not every customer
        category_orders={     # consistent left-to-right order
            "rfm_segment": ["VIP", "Regular", "New Customer", "Churn Risk"]
        },
    )
    fig_box.update_layout(
        height=320,
        margin=dict(t=10, b=10, l=10, r=10),
        xaxis_title="",
        yaxis_title="RFM Score (3–12)",
        yaxis_range=[2, 13],   # give a little breathing room above/below
        showlegend=False,      # color already identifies segment on x-axis
    )
    st.plotly_chart(fig_box, width='stretch')

with col_scatter:
    st.subheader("Frequency vs Monetary by Segment")
    st.caption("Each dot is a customer. Size = RFM score. Higher right = more valuable.")

    # Cap axes at 95th percentile to avoid outliers compressing the view
    # Outliers (e.g. bulk/catering orders) would push all other customers
    # into a tiny corner making segments invisible
    freq_cap = df_filtered["frequency"].quantile(0.95)
    mon_cap  = df_filtered["monetary"].quantile(0.95)

    df_plot = df_filtered[
        (df_filtered["frequency"] <= freq_cap) &
        (df_filtered["monetary"]  <= mon_cap)
    ].copy()

    outlier_count = len(df_filtered) - len(df_plot)

    fig_scatter = go.Figure()

    for segment, group in df_plot.groupby("rfm_segment"):
        fig_scatter.add_trace(go.Scatter(
            x=group["frequency"],
            y=group["monetary"],
            mode="markers",
            name=segment,
            marker=dict(
                color=color_map.get(segment, "#95a5a6"),
                size=5,          # fixed small size so no segment gets buried
                opacity=0.5,
                line=dict(width=0.3, color="white"),
            ),
            text=group.apply(
                lambda r: f"Recency: {r['recency_days']} days<br>RFM Score: {r['rfm_score']}",
                axis=1
            ),
            hovertemplate=(
                "<b>%{fullData.name}</b><br>"
                "Frequency: %{x}<br>"
                "Monetary: $%{y:,.2f}<br>"
                "%{text}<extra></extra>"
            ),
        ))

    fig_scatter.update_layout(
        height=320,
        margin=dict(t=10, b=10, l=10, r=10),
        xaxis_title="Frequency (orders in last 6 months)",
        yaxis_title="Monetary (spend in last 6 months $)",
        legend_title="Segment",
    )

    st.plotly_chart(fig_scatter, width='stretch')

    if outlier_count > 0:
        st.caption(
            f"ℹ️ {outlier_count} outlier customers hidden (above 95th percentile) "
            f"to keep the chart readable. Likely bulk/catering orders."
        )

st.divider()

# ── Row 3: R / F / M Score Breakdown per Segment ──────────────────────────────
st.subheader("📊 Recency, Frequency, Monetary (RFM) Score Breakdown by Segment")
st.caption(
    "Average individual scores per segment (1=worst, 4=best). "
    "Helps identify which dimension is driving each segment's classification."
)

score_profile = df_filtered.groupby("rfm_segment").agg(
    R=("r_score", "mean"),
    F=("f_score", "mean"),
    M=("m_score", "mean"),
).reset_index()

# Melt to long format for grouped bar chart
# Wide: one column per score → Long: one row per score per segment
score_melted = score_profile.melt(
    id_vars="rfm_segment",
    value_vars=["R", "F", "M"],
    var_name="Dimension",
    value_name="Avg Score",
)

fig_grouped = px.bar(
    score_melted,
    x="rfm_segment",
    y="Avg Score",
    color="Dimension",
    barmode="group",
    color_discrete_map={"R": "#e74c3c", "F": "#3498db", "M": "#2ecc71"},
    text_auto=".2f",
)
fig_grouped.update_layout(
    height=320,
    margin=dict(t=10, b=10, l=10, r=10),
    xaxis_title="",
    yaxis_title="Avg Score (1–4)",
    yaxis_range=[0, 4.5],
    legend_title="RFM Dimension",
)
st.plotly_chart(fig_grouped, width='stretch')

st.divider()

# ── Row 4: Marketing Action Guide ─────────────────────────────────────────────
st.subheader("📋 Marketing Action Guide")
st.caption("Recommended actions per segment based on RFM profile.")

actions = pd.DataFrame({
    "Segment": ["VIP", "Regular", "New Customer", "Churn Risk"],
    "Profile": [
        "Ordered recently, often, and spends a lot",
        "Active but not top tier across all dimensions",
        "Ordered recently but infrequently",
        "Hasn't ordered in a while, low frequency",
    ],
    "Recommended Action": [
        "Reward with exclusive offers, early access, loyalty perks",
        "Upsell with bundle deals, encourage loyalty sign-up",
        "Onboard with welcome series, promote loyalty program",
        "Re-engage with win-back campaign, time-limited discount",
    ],
    "Priority": ["🔴 High", "🟡 Medium", "🟢 Monitor", "🔴 High"],
    "Customers": [
        seg_counts.get("VIP", 0),
        seg_counts.get("Regular", 0),
        seg_counts.get("New Customer", 0),
        seg_counts.get("Churn Risk", 0),
    ],
})

st.dataframe(actions, width='stretch', hide_index=True)