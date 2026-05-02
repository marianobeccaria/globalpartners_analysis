# pages/5_Loyalty_Impact.py
# GlobalPartners Dashboard — Loyalty Program Impact Page
# Purpose: Compare key KPIs between loyalty members and non-members
#          to evaluate the ROI of the loyalty program and identify
#          opportunities to grow membership.

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.data_loader import load_loyalty, load_clv

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Loyalty Impact | GlobalPartners",
    page_icon="🎖️",
    layout="wide",
)

st.title("🎖️ Loyalty Program Impact")
st.caption(
    "How does loyalty membership affect customer spending and engagement? "
    "Use this page to evaluate program ROI and identify conversion opportunities."
)
st.divider()

# ── Load data ──────────────────────────────────────────────────────────────────
df_loyalty = load_loyalty()
df_clv     = load_clv()

if df_loyalty.empty:
    st.error("No loyalty data available. Please run the pipeline first.")
    st.stop()

# ── Separate loyalty vs non-loyalty rows ──────────────────────────────────────
loyalty_row    = df_loyalty[df_loyalty["is_loyalty"] == True]
non_loyalty_row = df_loyalty[df_loyalty["is_loyalty"] == False]

print("=" * 110)
print(f"\nLoyalty: \n{loyalty_row}\n")
print(f"NonLoyalty: \n{non_loyalty_row}\n")

# Extract scalar values safely
def get_val(df_row, col, default=0):
    return df_row[col].values[0] if not df_row.empty else default

loy_customers    = get_val(loyalty_row,     "total_customers")
non_customers    = get_val(non_loyalty_row, "total_customers")
total_customers  = loy_customers + non_customers
loy_pct          = loy_customers / total_customers * 100 if total_customers > 0 else 0

loy_avg_clv      = get_val(loyalty_row,     "avg_clv")
non_avg_clv      = get_val(non_loyalty_row, "avg_clv")
clv_lift         = ((loy_avg_clv - non_avg_clv) / non_avg_clv * 100) if non_avg_clv > 0 else 0

loy_avg_orders   = get_val(loyalty_row,     "avg_orders_per_customer")
non_avg_orders   = get_val(non_loyalty_row, "avg_orders_per_customer")

loy_avg_order_val = get_val(loyalty_row,     "avg_order_value")
non_avg_order_val = get_val(non_loyalty_row, "avg_order_value")

loy_repeat_rate  = get_val(loyalty_row,     "repeat_purchase_rate_pct")
non_repeat_rate  = get_val(non_loyalty_row, "repeat_purchase_rate_pct")

# ── KPI Cards ──────────────────────────────────────────────────────────────────
st.subheader("📊 Program Overview")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Loyalty Members",
        f"{loy_customers:,}",
        delta=f"{loy_pct:.1f}% of customer base",
    )
with col2:
    st.metric(
        "CLV Lift from Loyalty",
        f"{clv_lift:+.1f}%",
        delta="vs non-members",
        delta_color="normal" if clv_lift >= 0 else "inverse",
        help="How much more lifetime value loyalty members generate vs non-members."
    )
with col3:
    st.metric(
        "Loyalty Avg Orders",
        f"{loy_avg_orders:.1f}",
        delta=f"{loy_avg_orders - non_avg_orders:+.1f} vs non-members",
        delta_color="normal",
    )
with col4:
    st.metric(
        "Loyalty Repeat Rate",
        f"{loy_repeat_rate:.1f}%",
        delta=f"{loy_repeat_rate - non_repeat_rate:+.1f}% vs non-members",
        delta_color="normal",
    )

st.divider()

# ── Row 1: Membership Split + CLV Comparison ──────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Membership Split")
    st.caption("What proportion of the customer base are loyalty members?")

    fig_donut = go.Figure(go.Pie(
        labels=["Loyalty Members", "Non-Members"],
        values=[loy_customers, non_customers],
        hole=0.45,
        marker_colors=["#2ecc71", "#95a5a6"],
        textinfo="label+percent+value",
    ))
    fig_donut.update_layout(
        height=300,
        margin=dict(t=10, b=10, l=10, r=10),
        showlegend=False,
    )
    st.plotly_chart(fig_donut, width='stretch')

with col_right:
    st.subheader("Avg CLV: Loyalty vs Non-Members")
    st.caption("How much more do loyalty members spend over their lifetime?")

    fig_clv = go.Figure(go.Bar(
        x=["Non-Members", "Loyalty Members"],
        y=[non_avg_clv, loy_avg_clv],
        marker_color=["#95a5a6", "#2ecc71"],
        text=[f"${non_avg_clv:,.2f}", f"${loy_avg_clv:,.2f}"],
        textposition="outside",
        width=0.4,
    ))
    fig_clv.update_layout(
        height=300,
        margin=dict(t=10, b=10, l=10, r=10),
        yaxis_title="Avg Lifetime Revenue ($)",
        yaxis_tickprefix="$",
        yaxis_tickformat=",.0f",
        xaxis_title="",
        showlegend=False,
    )
    st.plotly_chart(fig_clv, width='stretch')

st.divider()

# ── Row 2: Side by Side KPI Comparison ────────────────────────────────────────
st.subheader("📋 Head-to-Head KPI Comparison")
st.caption("All key metrics compared side by side.")

comparison_df = pd.DataFrame({
    "Metric": [
        "Avg CLV ($)",
        "Avg Orders per Customer",
        "Avg Order Value ($)",
        "Repeat Purchase Rate (%)",
        "Total Customers",
    ],
    "Loyalty Members": [
        f"${loy_avg_clv:,.2f}",
        f"{loy_avg_orders:.1f}",
        f"${loy_avg_order_val:,.2f}",
        f"{loy_repeat_rate:.1f}%",
        f"{loy_customers:,}",
    ],
    "Non-Members": [
        f"${non_avg_clv:,.2f}",
        f"{non_avg_orders:.1f}",
        f"${non_avg_order_val:,.2f}",
        f"{non_repeat_rate:.1f}%",
        f"{non_customers:,}",
    ],
    "Loyalty Advantage": [
        f"{clv_lift:+.1f}%",
        f"{loy_avg_orders - non_avg_orders:+.1f}",
        f"{loy_avg_order_val - non_avg_order_val:+.2f}",
        f"{loy_repeat_rate - non_repeat_rate:+.1f}%",
        "—",
    ],
})

st.dataframe(comparison_df, width='stretch', hide_index=True)

st.divider()

# ── Row 3: Grouped Bar — All KPIs side by side ────────────────────────────────
st.subheader("📊 KPI Comparison — Visual")
st.caption(
    "Normalized comparison of key metrics. "
    "Each metric scaled to 100 for non-members — bars above 100 mean "
    "loyalty members outperform."
)

# Normalize each metric to non-member baseline = 100
metrics = {
    "Avg CLV":           (loy_avg_clv,       non_avg_clv),
    "Avg Orders":        (loy_avg_orders,     non_avg_orders),
    "Avg Order Value":   (loy_avg_order_val,  non_avg_order_val),
    "Repeat Rate":       (loy_repeat_rate,    non_repeat_rate),
}

normalized = []
for metric, (loy_val, non_val) in metrics.items():
    baseline = non_val if non_val > 0 else 1
    normalized.append({
        "Metric":  metric,
        "Group":   "Non-Members",
        "Value":   100.0,
    })
    normalized.append({
        "Metric":  metric,
        "Group":   "Loyalty Members",
        "Value":   round(loy_val / baseline * 100, 1),
    })

norm_df = pd.DataFrame(normalized)

fig_grouped = px.bar(
    norm_df,
    x="Metric",
    y="Value",
    color="Group",
    barmode="group",
    color_discrete_map={
        "Loyalty Members": "#2ecc71",
        "Non-Members":     "#95a5a6",
    },
    text="Value",
)
fig_grouped.update_traces(texttemplate="%{text:.0f}", textposition="outside")
fig_grouped.add_hline(
    y=100,
    line_dash="dash",
    line_color="#c0392b",
    annotation_text="Non-member baseline (100)",
    annotation_position="bottom right",
)
fig_grouped.update_layout(
    height=350,
    margin=dict(t=10, b=10, l=10, r=10),
    xaxis_title="",
    yaxis_title="Index (Non-member = 100)",
    legend_title="Group",
)
st.plotly_chart(fig_grouped, width='stretch')

st.divider()

# ── Row 4: CLV Distribution — Loyalty vs Non ──────────────────────────────────
st.subheader("💎 CLV Distribution — Loyalty vs Non-Members")
st.caption(
    "Distribution of individual customer lifetime values. "
    "Shows whether loyalty members cluster at higher CLV values."
)

if not df_clv.empty:
    # Get each customer's final CLV and loyalty status
    latest_clv = df_clv.sort_values("snapshot_date") \
        .groupby("user_id").last().reset_index()

    # Cap at 95th percentile to avoid outlier compression
    clv_cap = latest_clv["total_revenue_to_date"].quantile(0.95)
    latest_clv_plot = latest_clv[latest_clv["total_revenue_to_date"] <= clv_cap]

    fig_dist = px.box(
        latest_clv_plot,
        x="is_loyalty",
        y="total_revenue_to_date",
        color="is_loyalty",
        color_discrete_map={True: "#2ecc71", False: "#95a5a6"},
        points="outliers",
        labels={
            "is_loyalty":             "Loyalty Member",
            "total_revenue_to_date":  "Lifetime Revenue ($)",
        },
    )
    fig_dist.update_layout(
        height=320,
        margin=dict(t=10, b=10, l=10, r=10),
        xaxis_ticktext=["Non-Members", "Loyalty Members"],
        xaxis_tickvals=[False, True],
        xaxis_title="",
        yaxis_title="Lifetime Revenue ($)",
        yaxis_tickprefix="$",
        yaxis_tickformat=",.0f",
        showlegend=False,
    )
    st.plotly_chart(fig_dist, width='stretch')

st.divider()

# ── Business Insight ───────────────────────────────────────────────────────────
st.subheader("💡 Key Takeaways")

col_t1, col_t2, col_t3 = st.columns(3)

with col_t1:
    st.info(
        f"**Program Reach**\n\n"
        f"Only **{loy_pct:.1f}%** of customers are loyalty members. "
        f"With **{non_customers:,}** non-members, there is significant "
        f"room to grow program enrollment."
    )
with col_t2:
    st.success(
        f"**Revenue Impact**\n\n"
        f"Loyalty members generate **{clv_lift:+.1f}%** more lifetime "
        f"revenue than non-members. Converting even 10% of non-members "
        f"would meaningfully grow total revenue."
    )
with col_t3:
    st.warning(
        f"**Engagement Gap**\n\n"
        f"Loyalty members place **{loy_avg_orders:.1f}** orders on average "
        f"vs **{non_avg_orders:.1f}** for non-members. "
        f"Focus re-engagement campaigns on high-spend non-members first."
    )
