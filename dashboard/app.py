# app.py
# GlobalPartners Business Insights Dashboard
# Main entry point — renders the home/overview page
# and configures sidebar navigation.

import streamlit as st
import plotly.graph_objects as go
from utils.data_loader import (
    load_clv, load_rfm, load_churn,
    load_sales_trends, load_loyalty, load_locations
)

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="GlobalPartners | Business Insights",
    page_icon="🍽️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Sidebar ────────────────────────────────────────────────────────────────────
st.sidebar.image("https://img.icons8.com/fluency/96/restaurant.png", width=60)
st.sidebar.title("GlobalPartners")
st.sidebar.caption("Business Insights Dashboard")
st.sidebar.divider()
st.sidebar.markdown("""
**Navigation**
Use the pages below to explore each metric area.
""")

# ── Header ─────────────────────────────────────────────────────────────────────
st.title("🍽️ GlobalPartners — Business Insights")
st.caption("Restaurant chain performance across 28 locations | Data: Apr 2020 – Feb 2024")
st.divider()

# ── Load all data for KPI cards ────────────────────────────────────────────────
with st.spinner("Loading dashboard data..."):
    df_clv      = load_clv()
    df_rfm      = load_rfm()
    df_churn    = load_churn()
    df_trends   = load_sales_trends()
    df_loyalty  = load_loyalty()
    df_location = load_locations()

# Getting some sample data in the logs to examine
print("=" * 110)
print(f"Sample `df_clv`: \n{df_clv.head(5)}\n")
print(f"Sample `df_rfm`: \n{df_rfm.head(5)}\n")
print(f"Sample `df_churn`: \n{df_churn.head(5)}\n")
print(f"Sample `df_trends`: \n{df_trends.head(5)}\n")
print(f"Sample `df_loyalty`: \n{df_loyalty.head(5)}\n")
print(f"Sample `df_location`: \n{df_location.head(5)}\n")
print("=" * 110)

# ── KPI Cards Row 1 ────────────────────────────────────────────────────────────
st.subheader("📊 Key Business Metrics")

col1, col2, col3, col4 = st.columns(4)

with col1:
    total_customers = df_clv["user_id"].nunique() if not df_clv.empty else 0
    st.metric("Total Customers", f"{total_customers:,}")

with col2:
    # total_revenue_to_date is a cumulative value. That's why take the max()
    total_revenue = df_clv.groupby("user_id")["total_revenue_to_date"].max().sum()
    st.metric("Total Revenue", f"${total_revenue:,.0f}")

with col3:
    avg_clv = df_clv.groupby("user_id")["total_revenue_to_date"].max().mean()
    st.metric("Avg Customer LTV", f"${avg_clv:,.2f}")

with col4:
    churn_count = df_churn["churn_risk_flag"].sum() if not df_churn.empty else 0
    churn_pct   = churn_count / len(df_churn) * 100 if not df_churn.empty else 0
    st.metric("At-Risk Customers", f"{churn_count:,}", delta=f"{churn_pct:.1f}% of base", delta_color="inverse")

# ── KPI Cards Row 2 ────────────────────────────────────────────────────────────
col5, col6, col7, col8 = st.columns(4)

with col5:
    vip_count = len(df_rfm[df_rfm["rfm_segment"] == "VIP"]) if not df_rfm.empty else 0
    st.metric("VIP Customers", f"{vip_count:,}")

with col6:
    loyalty_pct = df_churn["churn_risk_flag"].count()
    loyalty_customers = len(df_loyalty[df_loyalty["is_loyalty"] == True]) if not df_loyalty.empty else 0
    loyalty_row = df_loyalty[df_loyalty["is_loyalty"] == True]
    loyalty_avg_clv = loyalty_row["avg_clv"].values[0] if not loyalty_row.empty else 0
    st.metric("Loyalty Member Avg CLV", f"${loyalty_avg_clv:,.2f}")

with col7:
    top_location = df_location[df_location["revenue_rank"] == 1]["restaurant_id"].values[0] if not df_location.empty else "N/A"
    top_revenue  = df_location[df_location["revenue_rank"] == 1]["total_revenue"].values[0] if not df_location.empty else 0
    st.metric("Top Location Revenue", f"${top_revenue:,.0f}")

with col8:
    total_orders = df_trends["order_count"].sum() if not df_trends.empty else 0
    st.metric("Total Orders", f"{total_orders:,}")

st.divider()

# ── CLV Tier Distribution ──────────────────────────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("💎 CLV Tier Distribution")
    if not df_clv.empty:
        # Get each customer's final (most recent) CLV tier
        latest_clv = df_clv.sort_values("snapshot_date").groupby("user_id").last().reset_index()
        tier_counts = latest_clv["clv_tier"].value_counts()
        #print(f"tier_counts:\n{tier_counts}")
        fig = go.Figure(go.Pie(
            labels=tier_counts.index,
            values=tier_counts.values,
            hole=0.4,
            marker_colors=["#FFD700", "#C0C0C0", "#CD7F32"],
        ))
        fig.update_layout(
            margin=dict(t=20, b=20, l=20, r=20),
            height=280,
            showlegend=True,
        )
        st.plotly_chart(fig, width='stretch')

with col_right:
    st.subheader("🎯 Recency*Frequency*Monetary Segment Distribution")
    if not df_rfm.empty:
        seg_counts = df_rfm["rfm_segment"].value_counts()
        colors = {
            "VIP": "#2ecc71",
            "Regular": "#3498db",
            "New Customer": "#f39c12",
            "Churn Risk": "#e74c3c",
        }
        fig2 = go.Figure(go.Bar(
            x=seg_counts.index,
            y=seg_counts.values,
            marker_color=[colors.get(s, "#95a5a6") for s in seg_counts.index],
            text=seg_counts.values,
            textposition="outside",
        ))
        fig2.update_layout(
            margin=dict(t=20, b=20, l=20, r=20),
            height=280,
            xaxis_title="",
            yaxis_title="Customers",
            showlegend=False,
        )
        st.plotly_chart(fig2, width='stretch')

st.divider()

# ── Revenue Trend Sparkline ────────────────────────────────────────────────────
st.subheader("📈 Monthly Revenue Trend")
if not df_trends.empty:
    monthly = df_trends.groupby(
        df_trends["order_date"].dt.to_period("M")
    )["total_revenue"].sum().reset_index()
    monthly["order_date"] = monthly["order_date"].dt.to_timestamp()

    fig3 = go.Figure(go.Scatter(
        x=monthly["order_date"],
        y=monthly["total_revenue"],
        mode="lines",
        fill="tozeroy",
        line=dict(color="#2E75B6", width=2),
        fillcolor="rgba(46, 117, 182, 0.15)",
    ))
    fig3.update_layout(
        margin=dict(t=10, b=30, l=40, r=20),
        height=220,
        xaxis_title="",
        yaxis_title="Revenue ($)",
        yaxis_tickprefix="$",
        yaxis_tickformat=",.0f",
    )
    st.plotly_chart(fig3, width='stretch')

st.caption("Data refreshes every hour. Pipeline runs daily at 2:00 AM UTC.")