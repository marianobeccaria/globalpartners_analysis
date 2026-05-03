# pages/7_Discount_Effectiveness.py
# GlobalPartners Dashboard — Discount Effectiveness Page
# Purpose: Since no explicit discount codes exist in the dataset,
#          this page surfaces promotional pricing signals using:
#   Option 1 — Zero-price items (free item giveaways)
#   Option 2 — Below-average price items (promotional pricing proxy)
#
# Helps marketing and operations understand where and when
# promotional pricing occurs and its impact on basket size.

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.data_loader import load_discount

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Discount Effectiveness | GlobalPartners",
    page_icon="🏷️",
    layout="wide",
)

st.title("🏷️ Pricing & Discount Effectiveness")
st.caption(
    "No explicit discount codes were found in the dataset. "
    "This analysis uses two proxy signals to identify promotional pricing: "
    "zero-price items (free giveaways) and items priced below their category average. "
    "Use this page to understand where promotional pricing occurs and its revenue impact."
)
st.divider()

# ── Load data ──────────────────────────────────────────────────────────────────
df = load_discount()

# Debugging to view what's being presented to streamlit

print("=" * 110)
# with pd.option_context(
#     "display.max_columns", None,
#     "display.width", 2000,
#     "display.max_colwidth", None,
# ):
#     print("=" * 110)
#     print("\nDiscounts sample:")
#     print(df.head(20).to_string(index=False))

print(df.head(20).T.to_string())
# print(f"\nDiscounts: \n{df.head(20)}\n")

if df.empty:
    st.error("No discount data available. Please run the pipeline first.")
    st.stop()

# ── Parse dates ───────────────────────────────────────────────────────────────
df["order_date"] = pd.to_datetime(df["order_date"])

# ── Sidebar filters ────────────────────────────────────────────────────────────
st.sidebar.header("Filters")

date_range = st.sidebar.date_input(
    "Date Range",
    value=[df["order_date"].min(), df["order_date"].max()],
    min_value=df["order_date"].min(),
    max_value=df["order_date"].max(),
)

order_types = ["All"] + sorted(df["order_type"].unique().tolist())
selected_type = st.sidebar.selectbox("Order Type", order_types)

loyalty_filter = st.sidebar.radio(
    "Loyalty Status",
    ["All", "Loyalty Members", "Non-Members"],
)

# ── Apply filters ──────────────────────────────────────────────────────────────
df_filtered = df.copy()

if len(date_range) == 2:
    df_filtered = df_filtered[
        (df_filtered["order_date"] >= pd.Timestamp(date_range[0])) &
        (df_filtered["order_date"] <= pd.Timestamp(date_range[1]))
    ]

if selected_type != "All":
    df_filtered = df_filtered[df_filtered["order_type"] == selected_type]

if loyalty_filter == "Loyalty Members":
    df_filtered = df_filtered[df_filtered["is_loyalty"] == True]
elif loyalty_filter == "Non-Members":
    df_filtered = df_filtered[df_filtered["is_loyalty"] == False]

# ── KPI Cards ──────────────────────────────────────────────────────────────────
# total_orders      = len(df_filtered)
# promo_orders      = df_filtered["is_promotional_order"].sum()
# promo_pct         = promo_orders / total_orders * 100 if total_orders > 0 else 0
# total_discount    = df_filtered["total_estimated_discount"].sum()
# avg_promo_basket  = df_filtered[df_filtered["is_promotional_order"]]["gross_revenue"].mean()
# avg_full_basket   = df_filtered[~df_filtered["is_promotional_order"]]["gross_revenue"].mean()
# basket_lift       = ((avg_promo_basket - avg_full_basket) / avg_full_basket * 100
#                      if avg_full_basket > 0 else 0)

total_orders      = len(df_filtered)
promo_orders      = df_filtered["is_promotional_order"].sum()
promo_pct         = promo_orders / total_orders * 100 if total_orders > 0 else 0
total_discount    = df_filtered["total_estimated_discount"].sum()

promo_baskets = df_filtered[df_filtered["is_promotional_order"]]["gross_revenue"]
full_baskets  = df_filtered[~df_filtered["is_promotional_order"]]["gross_revenue"]

avg_promo_basket = promo_baskets.mean()
avg_full_basket  = full_baskets.mean()

has_both_basket_groups = (
    not promo_baskets.empty
    and not full_baskets.empty
    and pd.notna(avg_promo_basket)
    and pd.notna(avg_full_basket)
    and avg_full_basket > 0
)

basket_lift = (
    (avg_promo_basket - avg_full_basket) / avg_full_basket * 100
    if has_both_basket_groups
    else None
)

restaurant_labels = {
    restaurant_id: f"Loc-{str(restaurant_id)[-6:]}"
    for restaurant_id in sorted(df["restaurant_id"].dropna().unique())
}

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Orders", f"{total_orders:,}")
with col2:
    st.metric(
        "Promotional Orders",
        f"{promo_orders:,}",
        delta=f"{promo_pct:.1f}% of total",
        delta_color="off",
    )
with col3:
    st.metric(
        "Estimated Discount Total",
        f"${total_discount:,.2f}",
        help="Sum of estimated discount value across all promotional orders."
    )
# with col4:
#     st.metric(
#         "Promo vs Full-Price Basket",
#         f"{basket_lift:+.1f}%",
#         delta="promotional basket vs full-price",
#         delta_color="normal" if basket_lift >= 0 else "inverse",
#         help="Are promotional orders generating larger baskets than full-price orders?"
#     )

with col4:
    st.metric(
        "Promo vs Full-Price Basket",
        f"{basket_lift:+.1f}%" if basket_lift is not None else "N/A",
        delta=(
            "promotional basket vs full-price"
            if basket_lift is not None
            else "requires both order types"
        ),
        delta_color=(
            "normal"
            if basket_lift is not None and basket_lift >= 0
            else "inverse"
        ),
        help="Are promotional orders generating larger baskets than full-price orders?"
    )


st.divider()

# ── Row 1: Order Type Distribution + Promotional Rate Over Time ───────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Order Type Breakdown")
    st.caption("Distribution of full-price vs promotional order types.")

    type_counts = df_filtered["order_type"].value_counts().reset_index()
    type_counts.columns = ["Order Type", "Count"]

    color_map = {
        "full_price":          "#2ecc71",
        "free_item":           "#e74c3c",
        "below_avg_price":     "#f39c12",
        "free_item+below_avg": "#8e44ad",
    }

    fig_donut = go.Figure(go.Pie(
        labels=type_counts["Order Type"],
        values=type_counts["Count"],
        hole=0.45,
        marker_colors=[color_map.get(t, "#95a5a6") for t in type_counts["Order Type"]],
        textinfo="label+percent+value",
    ))
    fig_donut.update_layout(
        height=320,
        margin=dict(t=10, b=10, l=10, r=10),
        showlegend=False,
    )
    st.plotly_chart(fig_donut, width='stretch')

with col_right:
    st.subheader("Promotional Order Rate Over Time")
    st.caption("Monthly % of orders flagged as promotional.")

    df_filtered["month_start"] = df_filtered["order_date"].dt.to_period("M").dt.to_timestamp()
    monthly = df_filtered.groupby("month_start").agg(
        total=("order_id", "count"),
        promo=("is_promotional_order", "sum"),
    ).reset_index()
    monthly["promo_rate"] = (monthly["promo"] / monthly["total"] * 100).round(2)

    fig_rate = go.Figure(go.Scatter(
        x=monthly["month_start"],
        y=monthly["promo_rate"],
        mode="lines+markers",
        line=dict(color="#f39c12", width=2),
        fill="tozeroy",
        fillcolor="rgba(243,156,18,0.1)",
        hovertemplate="<b>%{x}</b><br>Promo Rate: %{y:.1f}%<extra></extra>",
    ))
    fig_rate.update_layout(
        height=320,
        margin=dict(t=10, b=10, l=40, r=20),
        xaxis_title="",
        yaxis_title="Promotional Order Rate (%)",
        yaxis_ticksuffix="%",
    )
    st.plotly_chart(fig_rate, width='stretch')

st.divider()

# ── Row 2: Basket Size Comparison ─────────────────────────────────────────────
st.subheader("💰 Basket Size: Promotional vs Full-Price Orders")
st.caption(
    "Do promotional orders generate larger or smaller baskets? "
    "Capped at 95th percentile to remove bulk/catering order outliers."
)

# Cap at 95th percentile to remove bulk order outliers
revenue_cap = df_filtered["gross_revenue"].quantile(0.95)
df_plot     = df_filtered[df_filtered["gross_revenue"] <= revenue_cap].copy()
df_plot["Order Type"] = df_plot["is_promotional_order"].map(
    {True: "Promotional", False: "Full Price"}
)

col_box, col_bar = st.columns(2)

with col_box:
    fig_box = px.box(
        df_plot,
        x="Order Type",
        y="gross_revenue",
        color="Order Type",
        color_discrete_map={
            "Promotional": "#f39c12",
            "Full Price":  "#2ecc71",
        },
        points="outliers",
    )
    fig_box.update_layout(
        height=320,
        margin=dict(t=10, b=10, l=10, r=10),
        xaxis_title="",
        yaxis_title="Order Revenue ($)",
        yaxis_tickprefix="$",
        yaxis_tickformat=",.0f",
        showlegend=False,
    )
    st.plotly_chart(fig_box, width='stretch')

# with col_bar:
#     # Avg basket by order type
#     avg_basket = df_plot.groupby("Order Type")["gross_revenue"].mean().reset_index()
#     avg_basket.columns = ["Order Type", "Avg Basket ($)"]

#     fig_avg = go.Figure(go.Bar(
#         x=avg_basket["Order Type"],
#         y=avg_basket["Avg Basket ($)"],
#         marker_color=["#f39c12", "#2ecc71"],
#         text=[f"${v:,.2f}" for v in avg_basket["Avg Basket ($)"]],
#         textposition="outside",
#         width=0.4,
#     ))
#     fig_avg.update_layout(
#         height=320,
#         margin=dict(t=10, b=10, l=10, r=10),
#         xaxis_title="",
#         yaxis_title="Avg Order Revenue ($)",
#         yaxis_tickprefix="$",
#         yaxis_tickformat=",.0f",
#         showlegend=False,
#     )
#     st.plotly_chart(fig_avg, width='stretch')

with col_bar:
    # Avg basket by order type
    basket_color_map = {
        "Promotional": "#f39c12",
        "Full Price":  "#2ecc71",
    }

    avg_basket = (
        df_plot.groupby("Order Type", as_index=False)["gross_revenue"]
        .mean()
        .rename(columns={"gross_revenue": "Avg Basket ($)"})
    )

    avg_basket["Order Type"] = pd.Categorical(
        avg_basket["Order Type"],
        categories=["Full Price", "Promotional"],
        ordered=True,
    )
    avg_basket = avg_basket.sort_values("Order Type")

    fig_avg = go.Figure(go.Bar(
        x=avg_basket["Order Type"],
        y=avg_basket["Avg Basket ($)"],
        marker_color=[
            basket_color_map.get(order_type, "#95a5a6")
            for order_type in avg_basket["Order Type"]
        ],
        text=[f"${v:,.2f}" for v in avg_basket["Avg Basket ($)"]],
        textposition="outside",
        width=0.4,
    ))
    fig_avg.update_layout(
        height=320,
        margin=dict(t=10, b=10, l=10, r=10),
        xaxis_title="",
        yaxis_title="Avg Order Revenue ($)",
        yaxis_tickprefix="$",
        yaxis_tickformat=",.0f",
        showlegend=False,
    )
    st.plotly_chart(fig_avg, width='stretch')

st.divider()

# ── Row 3: Promotional Orders by Location + Loyalty ───────────────────────────
col_loc, col_loy = st.columns(2)

with col_loc:
    st.subheader("Promotional Orders by Restaurant Location")
    st.caption("Which restaurants have the highest promotional order rate?")

    loc_promo = df_filtered.groupby("restaurant_id").agg(
        total=("order_id", "count"),
        promo=("is_promotional_order", "sum"),
    ).reset_index()

    loc_promo["promo_rate"] = (loc_promo["promo"] / loc_promo["total"] * 100).round(2)
    
    loc_promo["location_label"] = loc_promo["restaurant_id"].map(restaurant_labels)

    # loc_promo["location_label"] = (
    #     "Loc-"
    #     + loc_promo["restaurant_id"].astype(str).str[:10]
    # )
    
    loc_promo = loc_promo.sort_values("promo_rate", ascending=True).tail(15)

    fig_loc = go.Figure(go.Bar(
        x=loc_promo["promo_rate"],
        y=loc_promo["location_label"],
        orientation="h",
        marker_color="#f39c12",
        text=[f"{v:.1f}%" for v in loc_promo["promo_rate"]],
        textposition="outside",
    ))
    fig_loc.update_layout(
        height=400,
        margin=dict(t=10, b=10, l=10, r=60),
        xaxis_title="Promotional Order Rate (%)",
        xaxis_ticksuffix="%",
        yaxis_title="",
    )
    st.plotly_chart(fig_loc, width='stretch')

with col_loy:
    st.subheader("Promotional Orders: Loyalty vs Non-Members")
    st.caption("Do loyalty members receive more promotional pricing?")

    loyalty_color_map = {
        "Loyalty Members": "#2ecc71",
        "Non-Members":     "#95a5a6",
    }

    loy_promo = df_filtered.groupby("is_loyalty").agg(
        total=("order_id", "count"),
        promo=("is_promotional_order", "sum"),
        avg_discount=("total_estimated_discount", "mean"),
    ).reset_index()

    loy_promo["promo_rate"] = (
        loy_promo["promo"] / loy_promo["total"] * 100
    ).round(2)

    loy_promo["Group"] = loy_promo["is_loyalty"].map(
        {True: "Loyalty Members", False: "Non-Members"}
    )

    loy_promo["Group"] = pd.Categorical(
        loy_promo["Group"],
        categories=["Non-Members", "Loyalty Members"],
        ordered=True,
    )
    loy_promo = loy_promo.sort_values("Group")

    fig_loy = go.Figure(go.Bar(
        x=loy_promo["Group"],
        y=loy_promo["promo_rate"],
        marker_color=[
            loyalty_color_map.get(group, "#95a5a6")
            for group in loy_promo["Group"]
        ],
        text=[f"{v:.1f}%" for v in loy_promo["promo_rate"]],
        textposition="outside",
        width=0.4,
    ))
    fig_loy.update_layout(
        height=400,
        margin=dict(t=10, b=10, l=10, r=10),
        xaxis_title="",
        yaxis_title="Promotional Order Rate (%)",
        xaxis_ticksuffix="%",
        showlegend=False,
    )
    st.plotly_chart(fig_loy, width='stretch')

st.divider()

# ── Row 4: Estimated Discount Detail Table ─────────────────────────────────────
st.subheader("📋 Promotional Order Detail")
st.caption(
    "Top promotional orders ranked by estimated discount value. "
    "Useful for identifying patterns in high-discount orders."
)

promo_table = df_filtered[df_filtered["is_promotional_order"] == True] \
    .sort_values("total_estimated_discount", ascending=False) \
    .head(50)[[
        "order_id", "order_date", "restaurant_id", "is_loyalty",
        "gross_revenue", "order_type", "free_item_count",
        "below_avg_price_item_count", "total_estimated_discount",
    ]].copy()

promo_table.columns = [
    "Order ID", "Date", "Restaurant", "Loyalty Member",
    "Gross Revenue", "Order Type", "Free Items",
    "Below-Avg Items", "Est. Discount ($)",
]

promo_table["Order ID"]      = promo_table["Order ID"].astype(str).str[:12] + "..."
#promo_table["Restaurant"]    = "Loc-" + promo_table["Restaurant"].astype(str).str[:6]
promo_table["Restaurant"] = promo_table["Restaurant"].map(restaurant_labels)
promo_table["Gross Revenue"] = promo_table["Gross Revenue"].apply(lambda x: f"${x:,.2f}")
promo_table["Est. Discount ($)"] = promo_table["Est. Discount ($)"].apply(lambda x: f"${x:,.2f}")

st.dataframe(promo_table, width='stretch', hide_index=True)
st.caption(f"Showing top 50 of {df_filtered['is_promotional_order'].sum():,} promotional orders.")

st.divider()

# ── Methodology Note ───────────────────────────────────────────────────────────
with st.expander("ℹ️ Methodology — How promotional orders are identified"):
    st.markdown("""
    Since no explicit discount codes exist in this dataset, two proxy signals are used:

    **Signal 1 — Free Items (Option 1)**
    Orders containing at least one item with `item_price = $0.00` are flagged as
    promotional. The estimated discount is calculated as the category average price
    multiplied by the quantity — representing the revenue foregone by giving the item free.

    **Signal 2 — Below-Average Price Items (Option 2)**
    Items priced more than one standard deviation below their category average price
    are flagged as potentially promotional. The estimated discount is the price gap
    multiplied by quantity ordered.

    **Limitations**
    - These are proxy signals, not confirmed discounts
    - Zero-price items may represent data entry errors rather than intentional promotions
    - Below-average pricing may reflect legitimate lower-tier menu items, not discounts
    - Results should be validated against POS system promotional records when available
    """)