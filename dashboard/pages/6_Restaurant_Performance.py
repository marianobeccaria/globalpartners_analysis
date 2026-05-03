# pages/6_Restaurnat_Performance.py
# GlobalPartners Dashboard — Restaurant Performance Page
# Purpose: Rank all 28 restaurant locations by revenue and surface
#          operational metrics to identify top and bottom performers.
#          Helps leadership make decisions about expansion, staffing,
#          and targeted promotions.

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.data_loader import load_locations, load_sales_trends

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Restuarnat Performance | GlobalPartners",
    page_icon="📍",
    layout="wide",
)

st.title("📍 Restaurant Performance")
st.divider()

# ── Load data ──────────────────────────────────────────────────────────────────
df          = load_locations()
df_trends   = load_sales_trends()

print("=" * 110)
print(f"\nRestaurant: \n{df}\n")
print(f"'df_trends': \n{df_trends.head()}\n")

if df.empty:
    st.error("No location data available. Please run the pipeline first.")
    st.stop()

# ── Derived constants — computed after data is loaded ─────────────────────────
total_locations = len(df)
n_compare       = min(5, len(df) // 2)

st.caption(
    f"Revenue ranking and operational metrics across all {total_locations} restaurant locations. "
    "Use this page to identify top performers, underperformers, and expansion opportunities."
)
# ── Shorten location IDs for display ──────────────────────────────────────────
# Restaurant IDs are long MongoDB ObjectIds — truncate for readability
df["location_label"] = "Loc-" + df["restaurant_id"].astype(str).str[:6]

# ── Sidebar filters ────────────────────────────────────────────────────────────
st.sidebar.header("Filters")

tiers = ["All"] + sorted(df["performance_tier"].unique().tolist())
selected_tier = st.sidebar.selectbox("Performance Tier", tiers)

top_n = st.sidebar.slider(
    "Show Top N Restaurnats",
    min_value=5,
    max_value=len(df),
    value=len(df),
    step=1,
)

sort_by = st.sidebar.radio(
    "Sort By",
    ["Total Revenue", "Avg Order Value", "Total Orders", "Unique Customers"],
)

# ── Apply filters ──────────────────────────────────────────────────────────────
df_filtered = df.copy()

if selected_tier != "All":
    df_filtered = df_filtered[df_filtered["performance_tier"] == selected_tier]

# Map sort_by label to column name
sort_col_map = {
    "Total Revenue":     "total_revenue",
    "Avg Order Value":   "avg_order_value",
    "Total Orders":      "total_orders",
    "Unique Customers":  "unique_customers",
}
sort_col = sort_col_map[sort_by]

df_filtered = df_filtered.sort_values(sort_col, ascending=False).head(top_n)

# ── KPI Cards ──────────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

top_location    = df[df["revenue_rank"] == 1].iloc[0]
bottom_location = df[df["revenue_rank"] == df["revenue_rank"].max()].iloc[0]
avg_rev         = df["total_revenue"].mean()
top_performers  = len(df[df["performance_tier"] == "Top Performer"])

with col1:
    st.metric(
        "Top Restaurants Revenue",
        f"${top_location['total_revenue']:,.0f}",
        delta=f"Rank #1 — {top_location['location_label']}",
    )
with col2:
    st.metric(
        "Avg Revenue per Restaurant",
        f"${avg_rev:,.0f}",
    )
with col3:
    rev_gap = top_location["total_revenue"] - bottom_location["total_revenue"]
    st.metric(
        "Top vs Bottom Gap",
        f"${rev_gap:,.0f}",
        delta="revenue difference",
        delta_color="off",
        help="Revenue difference between the highest and lowest performing locations."
    )
with col4:
    st.metric(
        "Top Performers",
        f"{top_performers} restaurants",
        delta=f"top {top_performers} by revenue rank",
    )

st.divider()

# ── Row 1: Revenue Ranking Bar Chart ──────────────────────────────────────────
st.subheader("🏆 Revenue Ranking — All Restaurnats")
st.caption("Sorted by selected metric. Color indicates performance tier.")

tier_colors = {
    "Top Performer":   "#2ecc71",
    "Mid Tier":        "#3498db",
    "Needs Attention": "#e74c3c",
}

fig_rank = go.Figure(go.Bar(
    x=df_filtered["location_label"],
    y=df_filtered[sort_col],
    marker_color=[
        tier_colors.get(t, "#95a5a6")
        for t in df_filtered["performance_tier"]
    ],
    text=[f"${v:,.0f}" if "revenue" in sort_col or "value" in sort_col
          else f"{v:,}"
          for v in df_filtered[sort_col]],
    textposition="outside",
    textangle=-45,
    hovertemplate=(
        "<b>%{x}</b><br>"
        f"{sort_by}: %{{y:,.0f}}<br>"
        "<extra></extra>"
    ),
))

fig_rank.update_layout(
    height=400,
    margin=dict(t=10, b=80, l=40, r=20),
    xaxis_title="",
    yaxis_title=sort_by,
    xaxis_tickangle=-45,
    showlegend=False,
)

# Add tier legend as annotations
for tier, color in tier_colors.items():
    fig_rank.add_annotation(
        text=f"● {tier}",
        xref="paper", yref="paper",
        x=1, y=1 - list(tier_colors.keys()).index(tier) * 0.08,
        showarrow=False,
        font=dict(color=color, size=11),
        xanchor="right",
    )

st.plotly_chart(fig_rank, use_container_width=True)

st.divider()

# ── Row 2: Scatter — Revenue vs Avg Order Value ───────────────────────────────
col_scatter, col_table = st.columns([1.2, 0.8])

with col_scatter:
    st.subheader("Revenue vs Avg Order Value")
    st.caption(
        "Each dot is a location. "
        "Top right = high revenue AND high avg order value — ideal performers. "
        "Size = total orders."
    )

    # Cap size to avoid one location dominating
    size_cap = df["total_orders"].quantile(0.95)
    df_plot  = df.copy()
    df_plot["size_capped"] = df_plot["total_orders"].clip(upper=size_cap)

    fig_scatter = go.Figure()

    for tier, group in df_plot.groupby("performance_tier"):
        fig_scatter.add_trace(go.Scatter(
            x=group["avg_order_value"],
            y=group["total_revenue"],
            mode="markers+text",
            name=tier,
            marker=dict(
                color=tier_colors.get(tier, "#95a5a6"),
                size=group["size_capped"] / size_cap * 30 + 8,
                opacity=0.8,
                line=dict(width=1, color="white"),
            ),
            text=group["location_label"],
            textposition="top center",
            textfont=dict(size=9),
            hovertemplate=(
                "<b>%{text}</b><br>"
                "Avg Order Value: $%{x:,.2f}<br>"
                "Total Revenue: $%{y:,.0f}<br>"
                "<extra></extra>"
            ),
        ))

    fig_scatter.update_layout(
        height=400,
        margin=dict(t=10, b=10, l=40, r=10),
        xaxis_title="Avg Order Value ($)",
        yaxis_title="Total Revenue ($)",
        xaxis_tickprefix="$",
        yaxis_tickprefix="$",
        yaxis_tickformat=",.0f",
        legend_title="Tier",
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

with col_table:
    st.subheader("Restaurant Scorecard")
    st.caption("Full metrics per location sorted by revenue rank.")

    scorecard = df.sort_values("revenue_rank")[[
        "revenue_rank",
        "location_label",
        "total_revenue",
        "avg_order_value",
        "total_orders",
        "unique_customers",
        "avg_daily_revenue",
        "performance_tier",
    ]].copy()

    scorecard.columns = [
        "Rank", "Restaurant", "Total Revenue",
        "Avg Order Value", "Orders",
        "Customers", "Avg Daily Rev", "Tier",
    ]

    scorecard["Total Revenue"]   = scorecard["Total Revenue"].apply(lambda x: f"${x:,.0f}")
    scorecard["Avg Order Value"] = scorecard["Avg Order Value"].apply(lambda x: f"${x:,.2f}")
    scorecard["Avg Daily Rev"]   = scorecard["Avg Daily Rev"].apply(lambda x: f"${x:,.0f}")
    scorecard["Orders"]          = scorecard["Orders"].apply(lambda x: f"{x:,}")
    scorecard["Customers"]       = scorecard["Customers"].apply(lambda x: f"{x:,}")

    st.dataframe(
        scorecard,
        width='stretch',
        hide_index=True,
        height=400,
    )

st.divider()

# ── Row 3: Revenue Trend per Restaurant ───────────────────────────────────────
st.subheader("📈 Monthly Revenue Trend by Restaurant")
st.caption(
    "Compare revenue trends across locations over time. "
    "Select specific locations to compare directly."
)

# ── Smart default ──────────────────────────────────────────────────────────────
MAX_DEFAULT_LOCATIONS    = 10
MAX_SELECTABLE_LOCATIONS = 40
RESTAURANT_SELECTION_KEY = "restaurant_selection_trend_v2"

# Build selector options and labels first
location_options = df.sort_values("revenue_rank")[[
    "restaurant_id",
    "revenue_rank",
]].copy()

print(f"location_options:\n{location_options}\n")

location_options["rank_label"] = (
    location_options["revenue_rank"]
    .astype(int)
    .astype(str)
    .str.zfill(2)
)

# Generate the selector labels from last 6 digits from restaurant_id
# rather than using 6 first in order to avoid duplicates
location_options["selector_label"] = (
    "#"
    + location_options["rank_label"]
    + " Loc-"
    + location_options["restaurant_id"].astype(str).str[-6:]
)

selectable_options = location_options["restaurant_id"].tolist()

if total_locations > MAX_SELECTABLE_LOCATIONS:
    selectable_options = selectable_options[:MAX_SELECTABLE_LOCATIONS]
    location_options = location_options[
        location_options["restaurant_id"].isin(selectable_options)
    ]

    st.info(
        f"ℹ️ Selection limited to top {MAX_SELECTABLE_LOCATIONS} restaurants "
        f"by revenue to keep the chart readable. "
        f"{total_locations - MAX_SELECTABLE_LOCATIONS} lowest-ranked locations excluded."
    )

all_location_labels = location_options.set_index("restaurant_id")[
    "selector_label"
].to_dict()

print(f"After location_options:\n{location_options}")


# Initialize session state on first load only
default_selection = selectable_options[:MAX_DEFAULT_LOCATIONS]

if RESTAURANT_SELECTION_KEY not in st.session_state:
    st.session_state[RESTAURANT_SELECTION_KEY] = default_selection
else:
    valid_options = set(selectable_options)
    st.session_state[RESTAURANT_SELECTION_KEY] = [
        restaurant_id
        for restaurant_id in st.session_state[RESTAURANT_SELECTION_KEY]
        if restaurant_id in valid_options
    ]

selected_compare = st.multiselect(
    "Select restaurants to compare",
    options=selectable_options,
    key=RESTAURANT_SELECTION_KEY,
    format_func=lambda x: all_location_labels.get(x, str(x)),
)

st.caption(
    f"Showing {len(selected_compare)} of {len(selectable_options)} restaurants selected."
)

# ── Plot ───────────────────────────────────────────────────────────────────────
if selected_compare and not df_trends.empty:

    trend_filtered = df_trends[
        df_trends["restaurant_id"].isin(selected_compare)
    ].copy()

    trend_filtered["month_start"] = pd.to_datetime(
        trend_filtered["order_date"]
    ).dt.to_period("M").dt.to_timestamp()

    trend_monthly = trend_filtered.groupby(
        ["month_start", "restaurant_id"]
    )["total_revenue"].sum().reset_index()

    trend_monthly["location_label"] = trend_monthly["restaurant_id"].map(
        all_location_labels
    )

    fig_trend = px.line(
        trend_monthly,
        x="month_start",
        y="total_revenue",
        color="location_label",
        markers=True,
        labels={
            "month_start":    "Month",
            "total_revenue":  "Revenue ($)",
            "location_label": "Restaurant",
        },
    )
    fig_trend.update_layout(
        height=350,
        margin=dict(t=10, b=10, l=40, r=10),
        xaxis_title="",
        yaxis_title="Monthly Revenue ($)",
        yaxis_tickprefix="$",
        yaxis_tickformat=",.0f",
        hovermode="x unified",
    )
    st.plotly_chart(fig_trend, use_container_width=True)

else:
    st.info("Select at least one restaurant to see the trend.")

st.divider()

# ── Row 4: Top vs Bottom Performer Deep Dive ──────────────────────────────────
st.subheader("🔍 Top vs Bottom Performer Comparison")
st.caption("Head-to-head comparison of the highest and lowest revenue locations.")

top5    = df.sort_values("revenue_rank").head(5)
bottom5 = df.sort_values("revenue_rank", ascending=False).head(5)

col_top, col_bot = st.columns(2)

metrics_to_show = [
    ("total_revenue",    "Total Revenue",    True),
    ("avg_order_value",  "Avg Order Value",  True),
    ("total_orders",     "Total Orders",     False),
    ("unique_customers", "Unique Customers", False),
    ("avg_daily_revenue","Avg Daily Revenue",True),
]

with col_top:
    st.markdown(f"### 🟢 Top {n_compare} Restaurants")
    for _, row in top5.iterrows():
        with st.expander(f"#{row['revenue_rank']} — {row['location_label']}"):
            for col_name, label, is_currency in metrics_to_show:
                val = row[col_name]
                formatted = f"${val:,.0f}" if is_currency else f"{val:,}"
                st.write(f"**{label}:** {formatted}")
            st.write(f"**Tier:** {row['performance_tier']}")

with col_bot:
    st.markdown(f"### 🔴 Bottom {n_compare} Restaurants")
    for _, row in bottom5.iterrows():
        with st.expander(f"#{row['revenue_rank']} — {row['location_label']}"):
            for col_name, label, is_currency in metrics_to_show:
                val = row[col_name]
                formatted = f"${val:,.0f}" if is_currency else f"{val:,}"
                st.write(f"**{label}:** {formatted}")
            st.write(f"**Tier:** {row['performance_tier']}")
            
