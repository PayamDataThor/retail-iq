"""
RetailIQ Insights — Streamlit dashboard backed by Lakebase (managed Postgres).

Auth: Databricks Apps injects DATABRICKS_HOST + token into the environment,
      so WorkspaceClient() works with no extra config.

Connection strategy:
  - SDK client and endpoint metadata cached permanently (_init_sdk).
  - Lakebase token generated fresh per query (expires in 1 h).
  - Query results cached 5 min (st.cache_data ttl=300).
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2

st.set_page_config(
    page_title="RetailIQ Insights",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Connection ────────────────────────────────────────────────────────────────

@st.cache_resource
def _init_sdk():
    """Resolve endpoint host/name once; cache for the lifetime of the process."""
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    project = os.environ.get("LAKEBASE_PROJECT", "retail-iq-db")
    parent = f"projects/{project}"
    branches  = list(w.postgres.list_branches(parent=parent))
    prod      = next(b for b in branches if b.name.endswith("/production"))
    endpoints = list(w.postgres.list_endpoints(parent=prod.name))
    primary   = next(e for e in endpoints if "primary" in e.name)
    return w, primary.name, primary.status.hosts.host, w.current_user.me().user_name


def _get_conn():
    w, ep_name, host, user = _init_sdk()
    token = w.postgres.generate_database_credential(endpoint=ep_name).token
    return psycopg2.connect(
        host=host, port=5432, dbname="databricks_postgres",
        user=user, password=token, sslmode="require",
        connect_timeout=15,
    )


@st.cache_data(ttl=300, show_spinner=False)
def query(sql: str) -> pd.DataFrame:
    conn = _get_conn()
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()


# ── Sidebar ───────────────────────────────────────────────────────────────────

st.sidebar.title("RetailIQ")
st.sidebar.caption("Business Insights Dashboard")
st.sidebar.divider()

try:
    categories = query("SELECT DISTINCT category FROM gold_product_performance ORDER BY 1")["category"].tolist()
    regions    = query("SELECT DISTINCT region    FROM gold_store_kpis          ORDER BY 1")["region"].tolist()
except Exception as e:
    st.error(f"Cannot connect to Lakebase: {e}")
    st.stop()

sel_cats    = st.sidebar.multiselect("Categories", categories, default=categories)
sel_regions = st.sidebar.multiselect("Regions",    regions,    default=regions)

# guard against empty selection — treat as "all"
if not sel_cats:    sel_cats    = categories
if not sel_regions: sel_regions = regions

# Build safe IN-list literals (values come from our own DB, not from user text)
cats_in    = ", ".join(f"'{c}'" for c in sel_cats)
regions_in = ", ".join(f"'{r}'" for r in sel_regions)

st.sidebar.divider()
if st.sidebar.button("Refresh data"):
    st.cache_data.clear()
    st.rerun()
st.sidebar.caption("Auto-refreshes every 5 min")


# ── Header KPIs ───────────────────────────────────────────────────────────────

st.title("RetailIQ Insights")

kpi = query(f"""
    SELECT
        ROUND(SUM(gross_revenue), 0)       AS total_revenue,
        SUM(num_orders)                    AS total_orders,
        ROUND(AVG(avg_order_value), 2)     AS avg_order_value,
        SUM(units_sold)                    AS total_units
    FROM gold_revenue_by_category_month
    WHERE category IN ({cats_in})
""").iloc[0]

cust_kpi = query("""
    SELECT COUNT(*) AS customers, ROUND(AVG(monetary), 2) AS avg_ltv
    FROM gold_customer_rfm
""").iloc[0]

prod_kpi = query(f"""
    SELECT
        COUNT(CASE WHEN gross_profit <= 0 THEN 1 END) AS negative_margin,
        ROUND(AVG(avg_discount_pct), 1)               AS avg_discount
    FROM gold_product_performance
    WHERE category IN ({cats_in})
""").iloc[0]

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Total Revenue",    f"${kpi.total_revenue:,.0f}")
c2.metric("Total Orders",     f"{kpi.total_orders:,}")
c3.metric("Avg Order Value",  f"${kpi.avg_order_value:,.2f}")
c4.metric("Customers",        f"{cust_kpi.customers:,}")
c5.metric("Avg Customer LTV", f"${cust_kpi.avg_ltv:,.2f}")
c6.metric("Avg Discount",     f"{prod_kpi.avg_discount:.1f}%",
          delta=f"{prod_kpi.negative_margin:.0f} neg-margin SKUs",
          delta_color="inverse")

st.divider()


# ── Tabs ──────────────────────────────────────────────────────────────────────

t_rev, t_stores, t_products, t_customers = st.tabs(
    ["Revenue", "Stores", "Products", "Customers"]
)


# ── Revenue tab ───────────────────────────────────────────────────────────────

with t_rev:
    st.subheader("Revenue by Category & Month")

    rev = query(f"""
        SELECT year, month, category,
               SUM(gross_revenue) AS gross_revenue,
               SUM(num_orders)    AS num_orders
        FROM gold_revenue_by_category_month
        WHERE category IN ({cats_in})
        GROUP BY year, month, category
        ORDER BY year, month
    """)
    rev["period"] = (rev["year"].astype(str) + "-"
                     + rev["month"].astype(str).str.zfill(2))

    col1, col2 = st.columns([3, 1])

    with col1:
        fig = px.area(
            rev, x="period", y="gross_revenue", color="category",
            title="Gross Revenue — stacked by category",
            labels={"gross_revenue": "Revenue ($)", "period": "Month"},
            color_discrete_sequence=px.colors.qualitative.Bold,
        )
        fig.update_layout(legend=dict(orientation="h", y=-0.25), height=380)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        cat_total = rev.groupby("category")["gross_revenue"].sum().sort_values(ascending=False)
        fig_pie = px.pie(
            values=cat_total.values, names=cat_total.index,
            title="Revenue share",
            hole=0.45,
            color_discrete_sequence=px.colors.qualitative.Bold,
        )
        fig_pie.update_layout(height=380, legend=dict(orientation="h", y=-0.25))
        st.plotly_chart(fig_pie, use_container_width=True)

    # Month-over-month growth
    monthly = (rev.groupby("period")["gross_revenue"].sum()
               .reset_index().sort_values("period"))
    monthly["mom_pct"] = monthly["gross_revenue"].pct_change() * 100

    fig_mom = go.Figure()
    fig_mom.add_bar(x=monthly["period"], y=monthly["gross_revenue"],
                    name="Revenue", marker_color="#2563eb", opacity=0.7)
    fig_mom.add_scatter(x=monthly["period"], y=monthly["mom_pct"],
                        name="MoM %", mode="lines+markers",
                        yaxis="y2", line=dict(color="#f59e0b", width=2))
    fig_mom.update_layout(
        title="Monthly Revenue & MoM Growth",
        yaxis=dict(title="Revenue ($)"),
        yaxis2=dict(title="MoM Growth %", overlaying="y", side="right",
                    ticksuffix="%", zeroline=False),
        legend=dict(orientation="h"),
        height=320,
    )
    st.plotly_chart(fig_mom, use_container_width=True)


# ── Stores tab ────────────────────────────────────────────────────────────────

with t_stores:
    st.subheader("Store Performance")

    stores = query(f"""
        SELECT store_name, city, state, region,
               total_revenue, total_orders, unique_customers, avg_basket_size
        FROM gold_store_kpis
        WHERE region IN ({regions_in})
        ORDER BY total_revenue DESC
    """)

    col1, col2 = st.columns(2)

    with col1:
        fig = px.bar(
            stores.head(15), x="total_revenue", y="store_name",
            orientation="h", color="region",
            title="Top 15 Stores by Revenue",
            labels={"total_revenue": "Revenue ($)", "store_name": ""},
            color_discrete_sequence=px.colors.qualitative.Safe,
        )
        fig.update_layout(height=500, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig2 = px.scatter(
            stores, x="total_orders", y="avg_basket_size",
            size="total_revenue", color="region",
            hover_name="store_name",
            hover_data={"city": True, "state": True,
                        "total_revenue": ":,.0f", "region": False},
            title="Basket Size vs Order Volume  (bubble = revenue)",
            labels={"total_orders": "Total Orders",
                    "avg_basket_size": "Avg Basket ($)"},
            color_discrete_sequence=px.colors.qualitative.Safe,
        )
        fig2.update_layout(height=500)
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Region Summary")
    region_agg = (stores
        .groupby("region")
        .agg(stores=("store_name", "count"),
             revenue=("total_revenue", "sum"),
             avg_basket=("avg_basket_size", "mean"),
             orders=("total_orders", "sum"),
             customers=("unique_customers", "sum"))
        .reset_index()
        .sort_values("revenue", ascending=False)
    )
    region_agg["revenue"]    = region_agg["revenue"].map("${:,.0f}".format)
    region_agg["avg_basket"] = region_agg["avg_basket"].map("${:,.2f}".format)
    region_agg["orders"]     = region_agg["orders"].map("{:,}".format)
    region_agg["customers"]  = region_agg["customers"].map("{:,}".format)
    st.dataframe(region_agg, use_container_width=True, hide_index=True)


# ── Products tab ──────────────────────────────────────────────────────────────

with t_products:
    st.subheader("Product Performance")

    prods = query(f"""
        SELECT product_name, category, brand,
               list_price, cost, units_sold,
               gross_revenue, gross_profit, avg_discount_pct,
               ROUND(gross_profit / NULLIF(gross_revenue, 0) * 100, 1) AS margin_pct
        FROM gold_product_performance
        WHERE category IN ({cats_in})
        ORDER BY gross_revenue DESC
    """)

    col1, col2 = st.columns(2)

    with col1:
        fig = px.scatter(
            prods, x="units_sold", y="margin_pct",
            size="gross_revenue", color="category",
            hover_name="product_name",
            hover_data={"gross_revenue": ":,.0f", "avg_discount_pct": ":.1f",
                        "category": False},
            title="Margin % vs Units Sold  (bubble = revenue)",
            labels={"units_sold": "Units Sold", "margin_pct": "Gross Margin %"},
            color_discrete_sequence=px.colors.qualitative.Bold,
        )
        fig.add_hline(y=0, line_dash="dash", line_color="red",
                      annotation_text="break-even", annotation_position="bottom right")
        fig.update_layout(height=420)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        cat_g = (prods.groupby("category")
                 .agg(revenue=("gross_revenue", "sum"),
                      profit=("gross_profit", "sum"))
                 .reset_index()
                 .sort_values("revenue", ascending=False))
        fig2 = go.Figure()
        fig2.add_bar(x=cat_g["category"], y=cat_g["revenue"],
                     name="Revenue", marker_color="#2563eb", opacity=0.8)
        fig2.add_bar(x=cat_g["category"], y=cat_g["profit"],
                     name="Gross Profit", marker_color="#10b981")
        fig2.update_layout(title="Revenue & Profit by Category",
                           barmode="overlay", height=420,
                           yaxis_title="Amount ($)")
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Product Details")
    col_sort, col_n = st.columns([2, 1])
    sort_col = col_sort.selectbox(
        "Sort by",
        ["gross_revenue", "gross_profit", "units_sold", "margin_pct", "avg_discount_pct"],
    )
    top_n = col_n.slider("Rows", 10, len(prods), 25)

    disp = prods.nlargest(top_n, sort_col)[
        ["product_name", "category", "brand", "list_price", "cost",
         "units_sold", "gross_revenue", "gross_profit", "margin_pct", "avg_discount_pct"]
    ].copy()
    disp["list_price"]       = disp["list_price"].map("${:,.2f}".format)
    disp["cost"]             = disp["cost"].map("${:,.2f}".format)
    disp["gross_revenue"]    = disp["gross_revenue"].map("${:,.0f}".format)
    disp["gross_profit"]     = disp["gross_profit"].map("${:,.0f}".format)
    disp["margin_pct"]       = disp["margin_pct"].map("{:.1f}%".format)
    disp["avg_discount_pct"] = disp["avg_discount_pct"].map("{:.1f}%".format)
    st.dataframe(disp, use_container_width=True, hide_index=True)


# ── Customers tab ─────────────────────────────────────────────────────────────

with t_customers:
    st.subheader("Customer RFM Segmentation")

    seg_summary = query("""
        SELECT rfm_segment,
               COUNT(*)                     AS customers,
               ROUND(AVG(monetary), 2)      AS avg_spend,
               ROUND(AVG(frequency), 1)     AS avg_orders,
               ROUND(AVG(recency_days))     AS avg_recency_days,
               ROUND(SUM(monetary), 0)      AS total_spend
        FROM gold_customer_rfm
        GROUP BY rfm_segment
        ORDER BY customers DESC
    """)

    col1, col2 = st.columns(2)

    with col1:
        fig = px.pie(
            seg_summary, values="customers", names="rfm_segment",
            title="Customers per segment",
            hole=0.42,
            color_discrete_sequence=px.colors.qualitative.Bold,
        )
        fig.update_layout(height=380, legend=dict(orientation="h", y=-0.25))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig2 = px.bar(
            seg_summary.sort_values("avg_spend", ascending=False),
            x="rfm_segment", y="avg_spend",
            color="rfm_segment", text="avg_spend",
            title="Avg Lifetime Spend per Segment",
            labels={"avg_spend": "Avg Spend ($)", "rfm_segment": ""},
            color_discrete_sequence=px.colors.qualitative.Bold,
        )
        fig2.update_traces(texttemplate="$%{text:,.0f}", textposition="outside")
        fig2.update_layout(height=380, showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)

    # Segment comparison radar
    radar_cols = ["avg_spend", "avg_orders", "avg_recency_days"]
    normed = seg_summary[["rfm_segment"] + radar_cols].copy()
    for c in radar_cols:
        mx = normed[c].max()
        if c == "avg_recency_days":   # lower recency = better, so invert
            normed[c] = 1 - normed[c] / mx
        else:
            normed[c] = normed[c] / mx

    fig_radar = go.Figure()
    for _, row in normed.iterrows():
        vals = [row[c] for c in radar_cols] + [row[radar_cols[0]]]
        fig_radar.add_trace(go.Scatterpolar(
            r=vals, theta=radar_cols + [radar_cols[0]],
            fill="toself", name=row["rfm_segment"],
        ))
    fig_radar.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
        title="Segment Profiles (normalised)",
        height=420,
    )
    st.plotly_chart(fig_radar, use_container_width=True)

    st.subheader("Segment Table")
    disp_seg = seg_summary.copy()
    disp_seg["customers"]        = disp_seg["customers"].map("{:,}".format)
    disp_seg["avg_spend"]        = disp_seg["avg_spend"].map("${:,.2f}".format)
    disp_seg["avg_recency_days"] = disp_seg["avg_recency_days"].map("{:.0f} days".format)
    disp_seg["total_spend"]      = disp_seg["total_spend"].map("${:,.0f}".format)
    st.dataframe(disp_seg, use_container_width=True, hide_index=True)

    st.subheader("Customer Lookup")
    seg_choice = st.selectbox(
        "Filter by segment",
        ["All"] + seg_summary["rfm_segment"].tolist(),
    )
    where_seg = (f"WHERE rfm_segment = '{seg_choice}'"
                 if seg_choice != "All" else "")
    customers = query(f"""
        SELECT full_name, email, rfm_segment, rfm_score,
               recency_days, frequency, ROUND(monetary, 2) AS monetary,
               r_score, f_score, m_score
        FROM gold_customer_rfm
        {where_seg}
        ORDER BY rfm_score DESC, monetary DESC
        LIMIT 200
    """)
    st.dataframe(customers, use_container_width=True, hide_index=True)
