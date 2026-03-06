"""
Kiabi Second-Hand Monitor – Streamlit Dashboard
Reads from nef_catalog.second_hand.gold_kiabi_listings via Databricks SQL.
"""

import os
import re
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from databricks import sql as dbsql

# ─── Page config ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Kiabi | Second-Hand Monitor",
    page_icon="🏷️",
    layout="wide",
)

# ─── Dark navy theme CSS ─────────────────────────────────────────────────────
st.markdown("""
<style>
[data-testid="stAppViewContainer"]  { background-color: #060C1C; }
[data-testid="stHeader"]            { background-color: #060C1C; }
section[data-testid="stSidebar"]    { background-color: #0D1526; }
.block-container                    { padding-top: 1.5rem; }

/* KPI metric cards */
div[data-testid="metric-container"] {
    background-color: #111B30;
    border-radius: 12px;
    padding: 18px 20px;
    border: 1px solid #1E2D4A;
}
div[data-testid="metric-container"] label { color: #8892A4 !important; font-size: 12px !important; }
div[data-testid="metric-container"] [data-testid="stMetricValue"] { color: white !important; font-size: 28px !important; }
div[data-testid="metric-container"] [data-testid="stMetricDelta"] { color: #22C55E !important; }

/* Tabs */
.stTabs [data-baseweb="tab-list"]  { background-color: #0D1526; border-radius: 10px; padding: 4px; gap: 4px; }
.stTabs [data-baseweb="tab"]       { color: #8892A4; border-radius: 8px; padding: 6px 20px; }
.stTabs [aria-selected="true"]     { background-color: #1E2D4A !important; color: white !important; }

/* Text */
h1, h2, h3, h4, h5 { color: white !important; }
p, label, li       { color: #8892A4; }

/* Dataframe */
[data-testid="stDataFrame"] { background-color: #111B30; border-radius: 10px; }

/* Divider */
hr { border-color: #1E2D4A; }

/* Inputs */
.stSelectbox [data-baseweb="select"] > div, .stTextInput > div > div > input {
    background-color: #111B30 !important;
    border-color: #1E2D4A !important;
    color: white !important;
}
</style>
""", unsafe_allow_html=True)

# ─── Constants ────────────────────────────────────────────────────────────────
WAREHOUSE_ID = os.getenv("WAREHOUSE_ID", "beb00aeaaa803c3e")
CATALOG = "nef_catalog"
SCHEMA = "second_hand"

SOURCE_COLORS = {"leboncoin": "#F97316", "vinted": "#22C55E"}
SOURCE_ICONS  = {"leboncoin": "🟠", "vinted": "🟢"}

# ─── Data helpers ─────────────────────────────────────────────────────────────

def _get_conn():
    host = os.getenv("DATABRICKS_HOST", "fevm-nef.cloud.databricks.com")
    host = host.replace("https://", "").replace("http://", "")
    token = os.getenv("DATABRICKS_TOKEN", "")
    return dbsql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        access_token=token,
    )


def _query(sql: str) -> pd.DataFrame:
    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    conn.close()
    return pd.DataFrame(rows, columns=cols)


def _parse_lat_lng(loc: str):
    """Extract lat/lng floats from the Location(...) repr string."""
    if not loc:
        return None, None
    lat = re.search(r"lat=([-\d.]+)", str(loc))
    lng = re.search(r"lng=([-\d.]+)", str(loc))
    if lat and lng:
        return float(lat.group(1)), float(lng.group(1))
    return None, None


@st.cache_data(ttl=300, show_spinner=False)
def load_listings() -> pd.DataFrame:
    df = _query(f"""
        SELECT
            source, title, price, location, published_at,
            is_never_worn, first_seen_at, last_seen_at, url, primary_image_url
        FROM {CATALOG}.{SCHEMA}.gold_kiabi_listings
        WHERE price IS NOT NULL AND price > 0 AND price < 500
    """)
    df[["lat", "lng"]] = df["location"].apply(lambda x: pd.Series(_parse_lat_lng(x)))
    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")
    df["week"] = df["published_at"].dt.to_period("W").dt.start_time
    return df


@st.cache_data(ttl=300, show_spinner=False)
def load_source_stats() -> pd.DataFrame:
    return _query(f"""
        SELECT
            source,
            COUNT(*)                                                AS total_listings,
            ROUND(AVG(CASE WHEN price < 500 THEN price END), 2)    AS avg_price,
            ROUND(MIN(CASE WHEN price < 500 THEN price END), 2)    AS min_price,
            ROUND(MAX(CASE WHEN price < 500 THEN price END), 2)    AS max_price,
            SUM(CASE WHEN is_never_worn THEN 1 ELSE 0 END)         AS never_worn
        FROM {CATALOG}.{SCHEMA}.gold_kiabi_listings
        GROUP BY source
        ORDER BY total_listings DESC
    """)


# ─── Map helper ───────────────────────────────────────────────────────────────

def france_map(df_map: pd.DataFrame, height: int = 320) -> go.Figure:
    traces = []
    for src, grp in df_map.dropna(subset=["lat", "lng"]).groupby("source"):
        traces.append(go.Scattergeo(
            lat=grp["lat"], lon=grp["lng"],
            mode="markers",
            name=src.capitalize(),
            marker=dict(size=6, color=SOURCE_COLORS.get(src, "#4169E1"), opacity=0.65),
            text=grp.get("title", src),
            hovertemplate="%{text}<extra></extra>",
        ))
    fig = go.Figure(traces)
    fig.update_layout(
        geo=dict(
            scope="europe",
            center=dict(lat=46.5, lon=2.3),
            projection_scale=8,
            showland=True, landcolor="#1A2340",
            showcoastlines=True, coastlinecolor="#2A3A60",
            showframe=False, bgcolor="#111B30",
            showcountries=True, countrycolor="#2A3A60",
        ),
        paper_bgcolor="#111B30",
        margin=dict(l=0, r=0, t=0, b=0),
        height=height,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, font=dict(color="white")),
    )
    return fig


# ─── Load data ────────────────────────────────────────────────────────────────
with st.spinner("Loading data from Databricks…"):
    df = load_listings()
    stats = load_source_stats()

# ─── Header ───────────────────────────────────────────────────────────────────
st.markdown(
    "<h1 style='margin-bottom:0;letter-spacing:2px;'>🏷️ KIABI &nbsp;<span style='color:#8892A4;font-size:18px;font-weight:400;'>Second-Hand Monitor</span></h1>",
    unsafe_allow_html=True,
)
st.markdown("<hr style='margin-top:8px;margin-bottom:16px;'>", unsafe_allow_html=True)

# ─── Navigation tabs ──────────────────────────────────────────────────────────
tab_overview, tab_vendors, tab_competitors = st.tabs(
    ["📊  Overview", "🏪  Vendors", "🔍  Competitor Analysis"]
)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 – OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════════
with tab_overview:
    total     = len(df)
    avg_price = df["price"].mean()
    nw_pct    = df["is_never_worn"].sum() / total * 100 if total else 0
    sources   = df["source"].nunique()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total Listings",  f"{total:,}")
    k2.metric("Avg Price",       f"€{avg_price:.2f}")
    k3.metric("Never Worn",      f"{nw_pct:.1f}%")
    k4.metric("Marketplaces",    str(sources))

    st.markdown("")

    # ── Trend chart + Map row ───────────────────────────────────────────────
    col_trend, col_map = st.columns([1, 1], gap="medium")

    with col_trend:
        st.markdown("#### Trends in Items Listed")
        trend = (
            df.dropna(subset=["week"])
            .groupby(["week", "source"])
            .size()
            .reset_index(name="count")
        )
        fig_trend = px.line(
            trend, x="week", y="count", color="source",
            color_discrete_map=SOURCE_COLORS,
            markers=True, template="plotly_dark",
        )
        fig_trend.update_layout(
            paper_bgcolor="#111B30", plot_bgcolor="#111B30",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, font=dict(color="white")),
            margin=dict(l=0, r=0, t=30, b=0), height=300,
            xaxis=dict(title="", gridcolor="#1E2D4A"),
            yaxis=dict(title="Listings", gridcolor="#1E2D4A"),
        )
        st.plotly_chart(fig_trend, use_container_width=True)

    with col_map:
        st.markdown("#### Listing Locations")
        st.plotly_chart(france_map(df), use_container_width=True)

    # ── Vendor performance row ──────────────────────────────────────────────
    st.markdown("#### Vendor Performance")
    col_bar, col_tbl = st.columns([1, 1], gap="medium")

    with col_bar:
        fig_bar = px.bar(
            stats, x="source", y="total_listings", color="source",
            color_discrete_map=SOURCE_COLORS,
            text="total_listings", template="plotly_dark",
        )
        fig_bar.update_traces(textposition="outside", textfont_color="white")
        fig_bar.update_layout(
            paper_bgcolor="#111B30", plot_bgcolor="#111B30",
            showlegend=False, height=260,
            margin=dict(l=0, r=0, t=10, b=0),
            xaxis=dict(title="", gridcolor="#1E2D4A"),
            yaxis=dict(title="Listings", gridcolor="#1E2D4A"),
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    with col_tbl:
        display_stats = stats.rename(columns={
            "source": "Marketplace", "total_listings": "Listings",
            "avg_price": "Avg Price (€)", "min_price": "Min (€)",
            "max_price": "Max (€)", "never_worn": "Never Worn",
        })
        st.dataframe(
            display_stats.style.format({"Avg Price (€)": "{:.2f}", "Min (€)": "{:.2f}", "Max (€)": "{:.2f}"}),
            use_container_width=True, hide_index=True,
        )

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 – VENDORS
# ═══════════════════════════════════════════════════════════════════════════════
with tab_vendors:
    st.markdown("## Vendors")

    # ── Filters ────────────────────────────────────────────────────────────
    f1, f2, f3 = st.columns([3, 2, 2])
    with f1:
        search = st.text_input("Search listings", placeholder="Search by title…")
    with f2:
        src_opts = ["All"] + sorted(df["source"].dropna().unique().tolist())
        source_filter = st.selectbox("Marketplace", src_opts)
    with f3:
        price_max = st.slider("Max price (€)", 0, 200, 150)

    fdf = df.copy()
    if search:
        fdf = fdf[fdf["title"].str.contains(search, case=False, na=False)]
    if source_filter != "All":
        fdf = fdf[fdf["source"] == source_filter]
    fdf = fdf[fdf["price"] <= price_max]

    st.markdown(f"<p style='color:#8892A4;'>Showing <b style='color:white'>{len(fdf):,}</b> listings</p>", unsafe_allow_html=True)

    col_left, col_right = st.columns([3, 2], gap="medium")

    with col_left:
        # ── Vendor summary cards ─────────────────────────────────────────
        for src, grp in fdf.groupby("source"):
            icon = SOURCE_ICONS.get(src, "⚪")
            avg  = grp["price"].mean()
            nw   = int(grp["is_never_worn"].sum())
            st.markdown(f"""
            <div style="background:#111B30;border-radius:10px;padding:16px 20px;
                        margin-bottom:12px;border:1px solid #1E2D4A;display:flex;align-items:center;gap:16px;">
                <span style="font-size:32px">{icon}</span>
                <div>
                    <div style="color:white;font-weight:700;font-size:17px">{src.capitalize()}</div>
                    <div style="color:#8892A4;font-size:13px;margin-top:4px">
                        <b style='color:white'>{len(grp):,}</b> listings &nbsp;·&nbsp;
                        Avg <b style='color:white'>€{avg:.2f}</b> &nbsp;·&nbsp;
                        <b style='color:white'>{nw}</b> never worn
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        # ── Recent listings table ─────────────────────────────────────────
        st.markdown("#### Recent Listings")
        cols_show = ["source", "title", "price", "is_never_worn", "published_at"]
        recent = (
            fdf[cols_show]
            .sort_values("published_at", ascending=False)
            .head(50)
            .rename(columns={
                "source": "Source", "title": "Title",
                "price": "Price (€)", "is_never_worn": "Never Worn",
                "published_at": "Published",
            })
        )
        st.dataframe(recent, use_container_width=True, hide_index=True)

    with col_right:
        # ── Price distribution ────────────────────────────────────────────
        st.markdown("#### Price Distribution")
        fig_hist = px.histogram(
            fdf, x="price", color="source", nbins=30,
            color_discrete_map=SOURCE_COLORS,
            template="plotly_dark", barmode="overlay", opacity=0.8,
        )
        fig_hist.update_layout(
            paper_bgcolor="#111B30", plot_bgcolor="#111B30",
            height=260, margin=dict(l=0, r=0, t=10, b=0),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, font=dict(color="white")),
            xaxis=dict(title="Price (€)", gridcolor="#1E2D4A"),
            yaxis=dict(title="Count", gridcolor="#1E2D4A"),
        )
        st.plotly_chart(fig_hist, use_container_width=True)

        # ── Filtered map ──────────────────────────────────────────────────
        st.markdown("#### Location Map")
        st.plotly_chart(france_map(fdf, height=290), use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 – COMPETITOR ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════
with tab_competitors:
    st.markdown("## Competitor Analysis")

    # Big KPIs
    total_all = int(stats["total_listings"].sum())
    lbc_row = stats[stats["source"] == "leboncoin"]
    vnt_row = stats[stats["source"] == "vinted"]

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Market Listings", f"{total_all:,}")
    c2.metric("LeBonCoin", f"{int(lbc_row['total_listings'].values[0]):,}" if not lbc_row.empty else "—",
              help="Listings scraped from LeBonCoin")
    c3.metric("Vinted", f"{int(vnt_row['total_listings'].values[0]):,}" if not vnt_row.empty else "—",
              help="Listings scraped from Vinted")

    st.markdown("")

    col_charts, col_table = st.columns([1, 1], gap="medium")

    with col_charts:
        st.markdown("#### Listings by Marketplace")
        fig_comp = px.bar(
            stats, x="source", y="total_listings", color="source",
            color_discrete_map=SOURCE_COLORS,
            text="total_listings", template="plotly_dark",
        )
        fig_comp.update_traces(textposition="outside", textfont_color="white")
        fig_comp.update_layout(
            paper_bgcolor="#111B30", plot_bgcolor="#111B30",
            showlegend=False, height=280,
            margin=dict(l=0, r=0, t=10, b=0),
            xaxis=dict(title="", gridcolor="#1E2D4A"),
            yaxis=dict(title="Listings", gridcolor="#1E2D4A"),
        )
        st.plotly_chart(fig_comp, use_container_width=True)

        # Never-worn comparison
        st.markdown("#### Never-Worn Ratio")
        nw_df = stats.copy()
        nw_df["worn"] = nw_df["total_listings"] - nw_df["never_worn"]
        nw_df["never_worn_pct"] = (nw_df["never_worn"] / nw_df["total_listings"] * 100).round(1)
        fig_nw = px.bar(
            nw_df, x="source", y="never_worn_pct", color="source",
            color_discrete_map=SOURCE_COLORS,
            text="never_worn_pct", template="plotly_dark",
        )
        fig_nw.update_traces(texttemplate="%{text:.1f}%", textposition="outside", textfont_color="white")
        fig_nw.update_layout(
            paper_bgcolor="#111B30", plot_bgcolor="#111B30",
            showlegend=False, height=250,
            margin=dict(l=0, r=0, t=10, b=0),
            xaxis=dict(title="", gridcolor="#1E2D4A"),
            yaxis=dict(title="% Never Worn", gridcolor="#1E2D4A"),
        )
        st.plotly_chart(fig_nw, use_container_width=True)

    with col_table:
        st.markdown("#### Vendor Performance Trace")
        perf_table = stats.rename(columns={
            "source": "Marketplace",
            "total_listings": "Listings",
            "avg_price": "Avg Price (€)",
            "min_price": "Min (€)",
            "max_price": "Max (€)",
            "never_worn": "Never Worn",
        })
        st.dataframe(
            perf_table.style.format({
                "Avg Price (€)": "{:.2f}",
                "Min (€)": "{:.2f}",
                "Max (€)": "{:.2f}",
            }),
            use_container_width=True, hide_index=True,
        )

        st.markdown("#### Price Distribution Comparison")
        fig_box = px.box(
            df[df["price"] < 200], x="source", y="price", color="source",
            color_discrete_map=SOURCE_COLORS, template="plotly_dark",
            points="outliers",
        )
        fig_box.update_layout(
            paper_bgcolor="#111B30", plot_bgcolor="#111B30",
            showlegend=False, height=310,
            margin=dict(l=0, r=0, t=10, b=0),
            xaxis=dict(title="", gridcolor="#1E2D4A"),
            yaxis=dict(title="Price (€)", gridcolor="#1E2D4A"),
        )
        st.plotly_chart(fig_box, use_container_width=True)
