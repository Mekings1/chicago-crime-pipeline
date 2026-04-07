import duckdb
import streamlit as st
import plotly.express as px

DB_PATH = "database/chicago_crime.duckdb"

st.set_page_config(
    page_title="Chicago Crime Dashboard",
    page_icon="🔵",
    layout="wide"
)

@st.cache_resource
def get_connection():
    return duckdb.connect(DB_PATH, read_only=True)

@st.cache_data
def load_crimes_by_type():
    con = get_connection()
    return con.execute("""
        SELECT crime_type, total_crimes, arrest_rate_pct
        FROM mart_crimes_by_type
        ORDER BY total_crimes DESC
        LIMIT 20
    """).df()

@st.cache_data
def load_crimes_over_time():
    con = get_connection()
    return con.execute("""
        SELECT month_start, SUM(total_crimes) AS total_crimes
        FROM mart_crimes_over_time
        GROUP BY month_start
        ORDER BY month_start
    """).df()

@st.cache_data
def load_kpis():
    con = get_connection()
    return con.execute("""
        SELECT
            SUM(total_crimes)   AS total_crimes,
            SUM(total_arrests)  AS total_arrests,
            ROUND(100.0 * SUM(total_arrests) / SUM(total_crimes), 1) AS arrest_rate
        FROM mart_crimes_by_type
    """).fetchone()

# ── Header ────────────────────────────────────────────────────────────────────
st.title("🔵 Chicago Crime Analytics")
st.caption("Data sourced from Chicago Data Portal · Powered by DuckDB + dbt")

# ── KPI row ───────────────────────────────────────────────────────────────────
total, arrests, rate = load_kpis()
k1, k2, k3 = st.columns(3)
k1.metric("Total Incidents",   f"{int(total):,}")
k2.metric("Total Arrests",     f"{int(arrests):,}")
k3.metric("Overall Arrest Rate", f"{rate}%")

st.divider()

# ── Tile 1: Crime type distribution (categorical) ─────────────────────────────
st.subheader("Crime Type Distribution")
df_type = load_crimes_by_type()

fig1 = px.bar(
    df_type,
    x="total_crimes",
    y="crime_type",
    orientation="h",
    color="arrest_rate_pct",
    color_continuous_scale="Blues",
    labels={
        "total_crimes":    "Total Incidents",
        "crime_type":      "Crime Type",
        "arrest_rate_pct": "Arrest Rate %"
    },
    title="Top 20 Crime Types by Incident Count (colour = arrest rate)"
)
fig1.update_layout(yaxis=dict(autorange="reversed"), height=520)
st.plotly_chart(fig1, use_container_width=True)

st.divider()

# ── Tile 2: Crimes over time (temporal) ──────────────────────────────────────
st.subheader("Monthly Crime Trend")
df_time = load_crimes_over_time()

fig2 = px.line(
    df_time,
    x="month_start",
    y="total_crimes",
    markers=True,
    labels={
        "month_start":  "Month",
        "total_crimes": "Total Incidents"
    },
    title="Total Crimes per Month"
)
fig2.update_traces(line_color="#1d6fba", marker_color="#1d6fba")
fig2.update_layout(height=400)
st.plotly_chart(fig2, use_container_width=True)

st.caption("Built with Streamlit · DuckDB · dbt · AWS S3")