"""Streamlit dashboard for earthquake monitoring."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pydeck as pdk
import streamlit as st

from quake_stream.db import get_connection, init_db

# --- Page config ---
st.set_page_config(
    page_title="Quake Stream Dashboard",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Dark theme CSS ---
st.markdown("""
<style>
    .stApp { background-color: #0e1117; }
    .metric-card {
        background: #1a1d24;
        border-radius: 10px;
        padding: 15px;
        border-left: 4px solid;
        margin-bottom: 10px;
    }
    .stMetric { background: #1a1d24; border-radius: 8px; padding: 10px; }
</style>
""", unsafe_allow_html=True)


# --- Data loading ---
@st.cache_data(ttl=60)
def load_earthquakes(hours: int) -> pd.DataFrame:
    """Load earthquakes from PostgreSQL."""
    try:
        conn = get_connection()
        query = """
            SELECT id, magnitude, place, time, longitude, latitude, depth, url, ingested_at
            FROM earthquakes
            WHERE time >= NOW() - INTERVAL '%s hours'
            ORDER BY time DESC
        """
        df = pd.read_sql(query, conn, params=[hours])
        conn.close()
        if not df.empty:
            df["time"] = pd.to_datetime(df["time"], utc=True)
            df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True)
        return df
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_pipeline_metrics() -> dict:
    """Load pipeline metrics from the database."""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT
                COUNT(*) as total_messages,
                MAX(ingested_at) as last_ingestion,
                MIN(ingested_at) as first_ingestion,
                COUNT(*) FILTER (WHERE ingested_at >= NOW() - INTERVAL '1 minute') as msgs_last_minute,
                COUNT(*) FILTER (WHERE ingested_at >= NOW() - INTERVAL '5 minutes') as msgs_last_5min
            FROM earthquakes
        """)
        row = cur.fetchone()
        conn.close()
        return {
            "total_messages": row[0],
            "last_ingestion": row[1],
            "first_ingestion": row[2],
            "msgs_last_minute": row[3],
            "msgs_last_5min": row[4],
        }
    except Exception:
        return {
            "total_messages": 0,
            "last_ingestion": None,
            "first_ingestion": None,
            "msgs_last_minute": 0,
            "msgs_last_5min": 0,
        }


def check_kafka_connection() -> dict:
    """Check Kafka broker connectivity."""
    try:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers": os.getenv("KAFKA_BROKER", "localhost:9092")})
        topics = admin.list_topics(timeout=5)
        earthquake_topic = topics.topics.get("earthquakes")
        partitions = len(earthquake_topic.partitions) if earthquake_topic else 0
        return {
            "connected": True,
            "topic_exists": earthquake_topic is not None,
            "partitions": partitions,
            "broker_count": len(topics.brokers),
        }
    except Exception as e:
        return {"connected": False, "error": str(e), "topic_exists": False, "partitions": 0, "broker_count": 0}


# --- Sidebar ---
with st.sidebar:
    st.title("🌍 Quake Stream")
    st.markdown("---")

    time_range = st.selectbox(
        "Time Range",
        options=[("Last Hour", 1), ("Last 24 Hours", 24), ("Last 7 Days", 168)],
        format_func=lambda x: x[0],
        index=1,
    )
    hours = time_range[1]

    min_mag = st.slider("Minimum Magnitude", 0.0, 8.0, 0.0, 0.5)

    st.markdown("---")
    auto_refresh = st.checkbox("Auto-refresh (60s)", value=True)
    if auto_refresh:
        st.markdown("*Dashboard refreshes every 60 seconds*")

    st.markdown("---")
    st.markdown("**Links**")
    st.markdown("- [Kafka UI](http://localhost:8080)")
    st.markdown("- [USGS Latest](https://earthquake.usgs.gov/earthquakes/map/)")

# --- Load data ---
df = load_earthquakes(hours)
if min_mag > 0 and not df.empty:
    df = df[df["magnitude"] >= min_mag]

# --- Auto refresh ---
if auto_refresh:
    st.empty()
    import time as _time
    _placeholder = st.empty()

# --- Header ---
st.title("🌍 Earthquake Monitor")
st.caption(f"Showing data for the last {time_range[0].lower()} | Updated: {datetime.now(timezone.utc):%H:%M:%S UTC}")

# --- Row 1: Stats cards ---
if not df.empty:
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("Total Events", f"{len(df):,}")
    col2.metric("Max Magnitude", f"{df['magnitude'].max():.1f}")
    col3.metric("Avg Magnitude", f"{df['magnitude'].mean():.1f}")
    col4.metric("M5.0+", f"{(df['magnitude'] >= 5.0).sum()}")
    col5.metric("M3.0-4.9", f"{((df['magnitude'] >= 3.0) & (df['magnitude'] < 5.0)).sum()}")
    col6.metric("Avg Depth", f"{df['depth'].mean():.0f} km")
else:
    st.warning("No earthquake data found. Make sure the producer and DB consumer are running.")

st.markdown("---")

# --- Row 2: Globe + Pipeline health ---
col_globe, col_pipeline = st.columns([3, 1])

with col_globe:
    st.subheader("3D Globe View")
    if not df.empty:
        globe_df = df.copy()
        globe_df["color_r"] = globe_df["magnitude"].apply(lambda m: 255 if m >= 5.0 else 255 if m >= 3.0 else 76)
        globe_df["color_g"] = globe_df["magnitude"].apply(lambda m: 50 if m >= 5.0 else 193 if m >= 3.0 else 175)
        globe_df["color_b"] = globe_df["magnitude"].apply(lambda m: 50 if m >= 5.0 else 7 if m >= 3.0 else 80)
        globe_df["radius"] = globe_df["magnitude"].apply(lambda m: max(m, 0.5) ** 2.5 * 8000)
        globe_df["elevation"] = globe_df["depth"] * 100

        layer = pdk.Layer(
            "ScatterplotLayer",
            data=globe_df,
            get_position=["longitude", "latitude"],
            get_radius="radius",
            get_fill_color=["color_r", "color_g", "color_b", 180],
            pickable=True,
            auto_highlight=True,
        )

        view_state = pdk.ViewState(
            latitude=20,
            longitude=0,
            zoom=1.2,
            pitch=45,
        )

        deck = pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip={"text": "M{magnitude} — {place}\nDepth: {depth} km"},
            map_style="mapbox://styles/mapbox/dark-v11",
        )
        st.pydeck_chart(deck, height=500)
    else:
        st.info("No data to display on globe.")

with col_pipeline:
    st.subheader("Pipeline Health")

    # Kafka connection
    kafka_info = check_kafka_connection()
    if kafka_info["connected"]:
        st.success("Kafka: Connected")
        st.metric("Brokers", kafka_info["broker_count"])
        st.metric("Partitions", kafka_info["partitions"])
        if kafka_info["topic_exists"]:
            st.success("Topic 'earthquakes': Active")
        else:
            st.warning("Topic 'earthquakes': Not found")
    else:
        st.error("Kafka: Disconnected")

    st.markdown("---")

    # DB metrics
    pipeline = load_pipeline_metrics()
    st.metric("Total in DB", f"{pipeline['total_messages']:,}")
    st.metric("Last minute", f"{pipeline['msgs_last_minute']}")
    st.metric("Last 5 min", f"{pipeline['msgs_last_5min']}")

    if pipeline["last_ingestion"]:
        last = pipeline["last_ingestion"]
        if hasattr(last, "strftime"):
            st.caption(f"Last ingestion: {last:%H:%M:%S UTC}")

    # Postgres connection
    try:
        conn = get_connection()
        conn.close()
        st.success("PostgreSQL: Connected")
    except Exception:
        st.error("PostgreSQL: Disconnected")

st.markdown("---")

# --- Row 3: Charts ---
if not df.empty:
    st.subheader("Analysis")
    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        # Magnitude histogram
        fig_hist = px.histogram(
            df, x="magnitude", nbins=20,
            title="Magnitude Distribution",
            color_discrete_sequence=["#ff6b6b"],
            template="plotly_dark",
        )
        fig_hist.update_layout(
            xaxis_title="Magnitude", yaxis_title="Count",
            height=350, margin=dict(t=40, b=30),
        )
        st.plotly_chart(fig_hist, use_container_width=True)

        # Depth vs Magnitude scatter
        fig_scatter = px.scatter(
            df, x="magnitude", y="depth",
            color="magnitude",
            color_continuous_scale="YlOrRd",
            title="Depth vs Magnitude",
            template="plotly_dark",
            hover_data=["place"],
        )
        fig_scatter.update_layout(
            xaxis_title="Magnitude", yaxis_title="Depth (km)",
            yaxis=dict(autorange="reversed"),
            height=350, margin=dict(t=40, b=30),
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

    with chart_col2:
        # Events per hour time series
        ts_df = df.set_index("time").resample("1h").size().reset_index(name="count")
        fig_ts = px.bar(
            ts_df, x="time", y="count",
            title="Events per Hour",
            color_discrete_sequence=["#4ecdc4"],
            template="plotly_dark",
        )
        fig_ts.update_layout(
            xaxis_title="Time (UTC)", yaxis_title="Event Count",
            height=350, margin=dict(t=40, b=30),
        )
        st.plotly_chart(fig_ts, use_container_width=True)

        # Top regions
        region_df = df["place"].apply(lambda p: p.split(", ")[-1] if ", " in p else p)
        top_regions = region_df.value_counts().head(10).reset_index()
        top_regions.columns = ["Region", "Count"]
        fig_regions = px.bar(
            top_regions, x="Count", y="Region",
            orientation="h",
            title="Top 10 Regions",
            color_discrete_sequence=["#45b7d1"],
            template="plotly_dark",
        )
        fig_regions.update_layout(
            height=350, margin=dict(t=40, b=30),
            yaxis=dict(autorange="reversed"),
        )
        st.plotly_chart(fig_regions, use_container_width=True)

    st.markdown("---")

# --- Row 4: Data table + export ---
st.subheader("Earthquake Data")

if not df.empty:
    # Export buttons
    exp_col1, exp_col2, exp_col3 = st.columns([1, 1, 6])
    with exp_col1:
        csv = df.to_csv(index=False)
        st.download_button("Download CSV", csv, "earthquakes.csv", "text/csv")
    with exp_col2:
        json_str = df.to_json(orient="records", date_format="iso")
        st.download_button("Download JSON", json_str, "earthquakes.json", "application/json")

    # Display table
    display_df = df[["magnitude", "place", "depth", "latitude", "longitude", "time"]].copy()
    display_df["time"] = display_df["time"].dt.strftime("%Y-%m-%d %H:%M UTC")
    display_df.columns = ["Magnitude", "Place", "Depth (km)", "Lat", "Lon", "Time"]

    st.dataframe(
        display_df,
        use_container_width=True,
        height=400,
        column_config={
            "Magnitude": st.column_config.NumberColumn(format="%.1f"),
            "Depth (km)": st.column_config.NumberColumn(format="%.1f"),
            "Lat": st.column_config.NumberColumn(format="%.2f"),
            "Lon": st.column_config.NumberColumn(format="%.2f"),
        },
    )
else:
    st.info("No data to display.")

# --- Auto refresh trigger ---
if auto_refresh:
    import time as _time
    _time.sleep(60)
    st.rerun()
