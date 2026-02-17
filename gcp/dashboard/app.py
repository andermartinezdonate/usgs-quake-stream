"""Streamlit dashboard for earthquake monitoring — GCP Cloud Run edition.

Reads from BigQuery unified_events and raw_events tables.
Features: source filtering, per-source KPIs, coverage map, source comparison,
pipeline health/observability.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from google.cloud import bigquery

from quake_stream.map_layers import (
    MAPBOX_STYLES,
    build_globe_map,
    build_mapbox_map,
)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", os.environ.get("GOOGLE_CLOUD_PROJECT", ""))
DATASET = os.environ.get("BQ_DATASET", "quake_stream")

ALL_SOURCES = ["usgs", "emsc", "gfz", "isc", "ipgp", "geonet"]

SOURCE_COLORS = {
    "usgs": "#ff6b6b",
    "emsc": "#4ecdc4",
    "gfz": "#feca57",
    "isc": "#a78bfa",
    "ipgp": "#45b7d1",
    "geonet": "#f093fb",
}

# ── Page config ───────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SeisMonitor",
    page_icon="https://em-content.zobj.net/source/apple/391/globe-showing-americas_1f30e.png",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS ───────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    .stApp {
        background: linear-gradient(135deg, #0a0a0f 0%, #0d1117 50%, #0a0f1a 100%);
        font-family: 'Inter', sans-serif;
    }

    .dash-header {
        background: linear-gradient(90deg, rgba(255,107,107,0.08) 0%, rgba(78,205,196,0.08) 100%);
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 16px;
        padding: 20px 28px;
        margin-bottom: 20px;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }
    .dash-header h1 {
        font-size: 1.8rem;
        font-weight: 700;
        background: linear-gradient(135deg, #ff6b6b, #feca57, #4ecdc4);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0;
    }
    .dash-header .subtitle {
        color: #8b949e;
        font-size: 0.82rem;
        margin: 2px 0 0 0;
    }
    .freshness {
        display: flex;
        align-items: center;
        gap: 8px;
        font-size: 0.78rem;
        color: #8b949e;
    }
    .freshness-dot {
        width: 8px; height: 8px;
        border-radius: 50%;
        display: inline-block;
        animation: pulse 2s infinite;
    }
    .freshness-dot.fresh { background: #4ecdc4; box-shadow: 0 0 6px #4ecdc4; }
    .freshness-dot.stale { background: #feca57; box-shadow: 0 0 6px #feca57; }
    .freshness-dot.offline { background: #ff6b6b; box-shadow: 0 0 6px #ff6b6b; }

    .alert-banner {
        background: linear-gradient(90deg, rgba(215,48,39,0.15), rgba(215,48,39,0.05));
        border: 1px solid rgba(215,48,39,0.3);
        border-left: 4px solid #d73027;
        border-radius: 8px;
        padding: 12px 20px;
        margin-bottom: 16px;
        font-size: 0.88rem;
    }
    .alert-banner .alert-title { color: #ff6b6b; font-weight: 600; }
    .alert-banner .alert-detail { color: #c9d1d9; margin-top: 2px; }

    .stat-card {
        background: linear-gradient(135deg, rgba(22,27,34,0.9), rgba(22,27,34,0.6));
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 12px;
        padding: 16px 20px;
        text-align: center;
        backdrop-filter: blur(10px);
        transition: border-color 0.2s;
    }
    .stat-card:hover { border-color: rgba(255,255,255,0.12); }
    .stat-card .value { font-size: 1.8rem; font-weight: 700; margin: 4px 0; line-height: 1.2; }
    .stat-card .label { font-size: 0.7rem; color: #8b949e; text-transform: uppercase; letter-spacing: 1px; }
    .stat-card.red .value { color: #ff6b6b; }
    .stat-card.yellow .value { color: #feca57; }
    .stat-card.green .value { color: #4ecdc4; }
    .stat-card.blue .value { color: #45b7d1; }
    .stat-card.purple .value { color: #a78bfa; }
    .stat-card.white .value { color: #e6edf3; }
    .stat-card.pink .value { color: #f093fb; }

    .pipe-card {
        background: rgba(22,27,34,0.8);
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 12px;
        padding: 14px;
        margin-bottom: 10px;
    }
    .pipe-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 5px 0;
        border-bottom: 1px solid rgba(255,255,255,0.04);
    }
    .pipe-row:last-child { border-bottom: none; }
    .pipe-label { color: #8b949e; font-size: 0.78rem; }
    .pipe-value { color: #e6edf3; font-weight: 600; font-size: 0.82rem; }
    .status-dot {
        display: inline-block; width: 8px; height: 8px;
        border-radius: 50%; margin-right: 6px;
        animation: pulse 2s infinite;
    }
    .status-dot.green { background: #4ecdc4; box-shadow: 0 0 6px #4ecdc4; }
    .status-dot.yellow { background: #feca57; box-shadow: 0 0 6px #feca57; }
    .status-dot.red { background: #ff6b6b; box-shadow: 0 0 6px #ff6b6b; }

    @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }

    .section-title {
        font-size: 1.05rem; font-weight: 600; color: #e6edf3;
        margin: 24px 0 14px 0; padding-bottom: 6px;
        border-bottom: 2px solid rgba(78,205,196,0.3);
    }

    .depth-legend {
        display: flex; align-items: center; gap: 4px;
        font-size: 0.72rem; color: #8b949e; margin-top: 6px;
    }
    .depth-legend .bar { height: 10px; flex: 1; border-radius: 2px; }

    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0d1117 0%, #0a0f1a 100%);
        border-right: 1px solid rgba(255,255,255,0.06);
    }
    section[data-testid="stSidebar"] .stMarkdown h1 {
        font-size: 1.2rem; color: #e6edf3;
    }
</style>
""", unsafe_allow_html=True)


# ── BigQuery client ──────────────────────────────────────────────────────
@st.cache_resource
def get_bq_client():
    return bigquery.Client(project=PROJECT_ID or None)


# ── Data loading ─────────────────────────────────────────────────────────
@st.cache_data(ttl=55)
def load_unified_events(hours: int, selected_sources: list[str] | None = None) -> pd.DataFrame:
    """Load deduplicated events from BigQuery unified_events."""
    try:
        client = get_bq_client()
        project = client.project

        source_filter = ""
        if selected_sources and set(selected_sources) != set(ALL_SOURCES):
            sources_sql = ", ".join(f"'{s}'" for s in selected_sources)
            source_filter = f"AND preferred_source IN ({sources_sql})"

        query = f"""
            SELECT unified_event_id AS id,
                   magnitude_value AS magnitude,
                   place,
                   origin_time_utc AS time,
                   longitude, latitude,
                   depth_km AS depth,
                   num_sources, preferred_source,
                   region, status, updated_at AS ingested_at,
                   magnitude_std, location_spread_km, source_agreement_score,
                   source_event_uids
            FROM `{project}.{DATASET}.unified_events`
            WHERE origin_time_utc >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
            {source_filter}
            ORDER BY origin_time_utc DESC
        """
        df = client.query(query).to_dataframe()
        if not df.empty:
            df["time"] = pd.to_datetime(df["time"], utc=True)
            df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True)
            if "url" not in df.columns:
                df["url"] = ""
        return df
    except Exception as e:
        st.error(f"BigQuery error: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=55)
def load_raw_events_by_source(hours: int) -> pd.DataFrame:
    """Load raw events grouped by source for coverage analysis."""
    try:
        client = get_bq_client()
        project = client.project
        query = f"""
            SELECT source, latitude, longitude, magnitude_value AS magnitude,
                   origin_time_utc AS time, event_uid
            FROM `{project}.{DATASET}.raw_events`
            WHERE origin_time_utc >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
            QUALIFY ROW_NUMBER() OVER (PARTITION BY event_uid ORDER BY fetched_at DESC) = 1
        """
        df = client.query(query).to_dataframe()
        if not df.empty:
            df["time"] = pd.to_datetime(df["time"], utc=True)
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=55)
def load_pipeline_health() -> dict:
    """Load pipeline health metrics from BigQuery."""
    try:
        client = get_bq_client()
        project = client.project

        # Pipeline run stats
        run_query = f"""
            SELECT
                COUNT(*) AS total_runs,
                COUNTIF(status = 'ok') AS ok_runs,
                COUNTIF(status = 'failed') AS failed_runs,
                MAX(started_at) AS last_run,
                AVG(duration_seconds) AS avg_duration,
                SUM(raw_events_count) AS total_raw,
                SUM(unified_events_count) AS total_unified
            FROM `{project}.{DATASET}.pipeline_runs`
            WHERE started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
        """
        run_row = list(client.query(run_query).result())[0]

        # Unified event stats
        event_query = f"""
            SELECT
                COUNT(*) AS total_events,
                COUNTIF(num_sources > 1) AS multi_source,
                MAX(updated_at) AS last_update
            FROM `{project}.{DATASET}.unified_events`
        """
        ev_row = list(client.query(event_query).result())[0]

        # Source breakdown
        source_query = f"""
            SELECT source, COUNT(DISTINCT event_uid) AS cnt
            FROM `{project}.{DATASET}.raw_events`
            WHERE origin_time_utc >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            GROUP BY source ORDER BY cnt DESC
        """
        source_rows = list(client.query(source_query).result())

        # Per-source pipeline health
        per_source_query = f"""
            SELECT
                source_name,
                COUNT(*) AS runs,
                COUNTIF(status = 'ok') AS ok_count,
                COUNTIF(status = 'failed') AS failed_count,
                MAX(started_at) AS last_run,
                AVG(duration_seconds) AS avg_duration
            FROM `{project}.{DATASET}.pipeline_runs`
            WHERE started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
              AND source_name IS NOT NULL
            GROUP BY source_name
        """
        per_source_rows = list(client.query(per_source_query).result())

        # Dead letter counts by source
        dead_query = f"""
            SELECT source, COUNT(*) AS cnt
            FROM `{project}.{DATASET}.dead_letter_events`
            WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            GROUP BY source
        """
        dead_rows = list(client.query(dead_query).result())

        return {
            "total_runs": run_row.total_runs,
            "ok_runs": run_row.ok_runs,
            "failed_runs": run_row.failed_runs,
            "last_run": run_row.last_run,
            "avg_duration": run_row.avg_duration,
            "total_raw_1h": run_row.total_raw,
            "total_unified_1h": run_row.total_unified,
            "total_events": ev_row.total_events,
            "multi_source": ev_row.multi_source,
            "last_update": ev_row.last_update,
            "sources": {r.source: r.cnt for r in source_rows},
            "per_source": {
                r.source_name: {
                    "runs": r.runs,
                    "ok_count": r.ok_count,
                    "failed_count": r.failed_count,
                    "last_run": r.last_run,
                    "avg_duration": r.avg_duration,
                }
                for r in per_source_rows
            },
            "dead_letters": {r.source: r.cnt for r in dead_rows},
        }
    except Exception:
        return {
            "total_runs": 0, "ok_runs": 0, "failed_runs": 0,
            "last_run": None, "avg_duration": 0,
            "total_raw_1h": 0, "total_unified_1h": 0,
            "total_events": 0, "multi_source": 0, "last_update": None,
            "sources": {}, "per_source": {}, "dead_letters": {},
        }


# ── Chart layout template ────────────────────────────────────────────────
CHART_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(22,27,34,0.6)",
    font=dict(family="Inter", color="#8b949e", size=11),
    margin=dict(l=50, r=20, t=40, b=40),
    height=320,
)


# ── Sidebar ──────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("# SeisMonitor")
    st.caption("Real-time seismic monitor — GCP Serverless")
    st.markdown("---")

    st.markdown("**Data Filters**")
    time_range = st.selectbox(
        "Time Range",
        options=[("Last Hour", 1), ("Last 6 Hours", 6), ("Last 24 Hours", 24),
                 ("Last 3 Days", 72), ("Last 7 Days", 168)],
        format_func=lambda x: x[0],
        index=2,
    )
    hours = time_range[1]
    min_mag = st.slider("Min Magnitude", 0.0, 8.0, 0.0, 0.5)

    st.markdown("---")

    st.markdown("**Source Filter**")
    selected_sources = st.multiselect(
        "Sources",
        options=ALL_SOURCES,
        default=ALL_SOURCES,
        help="Select which sources to include in the data",
    )
    if not selected_sources:
        selected_sources = ALL_SOURCES

    st.markdown("---")

    st.markdown("**Map Controls**")
    map_view = st.radio("Map View", ["Globe", "Interactive Map"], index=0, horizontal=True)

    if map_view == "Globe":
        map_projection = st.selectbox(
            "Projection",
            ["orthographic", "natural earth", "equirectangular"],
            index=0,
        )
    else:
        map_style_name = st.selectbox("Map Style", list(MAPBOX_STYLES.keys()), index=0)

    color_by = st.radio("Color By", ["Depth", "Magnitude"], index=0, horizontal=True)
    show_plates = st.checkbox("Tectonic plates", value=True)

    st.markdown("---")
    auto_refresh = st.checkbox("Auto-refresh (60s)", value=True)

    st.markdown("---")
    st.markdown("[USGS Live](https://earthquake.usgs.gov/earthquakes/map/)")


# ── Load data ─────────────────────────────────────────────────────────────
df = load_unified_events(hours, selected_sources)
if min_mag > 0 and not df.empty:
    df = df[df["magnitude"] >= min_mag]
pipeline = load_pipeline_health()

# ── Data freshness ────────────────────────────────────────────────────────
now = datetime.now(timezone.utc)
last_update = pipeline.get("last_update") or pipeline.get("last_run")
if last_update and hasattr(last_update, "timestamp"):
    if hasattr(last_update, "tzinfo") and last_update.tzinfo is None:
        last_update = last_update.replace(tzinfo=timezone.utc)
    staleness = (now - last_update).total_seconds()
    if staleness < 120:
        fresh_class, fresh_label = "fresh", "Live"
    elif staleness < 600:
        fresh_class, fresh_label = "stale", f"{int(staleness // 60)}m ago"
    else:
        fresh_class, fresh_label = "offline", f"{int(staleness // 60)}m ago"
else:
    fresh_class, fresh_label = "offline", "No data"

# ── Header ────────────────────────────────────────────────────────────────
sources_label = " + ".join(s.upper() for s in selected_sources)
st.markdown(f"""
<div class="dash-header">
    <div>
        <h1>SeisMonitor</h1>
        <p class="subtitle">Live seismic data &middot; {sources_label} &rarr; Cloud Run &rarr; BigQuery &middot; {time_range[0]}</p>
    </div>
    <div class="freshness">
        <span class="freshness-dot {fresh_class}"></span>
        <span>{fresh_label} &middot; {now:%H:%M:%S UTC}</span>
    </div>
</div>
""", unsafe_allow_html=True)

# ── Alert banner ──────────────────────────────────────────────────────────
if not df.empty:
    significant = df[df["magnitude"] >= 5.0]
    recent_significant = significant[
        significant["time"] >= (now - timedelta(hours=max(hours, 24)))
    ] if not significant.empty else significant

    if not recent_significant.empty:
        top = recent_significant.iloc[0]
        st.markdown(f"""
        <div class="alert-banner">
            <div class="alert-title">Significant Seismic Event Detected</div>
            <div class="alert-detail">
                M {top.magnitude:.1f} &mdash; {top.place}
                &middot; Depth: {top.depth:.1f} km
                &middot; {top.time:%Y-%m-%d %H:%M UTC}
            </div>
        </div>
        """, unsafe_allow_html=True)

# ── KPI stat cards ────────────────────────────────────────────────────────
if not df.empty:
    cards = [
        ("white",  "Total Events",  f"{len(df):,}"),
        ("red",    "Max Magnitude", f"{df['magnitude'].max():.1f}"),
        ("purple", "Avg Magnitude", f"{df['magnitude'].mean():.1f}"),
        ("red",    "M 5.0+",        f"{(df['magnitude'] >= 5.0).sum()}"),
        ("yellow", "M 3.0 - 4.9",   f"{((df['magnitude'] >= 3.0) & (df['magnitude'] < 5.0)).sum()}"),
        ("green",  "Below M 3.0",   f"{(df['magnitude'] < 3.0).sum()}"),
        ("blue",   "Multi-Source",   f"{(df['num_sources'] > 1).sum() if 'num_sources' in df.columns else 0}"),
        ("blue",   "Max Depth",     f"{df['depth'].max():.0f} km"),
    ]
    cols = st.columns(len(cards))
    for col, (color, label, value) in zip(cols, cards):
        col.markdown(f"""
        <div class="stat-card {color}">
            <div class="label">{label}</div>
            <div class="value">{value}</div>
        </div>
        """, unsafe_allow_html=True)

    # Per-source KPI cards row
    source_counts = pipeline.get("sources", {})
    if source_counts:
        st.markdown("")  # spacer
        src_cols = st.columns(len(source_counts))
        for col, (src, cnt) in zip(src_cols, source_counts.items()):
            color_hex = SOURCE_COLORS.get(src, "#8b949e")
            col.markdown(f"""
            <div class="stat-card" style="border-left: 3px solid {color_hex};">
                <div class="label">{src.upper()} (24h)</div>
                <div class="value" style="color: {color_hex};">{cnt:,}</div>
            </div>
            """, unsafe_allow_html=True)
else:
    st.warning("No earthquake data yet. The pipeline runs every minute - data will appear shortly.")
    st.stop()

# ── Map section ───────────────────────────────────────────────────────────
st.markdown('<div class="section-title">Seismic Activity Map</div>', unsafe_allow_html=True)

col_map, col_pipe = st.columns([4, 1])

with col_map:
    color_key = color_by.lower()

    if map_view == "Globe":
        fig_map = build_globe_map(
            df, show_plates=show_plates, color_by=color_key,
            projection=map_projection,
        )
    else:
        style = MAPBOX_STYLES.get(map_style_name, "carto-darkmatter")
        fig_map = build_mapbox_map(
            df, show_plates=show_plates, color_by=color_key,
            map_style=style,
        )

    st.plotly_chart(fig_map, use_container_width=True, config={"scrollZoom": True})

    if color_key == "depth":
        st.markdown("""
        <div class="depth-legend">
            <span>0 km</span>
            <div class="bar" style="background: linear-gradient(90deg, #d73027, #f46d43, #fdae61, #fee08b, #d9ef8b, #91cf60, #1a9850, #313695);"></div>
            <span>100 km</span>
            <span style="margin-left: 8px;">(Shallow &rarr; Deep)</span>
        </div>
        """, unsafe_allow_html=True)

# Pipeline health panel
with col_pipe:
    st.markdown('<div class="section-title">Pipeline</div>', unsafe_allow_html=True)

    runs_ok = pipeline["ok_runs"]
    runs_fail = pipeline["failed_runs"]
    pipe_dot = "green" if runs_ok > 0 and runs_fail == 0 else ("red" if runs_fail > runs_ok else "green")
    pipe_label = "Healthy" if runs_fail == 0 else f"{runs_fail} failed"

    sources = pipeline.get("sources", {})
    source_str = ", ".join(f"{s}: {c}" for s, c in sources.items()) if sources else "-"

    st.markdown(f"""
    <div class="pipe-card">
        <div class="pipe-row">
            <span class="pipe-label">Pipeline</span>
            <span class="pipe-value"><span class="status-dot {pipe_dot}"></span>{pipe_label}</span>
        </div>
        <div class="pipe-row">
            <span class="pipe-label">Runs (1h)</span>
            <span class="pipe-value">{pipeline['total_runs']}</span>
        </div>
        <div class="pipe-row">
            <span class="pipe-label">Avg Duration</span>
            <span class="pipe-value">{pipeline['avg_duration']:.1f}s</span>
        </div>
        <div class="pipe-row">
            <span class="pipe-label">Multi-source</span>
            <span class="pipe-value">{pipeline['multi_source']}</span>
        </div>
    </div>
    <div class="pipe-card">
        <div class="pipe-row">
            <span class="pipe-label">BigQuery</span>
            <span class="pipe-value"><span class="status-dot green"></span>Connected</span>
        </div>
        <div class="pipe-row">
            <span class="pipe-label">Total Events</span>
            <span class="pipe-value">{pipeline['total_events']:,}</span>
        </div>
        <div class="pipe-row">
            <span class="pipe-label">Sources (24h)</span>
            <span class="pipe-value">{source_str}</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    if pipeline.get("last_run") and hasattr(pipeline["last_run"], "strftime"):
        st.caption(f"Last pipeline run: {pipeline['last_run']:%H:%M:%S UTC}")


# ── Analytics section (tabbed) ────────────────────────────────────────────
st.markdown('<div class="section-title">Analytics</div>', unsafe_allow_html=True)

tab_freq, tab_mag, tab_depth, tab_regions, tab_coverage, tab_compare, tab_health = st.tabs([
    "Frequency", "Magnitude", "Depth", "Regions",
    "Source Coverage", "Source Comparison", "Pipeline Health",
])

with tab_freq:
    if hours <= 6:
        resample_rule, bar_label = "30min", "30-Minute Intervals"
    elif hours <= 48:
        resample_rule, bar_label = "1h", "Hourly"
    else:
        resample_rule, bar_label = "6h", "6-Hour Intervals"

    ts = df.set_index("time").resample(resample_rule).size().reset_index(name="count")
    fig_ts = go.Figure()
    fig_ts.add_trace(go.Bar(
        x=ts["time"], y=ts["count"],
        marker=dict(
            color=ts["count"],
            colorscale=[[0, "#0d2137"], [0.5, "#4ecdc4"], [1, "#a78bfa"]],
            line=dict(width=0),
        ),
    ))
    fig_ts.update_layout(
        **CHART_LAYOUT,
        title=dict(text=f"Earthquake Frequency ({bar_label})", font=dict(size=13, color="#e6edf3")),
        xaxis_title="Time (UTC)", yaxis_title="Events",
        bargap=0.15,
    )
    st.plotly_chart(fig_ts, use_container_width=True)

    cum_ts = df.sort_values("time").copy()
    cum_ts["cumulative"] = range(1, len(cum_ts) + 1)
    fig_cum = go.Figure()
    fig_cum.add_trace(go.Scatter(
        x=cum_ts["time"], y=cum_ts["cumulative"],
        mode="lines", line=dict(color="#4ecdc4", width=2),
        fill="tozeroy", fillcolor="rgba(78,205,196,0.1)",
    ))
    fig_cum.update_layout(
        **CHART_LAYOUT,
        title=dict(text="Cumulative Events", font=dict(size=13, color="#e6edf3")),
        xaxis_title="Time (UTC)", yaxis_title="Total Events",
    )
    st.plotly_chart(fig_cum, use_container_width=True)

with tab_mag:
    c1, c2 = st.columns(2)
    with c1:
        fig_hist = go.Figure()
        fig_hist.add_trace(go.Histogram(
            x=df["magnitude"], nbinsx=30,
            marker=dict(
                color=df["magnitude"],
                colorscale=[[0, "#4ecdc4"], [0.5, "#feca57"], [1, "#ff6b6b"]],
                line=dict(width=0.5, color="rgba(255,255,255,0.1)"),
            ),
        ))
        fig_hist.update_layout(
            **CHART_LAYOUT,
            title=dict(text="Magnitude Distribution", font=dict(size=13, color="#e6edf3")),
            xaxis_title="Magnitude", yaxis_title="Count",
            bargap=0.05,
        )
        st.plotly_chart(fig_hist, use_container_width=True)

    with c2:
        fig_mag_t = go.Figure()
        fig_mag_t.add_trace(go.Scatter(
            x=df["time"], y=df["magnitude"],
            mode="markers",
            marker=dict(
                size=6, color=df["magnitude"],
                colorscale=[[0, "#1a9641"], [0.4, "#fee08b"], [0.7, "#f46d43"], [1, "#d73027"]],
                opacity=0.7,
                line=dict(width=0.3, color="rgba(255,255,255,0.15)"),
            ),
            hovertemplate="<b>M%{y:.1f}</b><br>%{x}<extra></extra>",
        ))
        fig_mag_t.update_layout(
            **CHART_LAYOUT,
            title=dict(text="Magnitude Over Time", font=dict(size=13, color="#e6edf3")),
            xaxis_title="Time (UTC)", yaxis_title="Magnitude",
        )
        st.plotly_chart(fig_mag_t, use_container_width=True)

with tab_depth:
    c1, c2 = st.columns(2)
    with c1:
        fig_depth_h = go.Figure()
        fig_depth_h.add_trace(go.Histogram(
            x=df["depth"], nbinsx=30,
            marker=dict(
                color="#45b7d1",
                line=dict(width=0.5, color="rgba(255,255,255,0.1)"),
            ),
        ))
        fig_depth_h.update_layout(
            **CHART_LAYOUT,
            title=dict(text="Depth Distribution", font=dict(size=13, color="#e6edf3")),
            xaxis_title="Depth (km)", yaxis_title="Count",
            bargap=0.05,
        )
        st.plotly_chart(fig_depth_h, use_container_width=True)

    with c2:
        fig_sc = go.Figure()
        fig_sc.add_trace(go.Scatter(
            x=df["magnitude"], y=df["depth"],
            mode="markers",
            marker=dict(
                size=8, color=df["depth"],
                colorscale=[[0, "#d73027"], [0.15, "#fdae61"], [0.35, "#d9ef8b"],
                            [0.6, "#1a9850"], [1, "#313695"]],
                cmin=0, cmax=100,
                colorbar=dict(
                    title=dict(text="Depth (km)", font=dict(color="#8b949e", size=10)),
                    tickfont=dict(color="#8b949e", size=9),
                    len=0.8, thickness=10,
                ),
                opacity=0.75,
                line=dict(width=0.5, color="rgba(255,255,255,0.15)"),
            ),
            text=df["place"],
            hovertemplate="<b>M%{x:.1f}</b><br>Depth: %{y:.1f} km<br>%{text}<extra></extra>",
        ))
        fig_sc.update_layout(
            **CHART_LAYOUT,
            title=dict(text="Depth vs Magnitude", font=dict(size=13, color="#e6edf3")),
            xaxis_title="Magnitude", yaxis_title="Depth (km)",
            yaxis=dict(autorange="reversed"),
        )
        st.plotly_chart(fig_sc, use_container_width=True)

with tab_regions:
    c1, c2 = st.columns(2)
    with c1:
        regions = df["place"].apply(lambda p: p.split(", ")[-1] if isinstance(p, str) and ", " in p else (p or "Unknown"))
        top = regions.value_counts().head(12).reset_index()
        top.columns = ["Region", "Count"]
        fig_reg = go.Figure()
        fig_reg.add_trace(go.Bar(
            x=top["Count"], y=top["Region"],
            orientation="h",
            marker=dict(
                color=top["Count"],
                colorscale=[[0, "#0d2137"], [0.5, "#45b7d1"], [1, "#4ecdc4"]],
                line=dict(width=0),
            ),
        ))
        fig_reg.update_layout(
            **CHART_LAYOUT,
            title=dict(text="Most Active Regions", font=dict(size=13, color="#e6edf3")),
            yaxis=dict(autorange="reversed"),
            xaxis_title="Events",
        )
        st.plotly_chart(fig_reg, use_container_width=True)

    with c2:
        top_regions = regions.value_counts().head(8).index.tolist()
        df_top = df.copy()
        df_top["region_parsed"] = regions
        df_top = df_top[df_top["region_parsed"].isin(top_regions)]
        fig_box = go.Figure()
        for region in top_regions:
            subset = df_top[df_top["region_parsed"] == region]
            fig_box.add_trace(go.Box(
                y=subset["magnitude"], name=region[:20],
                marker_color="#4ecdc4", line=dict(color="#4ecdc4"),
                fillcolor="rgba(78,205,196,0.15)",
            ))
        fig_box.update_layout(
            **CHART_LAYOUT,
            title=dict(text="Magnitude by Region", font=dict(size=13, color="#e6edf3")),
            yaxis_title="Magnitude",
            showlegend=False,
        )
        st.plotly_chart(fig_box, use_container_width=True)

# ── Source Coverage tab ──────────────────────────────────────────────────
with tab_coverage:
    raw_df = load_raw_events_by_source(hours)
    if not raw_df.empty:
        fig_coverage = go.Figure()
        for src in ALL_SOURCES:
            src_df = raw_df[raw_df["source"] == src]
            if src_df.empty:
                continue
            fig_coverage.add_trace(go.Scattergeo(
                lat=src_df["latitude"],
                lon=src_df["longitude"],
                text=src_df.apply(lambda r: f"M{r['magnitude']:.1f} - {src.upper()}", axis=1),
                marker=dict(
                    size=5,
                    color=SOURCE_COLORS.get(src, "#888"),
                    opacity=0.6,
                    line=dict(width=0),
                ),
                name=src.upper(),
                hovertemplate="<b>%{text}</b><br>%{lat:.2f}, %{lon:.2f}<extra></extra>",
            ))

        fig_coverage.update_layout(
            **{**CHART_LAYOUT, "height": 500},
            title=dict(text="Event Coverage by Source", font=dict(size=13, color="#e6edf3")),
            geo=dict(
                bgcolor="rgba(0,0,0,0)",
                showland=True,
                landcolor="rgba(22,27,34,0.8)",
                showocean=True,
                oceancolor="rgba(10,15,26,0.8)",
                showcoastlines=True,
                coastlinecolor="rgba(255,255,255,0.15)",
                projection_type="natural earth",
            ),
            legend=dict(font=dict(color="#e6edf3")),
        )
        st.plotly_chart(fig_coverage, use_container_width=True)

        # Source event count bar chart
        src_counts = raw_df.groupby("source").size().reindex(ALL_SOURCES, fill_value=0)
        fig_src_bar = go.Figure()
        fig_src_bar.add_trace(go.Bar(
            x=[s.upper() for s in src_counts.index],
            y=src_counts.values,
            marker_color=[SOURCE_COLORS.get(s, "#888") for s in src_counts.index],
        ))
        fig_src_bar.update_layout(
            **CHART_LAYOUT,
            title=dict(text=f"Events per Source ({time_range[0]})", font=dict(size=13, color="#e6edf3")),
            yaxis_title="Event Count",
        )
        st.plotly_chart(fig_src_bar, use_container_width=True)
    else:
        st.info("No raw event data available for coverage analysis.")

# ── Source Comparison tab ────────────────────────────────────────────────
with tab_compare:
    if not df.empty and "magnitude_std" in df.columns:
        c1, c2 = st.columns(2)

        with c1:
            # Magnitude std histogram
            mag_std_data = df[df["magnitude_std"] > 0]["magnitude_std"]
            if not mag_std_data.empty:
                fig_mag_std = go.Figure()
                fig_mag_std.add_trace(go.Histogram(
                    x=mag_std_data, nbinsx=25,
                    marker=dict(color="#a78bfa", line=dict(width=0.5, color="rgba(255,255,255,0.1)")),
                ))
                fig_mag_std.update_layout(
                    **CHART_LAYOUT,
                    title=dict(text="Magnitude Std Dev (multi-source events)", font=dict(size=13, color="#e6edf3")),
                    xaxis_title="Magnitude Std Dev", yaxis_title="Count",
                )
                st.plotly_chart(fig_mag_std, use_container_width=True)

        with c2:
            # Location spread histogram
            spread_data = df[df["location_spread_km"] > 0]["location_spread_km"]
            if not spread_data.empty:
                fig_spread = go.Figure()
                fig_spread.add_trace(go.Histogram(
                    x=spread_data, nbinsx=25,
                    marker=dict(color="#45b7d1", line=dict(width=0.5, color="rgba(255,255,255,0.1)")),
                ))
                fig_spread.update_layout(
                    **CHART_LAYOUT,
                    title=dict(text="Location Spread (km)", font=dict(size=13, color="#e6edf3")),
                    xaxis_title="Max Pairwise Distance (km)", yaxis_title="Count",
                )
                st.plotly_chart(fig_spread, use_container_width=True)

        # Source agreement score distribution
        agreement_data = df[df["num_sources"] > 1]["source_agreement_score"]
        if not agreement_data.empty:
            fig_agree = go.Figure()
            fig_agree.add_trace(go.Histogram(
                x=agreement_data, nbinsx=20,
                marker=dict(color="#4ecdc4", line=dict(width=0.5, color="rgba(255,255,255,0.1)")),
            ))
            fig_agree.update_layout(
                **CHART_LAYOUT,
                title=dict(text="Source Agreement Score (multi-source events)", font=dict(size=13, color="#e6edf3")),
                xaxis_title="Agreement Score (unique sources / total members)", yaxis_title="Count",
            )
            st.plotly_chart(fig_agree, use_container_width=True)

        # Multi-source events table (delta view)
        multi_df = df[df["num_sources"] > 1].head(20)
        if not multi_df.empty:
            st.markdown("**Multi-Source Event Details**")
            display_cols = ["id", "magnitude", "place", "num_sources", "preferred_source",
                            "magnitude_std", "location_spread_km", "source_agreement_score"]
            display_cols = [c for c in display_cols if c in multi_df.columns]
            st.dataframe(
                multi_df[display_cols],
                use_container_width=True,
                height=300,
            )
    else:
        st.info("Quality metrics not yet available. Run the updated pipeline to generate comparison data.")

# ── Pipeline Health / Observability tab ──────────────────────────────────
with tab_health:
    per_source = pipeline.get("per_source", {})
    dead_letters = pipeline.get("dead_letters", {})

    if per_source:
        st.markdown("**Per-Source Pipeline Status (24h)**")

        health_data = []
        for src in ALL_SOURCES:
            info = per_source.get(src, {})
            dl_count = dead_letters.get(src, 0)
            runs = info.get("runs", 0)
            ok = info.get("ok_count", 0)
            failed = info.get("failed_count", 0)
            success_rate = (ok / runs * 100) if runs > 0 else 0

            if runs == 0:
                status_html = '<span class="status-dot yellow"></span>No data'
            elif failed == 0:
                status_html = '<span class="status-dot green"></span>Healthy'
            elif failed < runs * 0.2:
                status_html = '<span class="status-dot yellow"></span>Degraded'
            else:
                status_html = '<span class="status-dot red"></span>Failing'

            health_data.append({
                "Source": src.upper(),
                "Runs": runs,
                "Success": f"{success_rate:.0f}%",
                "Avg Duration": f"{info.get('avg_duration', 0):.1f}s",
                "Dead Letters": dl_count,
                "Status": status_html,
            })

        # Render as HTML table for colored status dots
        table_rows = ""
        for row in health_data:
            table_rows += f"""
            <tr>
                <td style="color: {SOURCE_COLORS.get(row['Source'].lower(), '#8b949e')}; font-weight: 600;">{row['Source']}</td>
                <td>{row['Runs']}</td>
                <td>{row['Success']}</td>
                <td>{row['Avg Duration']}</td>
                <td>{row['Dead Letters']}</td>
                <td>{row['Status']}</td>
            </tr>
            """

        st.markdown(f"""
        <table style="width: 100%; border-collapse: collapse; font-size: 0.85rem;">
            <thead>
                <tr style="border-bottom: 1px solid rgba(255,255,255,0.1); color: #8b949e;">
                    <th style="padding: 8px; text-align: left;">Source</th>
                    <th style="padding: 8px;">Runs</th>
                    <th style="padding: 8px;">Success</th>
                    <th style="padding: 8px;">Avg Duration</th>
                    <th style="padding: 8px;">Dead Letters</th>
                    <th style="padding: 8px;">Status</th>
                </tr>
            </thead>
            <tbody style="color: #e6edf3;">
                {table_rows}
            </tbody>
        </table>
        """, unsafe_allow_html=True)

        st.markdown("")  # spacer

        # Dead letter total
        total_dead = sum(dead_letters.values())
        if total_dead > 0:
            st.warning(f"Total dead-lettered events (24h): {total_dead}")
    else:
        st.info("Per-source pipeline data not yet available. Deploy per-source services to see health metrics.")

    # Overall pipeline stats
    st.markdown("**Overall Pipeline Stats (1h)**")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Runs", pipeline["total_runs"])
    c2.metric("Success", pipeline["ok_runs"])
    c3.metric("Failed", pipeline["failed_runs"])
    c4.metric("Avg Duration", f"{pipeline['avg_duration']:.1f}s")


# ── Recent events table ──────────────────────────────────────────────────
st.markdown('<div class="section-title">Recent Events</div>', unsafe_allow_html=True)

col_btns, _, _ = st.columns([2, 1, 7])
with col_btns:
    c_csv, c_json = st.columns(2)
    with c_csv:
        st.download_button("CSV", df.to_csv(index=False), "earthquakes.csv", "text/csv")
    with c_json:
        st.download_button("JSON", df.to_json(orient="records", date_format="iso"),
                           "earthquakes.json", "application/json")

if "num_sources" in df.columns:
    display = df[["id", "magnitude", "place", "depth", "latitude", "longitude",
                   "num_sources", "preferred_source", "time"]].copy()
    display["time"] = display["time"].dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    display.columns = ["Event ID", "Mag", "Location", "Depth (km)", "Lat", "Lon",
                        "Sources", "Preferred", "Time (UTC)"]
    col_config = {
        "Mag": st.column_config.NumberColumn(format="%.1f"),
        "Depth (km)": st.column_config.NumberColumn(format="%.1f"),
        "Lat": st.column_config.NumberColumn(format="%.3f"),
        "Lon": st.column_config.NumberColumn(format="%.3f"),
        "Sources": st.column_config.NumberColumn(format="%d"),
    }
else:
    display = df[["id", "magnitude", "place", "depth", "latitude", "longitude", "time"]].copy()
    display["time"] = display["time"].dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    display.columns = ["Event ID", "Mag", "Location", "Depth (km)", "Lat", "Lon", "Time (UTC)"]
    col_config = {
        "Mag": st.column_config.NumberColumn(format="%.1f"),
        "Depth (km)": st.column_config.NumberColumn(format="%.1f"),
        "Lat": st.column_config.NumberColumn(format="%.3f"),
        "Lon": st.column_config.NumberColumn(format="%.3f"),
    }

st.dataframe(
    display,
    use_container_width=True,
    height=420,
    column_config=col_config,
)


# ── Auto refresh ─────────────────────────────────────────────────────────
if auto_refresh:
    import time as _t
    _t.sleep(60)
    st.rerun()
