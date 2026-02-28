"""
Dashboard page -- key metrics, activity distribution, events over time,
events-per-case histogram, and start/end activity charts.
"""
import streamlit as st
from viz.charts import ChartBuilder
from core.helpers import format_duration, format_number

# ── Guard: data must be loaded ───────────────────────────────────────────────
if st.session_state.get("event_log_df") is None:
    st.info("No event log loaded. Please import data first.")
    st.stop()

duckdb_mgr = st.session_state["duckdb_mgr"]
chart = ChartBuilder()

st.header("Dashboard")

# ── Row 1: KPI metric cards ─────────────────────────────────────────────────
summary = duckdb_mgr.get_summary_stats()
duration_stats = duckdb_mgr.get_case_duration_stats()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Cases", format_number(summary["total_cases"]))
c2.metric("Total Events", format_number(summary["total_events"]))
c3.metric("Total Activities", format_number(summary["total_activities"]))
c4.metric("Avg Duration", format_duration(duration_stats["avg"]))

# ── Row 2: Activity distribution + Events over time ─────────────────────────
left, right = st.columns(2)

with left:
    activity_df = duckdb_mgr.get_activity_distribution(limit=15)
    fig_act = chart.create_bar_chart(
        activity_df,
        x_col="activity",
        y_col="count",
        title="Activity Distribution (Top 15)",
        x_label="Activity",
        y_label="Count",
    )
    st.plotly_chart(fig_act, use_container_width=True)

with right:
    events_time_df = duckdb_mgr.get_events_over_time()
    fig_time = chart.create_time_series(
        events_time_df,
        date_col="date",
        value_cols=["event_count", "case_count"],
        title="Events & Cases Over Time",
        y_label="Count",
    )
    st.plotly_chart(fig_time, use_container_width=True)

# ── Row 3: Events per case + Start/End activities ───────────────────────────
left2, right2 = st.columns(2)

with left2:
    epc_df = duckdb_mgr.get_events_per_case_distribution()
    fig_epc = chart.create_bar_chart(
        epc_df,
        x_col="num_events",
        y_col="case_count",
        title="Events per Case Distribution",
        x_label="Number of Events",
        y_label="Number of Cases",
    )
    st.plotly_chart(fig_epc, use_container_width=True)

with right2:
    tab_start, tab_end = st.tabs(["Start Activities", "End Activities"])

    with tab_start:
        start_df = duckdb_mgr.get_start_activities()
        fig_start = chart.create_bar_chart(
            start_df,
            x_col="activity",
            y_col="count",
            title="Start Activities",
            x_label="Activity",
            y_label="Count",
        )
        st.plotly_chart(fig_start, use_container_width=True)

    with tab_end:
        end_df = duckdb_mgr.get_end_activities()
        fig_end = chart.create_bar_chart(
            end_df,
            x_col="activity",
            y_col="count",
            title="End Activities",
            x_label="Activity",
            y_label="Count",
        )
        st.plotly_chart(fig_end, use_container_width=True)
