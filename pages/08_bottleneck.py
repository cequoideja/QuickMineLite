"""
Bottleneck Analysis page for QuickMineLite.

Uses the BottleneckAnalyzer to identify and visualize performance
bottlenecks across activity durations, waiting times, frequencies,
and resource workloads. Provides actionable recommendations.
"""

import streamlit as st
import pandas as pd
from analysis.bottleneck import BottleneckAnalyzer
from viz.charts import ChartBuilder
from core.helpers import format_duration, format_number

# ---------------------------------------------------------------------------
# Guard: require loaded data
# ---------------------------------------------------------------------------
if st.session_state.get('event_log_df') is None:
    st.info("No data loaded. Go to Import Data.")
    st.stop()

st.header("Bottleneck Analysis")

# ---------------------------------------------------------------------------
# Retrieve shared objects and create analyzer
# ---------------------------------------------------------------------------
filtered_df: pd.DataFrame = st.session_state['filtered_df']
chart_builder = ChartBuilder()

with st.spinner("Analyzing bottlenecks..."):
    bottleneck_analyzer = BottleneckAnalyzer(filtered_df)

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab_recs, tab_duration, tab_waiting, tab_freq, tab_resources = st.tabs(
    ["Recommendations", "Activity Duration", "Waiting Time", "Frequency", "Resources"]
)

# ---- Recommendations Tab --------------------------------------------------
with tab_recs:
    recommendations = bottleneck_analyzer.get_recommendations()
    if not recommendations:
        st.success("No significant bottleneck recommendations at this time.")
    else:
        for rec in recommendations:
            severity = rec.get('severity', 'Medium')
            rec_type = rec.get('type', '')
            description = rec.get('description', '')
            suggestion = rec.get('recommendation', '')

            if severity == 'High':
                st.error(f"**{rec_type}** -- {description}")
            else:
                st.warning(f"**{rec_type}** -- {description}")

            st.caption(suggestion)

# ---- Activity Duration Tab ------------------------------------------------
with tab_duration:
    st.subheader("Activity Duration Bottlenecks")
    duration_df = bottleneck_analyzer.analyze_activity_duration_bottlenecks(top_n=15)

    if duration_df.empty:
        st.info("No activity duration data available.")
    else:
        # Display table with formatted durations
        display_cols = ['activity', 'mean_duration_formatted', 'median_duration_formatted',
                        'count', 'bottleneck_score']
        available_display = [c for c in display_cols if c in duration_df.columns]
        st.dataframe(duration_df[available_display], use_container_width=True)

        # Bar chart of top bottlenecks
        chart_df = duration_df.head(10).copy()
        fig = chart_builder.create_bar_chart(
            chart_df, 'activity', 'mean_duration',
            "Top Activity Duration Bottlenecks",
            x_label="Activity", y_label="Mean Duration (seconds)",
        )
        st.plotly_chart(fig, use_container_width=True)

# ---- Waiting Time Tab -----------------------------------------------------
with tab_waiting:
    st.subheader("Waiting Time Bottlenecks")
    waiting_df = bottleneck_analyzer.analyze_waiting_time_bottlenecks(top_n=15)

    if waiting_df.empty:
        st.info("No waiting time data available.")
    else:
        # Display table
        display_cols = ['transition', 'mean_waiting_formatted', 'median_waiting_formatted',
                        'count', 'bottleneck_score']
        available_display = [c for c in display_cols if c in waiting_df.columns]
        st.dataframe(waiting_df[available_display], use_container_width=True)

        # Bar chart
        chart_df = waiting_df.head(10).copy()
        fig = chart_builder.create_bar_chart(
            chart_df, 'transition', 'mean_waiting',
            "Top Waiting Time Bottlenecks",
            x_label="Transition", y_label="Mean Waiting Time (seconds)",
        )
        st.plotly_chart(fig, use_container_width=True)

# ---- Frequency Tab --------------------------------------------------------
with tab_freq:
    st.subheader("Frequency Bottlenecks")
    freq_df = bottleneck_analyzer.analyze_frequency_bottlenecks(top_n=15)

    if freq_df.empty:
        st.info("No frequency data available.")
    else:
        # Display table
        display_cols = ['activity', 'total_occurrences', 'percentage',
                        'avg_per_case', 'case_coverage_%', 'bottleneck_score']
        available_display = [c for c in display_cols if c in freq_df.columns]
        st.dataframe(freq_df[available_display], use_container_width=True)

        # Bar chart
        chart_df = freq_df.head(10).copy()
        fig = chart_builder.create_bar_chart(
            chart_df, 'activity', 'total_occurrences',
            "Top Activity Frequencies",
            x_label="Activity", y_label="Total Occurrences",
        )
        st.plotly_chart(fig, use_container_width=True)

# ---- Resources Tab --------------------------------------------------------
with tab_resources:
    st.subheader("Resource Bottlenecks")
    resource_df = bottleneck_analyzer.analyze_resource_bottlenecks(top_n=15)

    if resource_df is None:
        st.info("No resource column detected in the dataset. Resource bottleneck analysis requires a resource attribute (e.g., org:resource).")
    elif resource_df.empty:
        st.info("No resource bottleneck data available.")
    else:
        # Display table
        display_cols = ['resource', 'total_events', 'unique_cases',
                        'avg_events_per_case', 'workload_%', 'bottleneck_score']
        available_display = [c for c in display_cols if c in resource_df.columns]
        st.dataframe(resource_df[available_display], use_container_width=True)

        # Bar chart
        chart_df = resource_df.head(10).copy()
        fig = chart_builder.create_bar_chart(
            chart_df, 'resource', 'total_events',
            "Top Resource Workloads",
            x_label="Resource", y_label="Total Events",
        )
        st.plotly_chart(fig, use_container_width=True)
