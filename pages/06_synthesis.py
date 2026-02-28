"""
Synthesis Analysis page for QuickMineLite.

Runs a comprehensive analysis suite and displays results in organized
expanders covering summary statistics, correlations, data quality,
variant distribution, and activity distribution.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from viz.charts import ChartBuilder
from core.helpers import format_duration, format_number

# ---------------------------------------------------------------------------
# Guard: require loaded data
# ---------------------------------------------------------------------------
if st.session_state.get('event_log_df') is None:
    st.info("No data loaded. Go to Import Data.")
    st.stop()

st.header("Synthesis Analysis")

# ---------------------------------------------------------------------------
# Retrieve shared objects
# ---------------------------------------------------------------------------
analyzer = st.session_state['analyzer']
chart_builder = ChartBuilder()

# ---------------------------------------------------------------------------
# Run Full Analysis button
# ---------------------------------------------------------------------------
if st.button("Run Full Analysis", type="primary"):
    with st.spinner("Running full analysis -- this may take a moment..."):
        # Collect all analysis results into session_state so they persist
        st.session_state['synthesis_summary'] = analyzer.get_summary_statistics()
        st.session_state['synthesis_correlations'] = analyzer.analyze_correlations()
        st.session_state['synthesis_quality'] = analyzer.analyze_data_quality()
        st.session_state['synthesis_variants'] = analyzer.get_variant_statistics() if hasattr(analyzer, 'get_variant_statistics') else None
        st.session_state['synthesis_activities'] = analyzer.get_activity_distribution()

    st.success("Analysis complete.")

# ---------------------------------------------------------------------------
# Display results (only if analysis has been run)
# ---------------------------------------------------------------------------
if 'synthesis_summary' not in st.session_state:
    st.info("Click **Run Full Analysis** to generate results.")
    st.stop()

# ---- 1. Summary Statistics ------------------------------------------------
with st.expander("Summary Statistics", expanded=True):
    stats = st.session_state['synthesis_summary']

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Events", format_number(stats['total_events']))
    col2.metric("Total Cases", format_number(stats['total_cases']))
    col3.metric("Total Activities", format_number(stats['total_activities']))
    col4.metric("Most Common Activity", stats.get('most_common_activity', 'N/A'))

    col5, col6, col7, col8 = st.columns(4)
    col5.metric("Avg Case Duration", format_duration(stats['avg_case_duration'].total_seconds() if hasattr(stats['avg_case_duration'], 'total_seconds') else stats['avg_case_duration']))
    col6.metric("Median Case Duration", format_duration(stats['median_case_duration'].total_seconds() if hasattr(stats['median_case_duration'], 'total_seconds') else stats['median_case_duration']))
    col7.metric("Avg Events/Case", f"{stats['avg_events_per_case']:.1f}")
    col8.metric("Date Range", f"{stats['start_date'].strftime('%Y-%m-%d')} to {stats['end_date'].strftime('%Y-%m-%d')}")

# ---- 2. Correlation Analysis ----------------------------------------------
with st.expander("Correlation Analysis"):
    corr_results = st.session_state['synthesis_correlations']

    # Numeric correlation matrix as heatmap
    if corr_results.get('numeric_correlations') is not None:
        st.subheader("Numeric Correlation Matrix")
        corr_matrix = corr_results['numeric_correlations']
        all_cols = corr_matrix.columns.tolist()
        selected_cols = st.multiselect(
            "Columns to include in correlation matrix",
            options=all_cols,
            default=all_cols,
            key="corr_columns"
        )
        if len(selected_cols) >= 2:
            filtered_matrix = corr_matrix.loc[selected_cols, selected_cols]
            fig = go.Figure(data=go.Heatmap(
                z=filtered_matrix.values,
                x=filtered_matrix.columns.tolist(),
                y=filtered_matrix.index.tolist(),
                colorscale='RdBu_r',
                zmin=-1, zmax=1,
                hovertemplate='%{x}<br>%{y}<br>r = %{z:.3f}<extra></extra>',
            ))
            fig.update_layout(title="Numeric Correlations", template='plotly_white')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Select at least 2 columns to display the correlation matrix.")
    else:
        st.info("Not enough numeric columns for correlation matrix.")

    # Activity duration stats as table
    if 'activity_duration_stats' in corr_results and corr_results['activity_duration_stats'] is not None:
        st.subheader("Activity Duration Statistics")
        act_dur = corr_results['activity_duration_stats']
        if isinstance(act_dur, pd.DataFrame):
            st.dataframe(act_dur, use_container_width=True)

    # Events-duration correlation value
    if corr_results.get('events_duration_correlation') is not None:
        st.subheader("Events vs Duration Correlation")
        corr_val = corr_results['events_duration_correlation']
        st.metric("Pearson Correlation (Events vs Duration)", f"{corr_val:.3f}")

# ---- 3. Data Quality ------------------------------------------------------
with st.expander("Data Quality"):
    quality = st.session_state['synthesis_quality']

    # Quality score
    q_col1, q_col2 = st.columns([1, 3])
    q_col1.metric("Quality Score", f"{quality['quality_score']:.1f} / 100")

    # Issues list
    if quality['issues']:
        q_col2.subheader("Issues Found")
        for issue in quality['issues']:
            q_col2.warning(issue)
    else:
        q_col2.success("No significant data quality issues detected.")

    # Missing values table
    if not quality['missing_values'].empty:
        st.subheader("Missing Values")
        st.dataframe(quality['missing_values'], use_container_width=True)

    # Timestamp issues
    ts = quality['timestamp_issues']
    if ts['negative_durations'] > 0 or ts['out_of_order'] > 0:
        st.subheader("Timestamp Issues")
        ts_col1, ts_col2 = st.columns(2)
        ts_col1.metric("Negative Durations", format_number(ts['negative_durations']))
        ts_col2.metric("Out-of-Order Events", format_number(ts['out_of_order']))

# ---- 4. Variant Distribution ----------------------------------------------
with st.expander("Variant Distribution"):
    variants_df = st.session_state.get('synthesis_variants')
    if variants_df is not None and not variants_df.empty:
        top_variants = variants_df.head(10).copy()
        # Truncate long variant labels for readability
        top_variants['variant_short'] = top_variants['variant'].str[:80] + top_variants['variant'].apply(
            lambda v: '...' if len(str(v)) > 80 else ''
        )
        fig = chart_builder.create_bar_chart(
            top_variants, 'variant_short', 'count',
            "Top 10 Variants by Case Count",
            x_label="Variant", y_label="Count",
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Variant statistics not available.")

# ---- 5. Activity Distribution ----------------------------------------------
with st.expander("Activity Distribution"):
    act_df = st.session_state['synthesis_activities']
    if not act_df.empty:
        fig = chart_builder.create_pie_chart(
            act_df, 'activity', 'count',
            "Activity Distribution",
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Activity distribution data not available.")
