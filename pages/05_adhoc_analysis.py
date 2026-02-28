"""
Ad-Hoc Analysis page for QuickMineLite.

Provides flexible analysis capabilities with configurable analysis types,
chart types, and result limits for exploring event log data interactively.
"""

import streamlit as st
import pandas as pd
from viz.charts import ChartBuilder
from core.helpers import format_duration, format_number

# ---------------------------------------------------------------------------
# Guard: require loaded data
# ---------------------------------------------------------------------------
if st.session_state.get('event_log_df') is None:
    st.info("No data loaded. Go to Import Data.")
    st.stop()

st.header("Ad-Hoc Analysis")

# ---------------------------------------------------------------------------
# Retrieve shared objects
# ---------------------------------------------------------------------------
duckdb_mgr = st.session_state['duckdb_mgr']
filtered_df = st.session_state['filtered_df']
chart_builder = ChartBuilder()

# ---------------------------------------------------------------------------
# Controls
# ---------------------------------------------------------------------------
col_ctrl1, col_ctrl2, col_ctrl3 = st.columns(3)

with col_ctrl1:
    analysis_type = st.selectbox(
        "Analysis Type",
        ["Activity Frequency", "Case Duration", "Variant Analysis",
         "Activity Pairs (DFG)", "Custom Pivot"],
    )

with col_ctrl2:
    chart_type = st.selectbox(
        "Chart Type",
        ["Bar", "Pie", "Line", "Histogram", "Table"],
    )

with col_ctrl3:
    limit_option = st.selectbox("Limit", ["10", "20", "50", "100", "All"])

limit = None if limit_option == "All" else int(limit_option)

# ---------------------------------------------------------------------------
# Run the selected analysis
# ---------------------------------------------------------------------------
result_df: pd.DataFrame = pd.DataFrame()
title = ""

if analysis_type == "Activity Frequency":
    title = "Activity Frequency"
    result_df = duckdb_mgr.get_activity_distribution(limit=limit)

elif analysis_type == "Case Duration":
    title = "Case Duration"
    result_df = duckdb_mgr.get_case_durations()
    # Add formatted duration column for display
    result_df['duration_formatted'] = result_df['duration_seconds'].apply(format_duration)
    if limit is not None:
        result_df = result_df.sort_values('duration_seconds', ascending=False).head(limit)

elif analysis_type == "Variant Analysis":
    title = "Variant Analysis"
    result_df = duckdb_mgr.get_variant_statistics()
    if limit is not None:
        result_df = result_df.head(limit)

elif analysis_type == "Activity Pairs (DFG)":
    title = "Activity Pairs (Directly-Follows Graph)"
    result_df = duckdb_mgr.get_dfg_edges()
    # Create a readable pair label
    result_df['pair'] = result_df['source'] + ' -> ' + result_df['target']
    if limit is not None:
        result_df = result_df.head(limit)

elif analysis_type == "Custom Pivot":
    title = "Custom Pivot"
    available_columns = list(filtered_df.columns)

    col_p1, col_p2 = st.columns(2)
    with col_p1:
        row_col = st.selectbox("Group by (rows)", available_columns, index=0)
    with col_p2:
        # Default to a numeric-like aggregation column if possible
        numeric_cols = filtered_df.select_dtypes(include='number').columns.tolist()
        value_options = available_columns
        default_idx = 0
        if numeric_cols:
            default_idx = available_columns.index(numeric_cols[0]) if numeric_cols[0] in available_columns else 0
        value_col = st.selectbox("Value column", value_options, index=default_idx)

    agg_func = st.selectbox("Aggregation", ["count", "mean", "sum", "median", "min", "max"])

    try:
        if agg_func == "count":
            result_df = (
                filtered_df.groupby(row_col)[value_col]
                .count()
                .reset_index(name='count')
                .sort_values('count', ascending=False)
            )
        else:
            result_df = (
                filtered_df.groupby(row_col)[value_col]
                .agg(agg_func)
                .reset_index(name=agg_func)
                .sort_values(agg_func, ascending=False)
            )

        if limit is not None:
            result_df = result_df.head(limit)
    except Exception as e:
        st.error(f"Pivot computation failed: {e}")
        st.stop()

# ---------------------------------------------------------------------------
# Display results
# ---------------------------------------------------------------------------
if result_df.empty:
    st.warning("No results to display for the selected analysis.")
    st.stop()

st.subheader(title)

if chart_type == "Table":
    st.dataframe(result_df, use_container_width=True)

elif chart_type == "Bar":
    # Determine x/y columns heuristically
    cols = result_df.columns.tolist()
    x_col = cols[0]
    y_col = cols[1] if len(cols) > 1 else cols[0]
    # Prefer a numeric column for y
    for c in cols[1:]:
        if pd.api.types.is_numeric_dtype(result_df[c]):
            y_col = c
            break
    fig = chart_builder.create_bar_chart(result_df, x_col, y_col, title)
    st.plotly_chart(fig, use_container_width=True)

elif chart_type == "Pie":
    cols = result_df.columns.tolist()
    label_col = cols[0]
    value_col_chart = cols[1] if len(cols) > 1 else cols[0]
    for c in cols[1:]:
        if pd.api.types.is_numeric_dtype(result_df[c]):
            value_col_chart = c
            break
    fig = chart_builder.create_pie_chart(result_df, label_col, value_col_chart, title)
    st.plotly_chart(fig, use_container_width=True)

elif chart_type == "Line":
    cols = result_df.columns.tolist()
    x_col = cols[0]
    y_col = cols[1] if len(cols) > 1 else cols[0]
    for c in cols[1:]:
        if pd.api.types.is_numeric_dtype(result_df[c]):
            y_col = c
            break
    fig = chart_builder.create_time_series(result_df, x_col, [y_col], title)
    st.plotly_chart(fig, use_container_width=True)

elif chart_type == "Histogram":
    # Pick the first numeric column
    numeric_series = None
    for c in result_df.columns:
        if pd.api.types.is_numeric_dtype(result_df[c]):
            numeric_series = result_df[c]
            break
    if numeric_series is not None:
        fig = chart_builder.create_histogram(numeric_series, title, numeric_series.name)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No numeric column available for histogram. Showing as table instead.")
        st.dataframe(result_df, use_container_width=True)

# ---------------------------------------------------------------------------
# Download button
# ---------------------------------------------------------------------------
st.divider()
csv_data = result_df.to_csv(index=False).encode('utf-8')
st.download_button(
    label="Download results as CSV",
    data=csv_data,
    file_name=f"adhoc_{analysis_type.lower().replace(' ', '_')}.csv",
    mime="text/csv",
)
