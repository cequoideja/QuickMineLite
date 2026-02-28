"""
Attribute Changes page for QuickMineLite.

Tracks how non-standard attribute values change across events within cases,
builds a transition matrix, and displays it as a heatmap along with
supporting statistics and value distributions.
"""

import streamlit as st
import pandas as pd
from viz.charts import ChartBuilder
from core.helpers import format_number

# ---------------------------------------------------------------------------
# Guard: require loaded data
# ---------------------------------------------------------------------------
if st.session_state.get('event_log_df') is None:
    st.info("No data loaded. Go to Import Data.")
    st.stop()

st.header("Attribute Changes")

# ---------------------------------------------------------------------------
# Retrieve shared objects
# ---------------------------------------------------------------------------
filtered_df: pd.DataFrame = st.session_state['filtered_df']
chart_builder = ChartBuilder()

# ---------------------------------------------------------------------------
# Identify non-standard columns
# ---------------------------------------------------------------------------
STANDARD_COLS = {'case:concept:name', 'concept:name', 'time:timestamp'}
non_standard_cols = [c for c in filtered_df.columns if c not in STANDARD_COLS]

if not non_standard_cols:
    st.info("No additional attribute columns found in the dataset beyond the standard process mining columns.")
    st.stop()

# ---------------------------------------------------------------------------
# Attribute selector
# ---------------------------------------------------------------------------
selected_attr = st.selectbox("Select an attribute column to analyze", non_standard_cols)

if selected_attr:
    st.subheader(f"Attribute: {selected_attr}")

    # ------------------------------------------------------------------
    # Build transition data
    # ------------------------------------------------------------------
    df_sorted = filtered_df.sort_values(['case:concept:name', 'time:timestamp']).copy()

    # Convert attribute values to string for consistent handling
    df_sorted['_attr_val'] = df_sorted[selected_attr].astype(str)

    # Previous value within each case
    df_sorted['_prev_val'] = df_sorted.groupby('case:concept:name')['_attr_val'].shift(1)

    # Keep only rows where we have a transition (not first event in case)
    transitions = df_sorted.dropna(subset=['_prev_val']).copy()

    # Filter to actual changes (exclude same-value "transitions" for the matrix)
    changes = transitions[transitions['_prev_val'] != transitions['_attr_val']]

    # ------------------------------------------------------------------
    # Summary statistics
    # ------------------------------------------------------------------
    unique_values = df_sorted['_attr_val'].nunique()
    total_transitions = len(transitions)
    total_changes = len(changes)

    st.markdown("#### Statistics")
    stat_c1, stat_c2, stat_c3 = st.columns(3)
    stat_c1.metric("Unique Values", format_number(unique_values))
    stat_c2.metric("Total Transitions", format_number(total_transitions))
    stat_c3.metric("Actual Value Changes", format_number(total_changes))

    if total_changes == 0:
        st.info("This attribute does not change within any case. There are no transitions to display.")
    else:
        # ------------------------------------------------------------------
        # Transition matrix (counts of from -> to, including same-value)
        # ------------------------------------------------------------------
        st.markdown("#### Transition Matrix")
        trans_counts = (
            transitions.groupby(['_prev_val', '_attr_val'])
            .size()
            .reset_index(name='count')
        )
        trans_counts.columns = ['from_value', 'to_value', 'count']

        # Show as heatmap
        try:
            fig = chart_builder.create_heatmap(
                trans_counts, 'to_value', 'from_value', 'count',
                f"Transition Counts for '{selected_attr}'",
            )
            st.plotly_chart(fig, use_container_width=True)
        except Exception:
            # Fallback to table if heatmap pivot fails (e.g. duplicate entries)
            pivot_table = trans_counts.pivot_table(
                index='from_value', columns='to_value', values='count', fill_value=0
            )
            st.dataframe(pivot_table, use_container_width=True)

        # ------------------------------------------------------------------
        # Most common transitions
        # ------------------------------------------------------------------
        st.markdown("#### Most Common Transitions")
        top_changes = (
            changes.groupby(['_prev_val', '_attr_val'])
            .size()
            .reset_index(name='count')
            .sort_values('count', ascending=False)
            .head(15)
        )
        top_changes.columns = ['From', 'To', 'Count']
        st.dataframe(top_changes, use_container_width=True)

    # ------------------------------------------------------------------
    # Value distribution
    # ------------------------------------------------------------------
    st.markdown("#### Value Distribution")
    value_dist = (
        df_sorted['_attr_val']
        .value_counts()
        .reset_index()
    )
    value_dist.columns = ['value', 'count']

    fig = chart_builder.create_bar_chart(
        value_dist.head(20), 'value', 'count',
        f"Value Distribution for '{selected_attr}'",
        x_label="Value", y_label="Count",
    )
    st.plotly_chart(fig, use_container_width=True)
