"""
Event Log viewer page -- browse, search, and download event-level data.
"""
import streamlit as st

# ── Guard: data must be loaded ───────────────────────────────────────────────
if st.session_state.get("event_log_df") is None:
    st.info("No event log loaded. Please import data first.")
    st.stop()

duckdb_mgr = st.session_state["duckdb_mgr"]

st.header("Event Log")

# ── Controls ─────────────────────────────────────────────────────────────────
all_columns = duckdb_mgr.get_columns()

selected_columns = st.multiselect(
    "Columns",
    options=all_columns,
    default=all_columns,
)

col1, col2 = st.columns([1, 2])

with col1:
    row_limit = st.number_input(
        "Rows to display",
        min_value=10,
        max_value=10000,
        value=100,
        step=100,
    )

with col2:
    search_text = st.text_input("Search", placeholder="Filter rows by text...")

# ── Fetch data ───────────────────────────────────────────────────────────────
if not selected_columns:
    st.warning("Please select at least one column.")
    st.stop()

df = duckdb_mgr.get_paginated_events(
    offset=0,
    limit=row_limit,
    columns=selected_columns,
)

# ── Apply search filter ─────────────────────────────────────────────────────
if search_text:
    mask = df.apply(
        lambda row: row.astype(str).str.contains(search_text, case=False, na=False).any(),
        axis=1,
    )
    df = df[mask]

# ── Display ──────────────────────────────────────────────────────────────────
total_events = duckdb_mgr.get_total_event_count()
st.caption(f"Showing {len(df):,} of {total_events:,} total events")

st.dataframe(df, use_container_width=True)

# ── Download ─────────────────────────────────────────────────────────────────
csv_bytes = df.to_csv(index=False).encode("utf-8")
st.download_button(
    label="Download filtered events as CSV",
    data=csv_bytes,
    file_name="event_log_filtered.csv",
    mime="text/csv",
)
