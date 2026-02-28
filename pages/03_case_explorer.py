"""
Case Explorer page -- browse cases, inspect individual case events, and view
Gantt-style timeline charts.
"""
import streamlit as st
from viz.gantt import create_gantt_chart

# ── Guard: data must be loaded ───────────────────────────────────────────────
if st.session_state.get("event_log_df") is None:
    st.info("No event log loaded. Please import data first.")
    st.stop()

duckdb_mgr = st.session_state["duckdb_mgr"]

st.header("Case Explorer")

# ── Case list ────────────────────────────────────────────────────────────────
case_list_df = duckdb_mgr.get_case_list()

st.subheader("Case List")
st.dataframe(case_list_df, use_container_width=True)

# Download button for the full case list
csv_data = case_list_df.to_csv(index=False).encode("utf-8")
st.download_button(
    label="Download case list as CSV",
    data=csv_data,
    file_name="case_list.csv",
    mime="text/csv",
)

# ── Case selection ───────────────────────────────────────────────────────────
st.subheader("Case Detail")

case_ids = case_list_df["case_id"].astype(str).tolist()

search_term = st.text_input("Search case ID", placeholder="Type to filter...")
if search_term:
    filtered_ids = [cid for cid in case_ids if search_term.lower() in cid.lower()]
else:
    filtered_ids = case_ids

if not filtered_ids:
    st.warning("No cases match the search term.")
    st.stop()

selected_case = st.selectbox(
    "Select a case",
    options=filtered_ids,
    index=0,
)

# ── Case events table ────────────────────────────────────────────────────────
if selected_case:
    case_events = duckdb_mgr.get_case_events(selected_case)

    if case_events.empty:
        st.warning(f"No events found for case '{selected_case}'.")
    else:
        st.write(f"**Events for case:** {selected_case} ({len(case_events)} events)")
        st.dataframe(case_events, use_container_width=True)

        # ── Gantt chart ──────────────────────────────────────────────────────
        st.subheader("Case Timeline")
        fig = create_gantt_chart(
            case_events,
            title=f"Timeline for Case {selected_case}",
        )
        st.plotly_chart(fig, use_container_width=True)
