"""
Data Import page -- upload a CSV event log, map columns, and load into session state.
"""
import streamlit as st
import pandas as pd
from core.data_loader import EventLogLoader
from core.duckdb_manager import DuckDBManager
from core.helpers import classify_columns
from analysis.process_analyzer import ProcessAnalyzer

st.header("Import Event Log")

# ── File uploader ────────────────────────────────────────────────────────────
uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])

if uploaded_file is None:
    st.info("Please upload a CSV file to get started.")
    st.stop()

# ── Load file ────────────────────────────────────────────────────────────────
loader = EventLogLoader()
success, message = loader.load_from_uploaded_file(uploaded_file)

if not success:
    st.error(message)
    st.stop()

st.success(message)

# ── Preview ──────────────────────────────────────────────────────────────────
st.subheader("Data Preview")
st.dataframe(loader.get_preview(10), use_container_width=True)

# ── Column mapping ───────────────────────────────────────────────────────────
st.subheader("Column Mapping")
detected = loader.detect_column_mapping()
columns = loader.get_columns()

col1, col2, col3 = st.columns(3)

with col1:
    case_id_col = st.selectbox(
        "Case ID column",
        options=columns,
        index=columns.index(detected["case_id"]) if detected["case_id"] in columns else 0,
    )

with col2:
    activity_col = st.selectbox(
        "Activity column",
        options=columns,
        index=columns.index(detected["activity"]) if detected["activity"] in columns else 0,
    )

with col3:
    timestamp_col = st.selectbox(
        "Timestamp column",
        options=columns,
        index=columns.index(detected["timestamp"]) if detected["timestamp"] in columns else 0,
    )

# Optional resource column
resource_options = ["(None)"] + columns
detected_resource_idx = (
    resource_options.index(detected["resource"])
    if detected["resource"] in resource_options
    else 0
)
resource_col = st.selectbox(
    "Resource column (optional)",
    options=resource_options,
    index=detected_resource_idx,
)
resource_col_value = None if resource_col == "(None)" else resource_col

# ── Sampling configuration ───────────────────────────────────────────────────
with st.expander("Sampling Configuration (optional)"):
    auto_sample = st.checkbox("Enable automatic sampling for large datasets", value=False)
    sampling_method = st.selectbox(
        "Sampling method",
        options=["stratified", "simple", "systematic"],
        index=0,
    )
    max_cases = st.number_input(
        "Maximum number of cases",
        min_value=100,
        max_value=1_000_000,
        value=50_000,
        step=1000,
    )

# ── Import button ────────────────────────────────────────────────────────────
if st.button("Import", type="primary"):
    # 1. Set column mapping & validate
    ok, msg = loader.set_column_mapping(case_id_col, activity_col, timestamp_col, resource_col_value)
    if not ok:
        st.error(msg)
        st.stop()

    # 2. Prepare event log
    with st.spinner("Preparing event log..."):
        event_log_df = loader.prepare_event_log()

    # 3. Create DuckDBManager and load data
    with st.spinner("Loading data into analytics engine..."):
        duckdb_mgr = DuckDBManager()
        duckdb_mgr.load_dataframe(event_log_df)

    # 4. Create ProcessAnalyzer (with sampling config if enabled)
    with st.spinner("Initializing process analyzer..."):
        sampling_config = None
        if auto_sample:
            sampling_config = {
                "method": sampling_method,
                "max_cases": max_cases,
            }
        analyzer = ProcessAnalyzer(
            event_log_df,
            auto_sample=auto_sample,
            sampling_config=sampling_config,
        )

    # 5. Detect column classification (case vs event level)
    with st.spinner("Detecting column types (case vs event)..."):
        col_classification = classify_columns(event_log_df)

    # 6. Build metadata
    metadata = loader.get_metadata()

    # 7. Store everything in session state
    st.session_state["event_log_df"] = event_log_df
    st.session_state["filtered_df"] = event_log_df
    st.session_state["duckdb_mgr"] = duckdb_mgr
    st.session_state["analyzer"] = analyzer
    st.session_state["metadata"] = metadata
    st.session_state["column_classification"] = col_classification

    # Reset filter state on new import
    st.session_state["custom_filters"] = []
    st.session_state["filters_applied"] = False
    st.session_state["filter_add_counter"] = 0

    # 8. Show sampling info if it was applied
    if analyzer.is_sampled and analyzer.sample_info:
        info = analyzer.sample_info
        st.info(
            f"Sampling applied: {info.get('sampled_cases', '?'):,} cases "
            f"({info.get('sampled_events', '?'):,} events) "
            f"sampled from {info.get('original_cases', '?'):,} cases "
            f"using **{info.get('method', 'stratified')}** method."
        )

    st.success(
        f"Event log imported successfully! "
        f"{metadata.get('total_cases', 0):,} cases, "
        f"{metadata.get('total_events', 0):,} events, "
        f"{metadata.get('total_activities', 0):,} activities."
    )

    # 9. Display column classification
    st.subheader("Column Classification")
    st.caption(
        "Columns are auto-classified as **Case** (constant per case) "
        "or **Event** (varies within a case). This is used for filtering."
    )

    rows = []
    for col in col_classification['standard_columns']:
        friendly = {'case:concept:name': 'Case ID', 'concept:name': 'Activity',
                     'time:timestamp': 'Timestamp'}.get(col, col)
        rows.append({'Column': col, 'Role': friendly, 'Level': 'Standard'})
    for col in col_classification['event_columns']:
        dtype = str(event_log_df[col].dtype)
        n_unique = event_log_df[col].nunique()
        rows.append({'Column': col, 'Role': f'{dtype} ({n_unique:,} unique)', 'Level': 'Event'})
    for col in col_classification['case_columns']:
        dtype = str(event_log_df[col].dtype)
        n_unique = event_log_df[col].nunique()
        rows.append({'Column': col, 'Role': f'{dtype} ({n_unique:,} unique)', 'Level': 'Case'})

    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
