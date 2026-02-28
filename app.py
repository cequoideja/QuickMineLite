"""
QuickMine Lite - Main Streamlit Entry Point

Process mining application built with Streamlit + DuckDB + PM4Py + Plotly.
Refactored from the original PyQt6 desktop application.
"""
import streamlit as st
import pandas as pd
from datetime import datetime

from core.config import Config
from core.duckdb_manager import DuckDBManager
from core.filter_engine import FilterManager, FilterStrategyFactory
from core.helpers import format_number
from analysis.process_analyzer import ProcessAnalyzer

# ---------------------------------------------------------------------------
# 1. Page Configuration
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="QuickMine Lite",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# 2. Operator Labels & Categories
# ---------------------------------------------------------------------------
OPERATOR_LABELS = {
    'equals': 'Equals (=)',
    'not_equals': 'Not Equals (\u2260)',
    'contains': 'Contains',
    'not_contains': 'Not Contains',
    'in': 'In List',
    'not_in': 'Not In List',
    'greater_than': 'Greater Than (>)',
    'less_than': 'Less Than (<)',
    'greater_equal': 'Greater or Equal (\u2265)',
    'less_equal': 'Less or Equal (\u2264)',
    'between': 'Between',
    'is_null': 'Is Null / Empty',
    'not_null': 'Is Not Null',
    'starts_with': 'Starts With',
    'ends_with': 'Ends With',
    'regex': 'Regex Match',
}

NO_VALUE_OPS = {'is_null', 'not_null'}
LIST_VALUE_OPS = {'in', 'not_in'}
RANGE_VALUE_OPS = {'between'}

# ---------------------------------------------------------------------------
# 3. Session State Initialization
# ---------------------------------------------------------------------------
_defaults = {
    "event_log_df": None,
    "filtered_df": None,
    "metadata": {},
    "duckdb_mgr": None,
    "analyzer": None,
    "sampling_config": {
        "auto_sample": False,
        "method": "stratified",
        "max_cases": 50_000,
    },
    "is_sampled": False,
    # Filter state
    "custom_filters": [],       # list of dicts: {column, operator, value, type, enabled}
    "filters_applied": False,
    "filter_add_counter": 0,    # incremented after each "Add Filter" to reset form widgets
}

for key, default in _defaults.items():
    if key not in st.session_state:
        st.session_state[key] = default

# ---------------------------------------------------------------------------
# 4. Helper Functions
# ---------------------------------------------------------------------------

def _coerce_value(val_str, df: pd.DataFrame, column: str):
    """Try to coerce a string value to the column's dtype (int/float)."""
    if not val_str:
        return val_str
    if column in df.columns and pd.api.types.is_numeric_dtype(df[column]):
        try:
            return float(val_str) if '.' in str(val_str) else int(val_str)
        except (ValueError, TypeError):
            return val_str
    return val_str


def _apply_filters(df_orig: pd.DataFrame):
    """Build a FilterManager from current sidebar state and apply all filters."""
    fm = FilterManager()
    fm.set_data(df_orig)

    # -- Quick filter: activities --
    selected_activities = st.session_state.get("sb_activities", [])
    if selected_activities:
        fm.add_event_filter("concept:name", "in", selected_activities)

    # -- Quick filter: date range --
    start_date = st.session_state.get("sb_start_date")
    end_date = st.session_state.get("sb_end_date")
    if start_date and end_date:
        ts_col = df_orig["time:timestamp"]
        data_min = ts_col.min().date() if pd.notna(ts_col.min()) else None
        data_max = ts_col.max().date() if pd.notna(ts_col.max()) else None
        # Only apply if the user changed the dates from the full range
        if start_date != data_min or end_date != data_max:
            start_dt = pd.Timestamp(datetime.combine(start_date, datetime.min.time()))
            end_dt = pd.Timestamp(datetime.combine(end_date, datetime.max.time()))
            fm.set_time_filter(start_time=start_dt, end_time=end_dt)

    # -- Custom filters --
    for f in st.session_state.get("custom_filters", []):
        if not f.get("enabled", True):
            continue
        if f["type"] == "event":
            fm.add_event_filter(f["column"], f["operator"], f["value"])
        else:
            fm.add_case_filter(f["column"], f["operator"], f["value"])

    filtered_df = fm.get_filtered_data()
    st.session_state.filtered_df = filtered_df

    # Determine if any filter is actually active
    has_custom = any(f.get("enabled", True) for f in st.session_state.get("custom_filters", []))
    has_activity = bool(selected_activities)
    has_date = (start_date != (ts_col.min().date() if pd.notna(ts_col.min()) else None)
                or end_date != (ts_col.max().date() if pd.notna(ts_col.max()) else None))
    st.session_state.filters_applied = has_custom or has_activity or has_date

    # Re-register filtered data in DuckDB
    if st.session_state.duckdb_mgr is not None:
        st.session_state.duckdb_mgr.load_dataframe(filtered_df)

    # Rebuild ProcessAnalyzer on filtered data
    sampling_cfg = st.session_state.sampling_config
    st.session_state.analyzer = ProcessAnalyzer(
        filtered_df,
        auto_sample=sampling_cfg.get("auto_sample", False),
        sampling_config=sampling_cfg,
    )


def _clear_filters(df_orig: pd.DataFrame):
    """Clear all filters and restore original data."""
    st.session_state.custom_filters = []
    st.session_state.filters_applied = False
    st.session_state.filter_add_counter = 0
    st.session_state.filtered_df = df_orig.copy()

    # Reset quick-filter widget values
    st.session_state.sb_activities = []
    ts_col = df_orig["time:timestamp"]
    if pd.notna(ts_col.min()):
        st.session_state.sb_start_date = ts_col.min().date()
    if pd.notna(ts_col.max()):
        st.session_state.sb_end_date = ts_col.max().date()

    # Re-register original data in DuckDB
    if st.session_state.duckdb_mgr is not None:
        st.session_state.duckdb_mgr.load_dataframe(df_orig)

    # Rebuild ProcessAnalyzer with full data
    sampling_cfg = st.session_state.sampling_config
    st.session_state.analyzer = ProcessAnalyzer(
        df_orig,
        auto_sample=sampling_cfg.get("auto_sample", False),
        sampling_config=sampling_cfg,
    )


# ---------------------------------------------------------------------------
# 5. Multipage Navigation
# ---------------------------------------------------------------------------
pages = {
    "Data": [
        st.Page("pages/data_import.py", title="Import Data", icon=":material/upload_file:"),
    ],
    "Analysis": [
        st.Page("pages/01_dashboard.py", title="Dashboard", icon=":material/dashboard:"),
        st.Page("pages/02_process_graph.py", title="Process Graph", icon=":material/account_tree:"),
        st.Page("pages/03_case_explorer.py", title="Case Explorer", icon=":material/search:"),
        st.Page("pages/04_event_log.py", title="Event Log", icon=":material/table_view:"),
        st.Page("pages/05_adhoc_analysis.py", title="Ad-Hoc Analysis", icon=":material/query_stats:"),
        st.Page("pages/06_synthesis.py", title="Synthesis", icon=":material/summarize:"),
        st.Page("pages/07_attribute_changes.py", title="Attribute Changes", icon=":material/swap_horiz:"),
        st.Page("pages/08_bottleneck.py", title="Bottleneck Analysis", icon=":material/speed:"),
        st.Page("pages/09_variants.py", title="Variant Analysis", icon=":material/alt_route:"),
        st.Page("pages/10_ml_predictions.py", title="ML Predictions", icon=":material/psychology:"),
    ],
}

pg = st.navigation(pages)

# ---------------------------------------------------------------------------
# 6. Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.title(Config.APP_NAME)
    st.caption(f"v{Config.APP_VERSION}")

    # ======================================================================
    # Only show dataset info & filters when data is loaded
    # ======================================================================
    if st.session_state.event_log_df is not None:
        df_orig: pd.DataFrame = st.session_state.event_log_df

        # -- Dataset Info (compact) ----------------------------------------
        st.divider()
        st.subheader("Dataset")
        file_name = st.session_state.metadata.get("file_name", "Unknown")
        st.markdown(f"**{file_name}**")

        activities_list = sorted(df_orig["concept:name"].dropna().unique().tolist())
        filt_df = st.session_state.filtered_df
        is_filtered = (
            st.session_state.filters_applied
            and filt_df is not None
            and len(filt_df) != len(df_orig)
        )

        if is_filtered:
            orig_events = len(df_orig)
            filt_events = len(filt_df)
            orig_cases = df_orig["case:concept:name"].nunique()
            filt_cases = filt_df["case:concept:name"].nunique()
            ic1, ic2, ic3 = st.columns(3)
            ic1.metric("Events", format_number(filt_events),
                        delta=f"{filt_events - orig_events:,}", delta_color="off")
            ic2.metric("Cases", format_number(filt_cases),
                        delta=f"{filt_cases - orig_cases:,}", delta_color="off")
            ic3.metric("Activities", format_number(
                filt_df["concept:name"].nunique()))
        else:
            ic1, ic2, ic3 = st.columns(3)
            ic1.metric("Events", format_number(len(df_orig)))
            ic2.metric("Cases", format_number(df_orig["case:concept:name"].nunique()))
            ic3.metric("Activities", format_number(len(activities_list)))

        if st.session_state.is_sampled:
            st.info("Sampled subset", icon=":material/info:")

        # ==================================================================
        # FILTERS SECTION
        # ==================================================================
        st.divider()
        st.subheader("Filters")

        # -- Quick filter: Activities --------------------------------------
        st.multiselect(
            "Activities",
            options=activities_list,
            key="sb_activities",
        )

        # -- Quick filter: Date range --------------------------------------
        ts_col = df_orig["time:timestamp"]
        min_date = ts_col.min().date() if pd.notna(ts_col.min()) else None
        max_date = ts_col.max().date() if pd.notna(ts_col.max()) else None

        dc1, dc2 = st.columns(2)
        with dc1:
            st.date_input(
                "Start",
                value=min_date,
                min_value=min_date,
                max_value=max_date,
                key="sb_start_date",
            )
        with dc2:
            st.date_input(
                "End",
                value=max_date,
                min_value=min_date,
                max_value=max_date,
                key="sb_end_date",
            )

        # -- Advanced / Custom Filters (expander) --------------------------
        has_custom = bool(st.session_state.custom_filters)
        with st.expander(
            f"Custom Filters ({len(st.session_state.custom_filters)})"
            if has_custom else "Custom Filters",
            expanded=has_custom,
        ):
            # Retrieve column classification (computed at import time)
            col_class = st.session_state.get("column_classification", {})
            case_cols_set = set(col_class.get("case_columns", []))
            event_cols_set = set(col_class.get("event_columns", []))

            # Columns available for filtering (all except timestamp)
            filter_cols = sorted(
                c for c in df_orig.columns if c != "time:timestamp"
            )

            add_ctr = st.session_state.filter_add_counter

            # Column selector -- shows [E] / [C] tag per column
            def _col_label(col_name):
                if col_name in case_cols_set:
                    return f"{col_name}  [Case]"
                if col_name in event_cols_set:
                    return f"{col_name}  [Event]"
                # Standard columns (concept:name, case:concept:name)
                return col_name

            filter_col = st.selectbox(
                "Column",
                options=filter_cols,
                format_func=_col_label,
                key=f"sb_fcol_{add_ctr}",
            )

            # Auto-detect level based on classification
            filter_type = "Case" if filter_col in case_cols_set else "Event"

            # Operator selector
            all_operators = list(OPERATOR_LABELS.keys())
            filter_op = st.selectbox(
                "Operator",
                options=all_operators,
                format_func=lambda x: OPERATOR_LABELS[x],
                key=f"sb_fop_{add_ctr}",
            )

            # Value input -- adapts to the selected operator
            filter_val = None
            if filter_op in NO_VALUE_OPS:
                st.caption("_No value needed for this operator._")

            elif filter_op in LIST_VALUE_OPS:
                try:
                    unique_vals = sorted(
                        df_orig[filter_col].dropna().unique().tolist(), key=str
                    )
                except TypeError:
                    unique_vals = df_orig[filter_col].dropna().unique().tolist()
                filter_val = st.multiselect(
                    "Values",
                    options=unique_vals,
                    key=f"sb_fval_list_{add_ctr}",
                )

            elif filter_op in RANGE_VALUE_OPS:
                rc1, rc2 = st.columns(2)
                v_min = rc1.text_input("Min", key=f"sb_fval_min_{add_ctr}")
                v_max = rc2.text_input("Max", key=f"sb_fval_max_{add_ctr}")
                filter_val = (v_min, v_max)

            else:
                filter_val = st.text_input(
                    "Value",
                    key=f"sb_fval_{add_ctr}",
                )

            # "Add Filter" button
            if st.button(
                "Add Filter",
                key=f"sb_fadd_{add_ctr}",
                use_container_width=True,
            ):
                # Parse / coerce the value
                if filter_op in NO_VALUE_OPS:
                    parsed = None
                elif filter_op in RANGE_VALUE_OPS:
                    parsed = [
                        _coerce_value(filter_val[0], df_orig, filter_col),
                        _coerce_value(filter_val[1], df_orig, filter_col),
                    ]
                elif filter_op in LIST_VALUE_OPS:
                    parsed = filter_val  # already a list from multiselect
                else:
                    parsed = _coerce_value(filter_val, df_orig, filter_col)

                st.session_state.custom_filters.append(
                    {
                        "column": filter_col,
                        "operator": filter_op,
                        "value": parsed,
                        "type": filter_type.lower(),
                        "enabled": True,
                    }
                )
                # Increment counter to reset the form widgets on rerun
                st.session_state.filter_add_counter = add_ctr + 1
                st.rerun()

        # -- Display active custom filters ---------------------------------
        custom_filters = st.session_state.custom_filters
        if custom_filters:
            st.caption(f"**{len(custom_filters)} custom filter(s):**")
            to_remove = []
            for i, f in enumerate(custom_filters):
                fc1, fc2 = st.columns([5, 1])
                op_label = OPERATOR_LABELS.get(f["operator"], f["operator"])
                val_display = ""
                if f["value"] is not None:
                    val_str = str(f["value"])
                    if len(val_str) > 30:
                        val_str = val_str[:27] + "..."
                    val_display = f" `{val_str}`"
                level_icon = "E" if f["type"] == "event" else "C"
                fc1.caption(
                    f"[{level_icon}] **{f['column']}** {op_label}{val_display}"
                )
                if fc2.button("\u2716", key=f"rmf_{i}"):
                    to_remove.append(i)

            # Process removals (reverse order to keep indices valid)
            if to_remove:
                for idx in sorted(to_remove, reverse=True):
                    st.session_state.custom_filters.pop(idx)
                st.rerun()

        # -- Apply / Clear buttons -----------------------------------------
        bc1, bc2 = st.columns(2)
        if bc1.button("Apply", type="primary", use_container_width=True):
            _apply_filters(df_orig)
            st.rerun()

        if bc2.button("Clear All", use_container_width=True):
            _clear_filters(df_orig)
            st.rerun()

        # -- Filter summary ------------------------------------------------
        if is_filtered:
            ev_pct = (len(filt_df) / len(df_orig) * 100) if len(df_orig) > 0 else 100
            ca_orig = df_orig["case:concept:name"].nunique()
            ca_filt = filt_df["case:concept:name"].nunique()
            ca_pct = (ca_filt / ca_orig * 100) if ca_orig > 0 else 100
            st.caption(
                f"**{format_number(len(filt_df))}** / {format_number(len(df_orig))} "
                f"events ({ev_pct:.1f}%)  \n"
                f"**{format_number(ca_filt)}** / {format_number(ca_orig)} "
                f"cases ({ca_pct:.1f}%)"
            )

# ---------------------------------------------------------------------------
# 7. Run the Selected Page
# ---------------------------------------------------------------------------
pg.run()
