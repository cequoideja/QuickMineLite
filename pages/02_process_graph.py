"""
Process Graph page -- DFG, BPMN, Petri Net, and Process Tree visualizations.
Interactive pan/zoom/fit via embedded SVG + JavaScript.
Export: PNG for all graph types, .bpmn XML for BPMN.
"""
import streamlit as st
import streamlit.components.v1 as components

# ── Guard: data must be loaded ───────────────────────────────────────────────
if st.session_state.get("event_log_df") is None:
    st.info("No event log loaded. Please import data first.")
    st.stop()

analyzer = st.session_state["analyzer"]

st.header("Process Graph")

GRAPH_HEIGHT = 650

# ── Tabs for each model type ─────────────────────────────────────────────────
tab_dfg, tab_bpmn, tab_petri, tab_tree = st.tabs(
    ["DFG", "BPMN", "Petri Net", "Process Tree"]
)

# ── DFG tab ──────────────────────────────────────────────────────────────────
with tab_dfg:
    view_type = st.radio(
        "View type",
        ["Frequency", "Performance"],
        horizontal=True,
    )
    coverage_pct = st.slider(
        "Path coverage %",
        min_value=0,
        max_value=100,
        value=100,
        step=5,
        help="Percentage of paths to keep. 100% = all paths, lower values = only the most frequent paths (simplified view).",
    )

    with st.spinner("Discovering DFG..."):
        dfg, start_activities, end_activities = analyzer.discover_dfg()

    # Apply frequency filter: keep paths that cover the given percentage
    if coverage_pct < 100:
        dfg, start_activities, end_activities = analyzer.filter_dfg_by_frequency(
            dfg, start_activities, end_activities, coverage_pct / 100.0
        )

    if not dfg:
        st.warning("No edges remain after filtering. Try lowering the threshold.")
    else:
        try:
            from viz.process_maps import render_dfg_interactive, render_dfg

            performance_map = None
            if view_type == "Performance":
                import pandas as pd

                df = analyzer.df.copy()
                df = df.sort_values(["case:concept:name", "time:timestamp"])
                df["next_activity"] = df.groupby("case:concept:name")["concept:name"].shift(-1)
                df["next_timestamp"] = df.groupby("case:concept:name")["time:timestamp"].shift(-1)
                df["duration"] = (df["next_timestamp"] - df["time:timestamp"]).dt.total_seconds()
                trans = df.dropna(subset=["next_activity"])
                perf = (
                    trans.groupby(["concept:name", "next_activity"])["duration"]
                    .mean()
                    .to_dict()
                )
                performance_map = {k: v for k, v in perf.items() if k in dfg}

            html = render_dfg_interactive(
                dfg,
                start_activities,
                end_activities,
                performance=performance_map,
                height=GRAPH_HEIGHT,
            )
            components.html(html, height=GRAPH_HEIGHT + 10, scrolling=False)

            # Export PNG
            png_bytes = render_dfg(dfg, start_activities, end_activities,
                                   performance=performance_map)
            st.download_button(
                "Export PNG",
                data=png_bytes,
                file_name=f"dfg_{view_type.lower()}.png",
                mime="image/png",
            )
        except Exception as e:
            st.error(f"Error rendering DFG: {e}")

        # Edge statistics table
        st.subheader("DFG Edge Statistics")
        dfg_stats = analyzer.get_dfg_statistics(dfg)
        st.dataframe(dfg_stats, use_container_width=True)

# ── BPMN tab ─────────────────────────────────────────────────────────────────
with tab_bpmn:
    try:
        from viz.process_maps import render_bpmn_interactive, render_bpmn, export_bpmn_xml

        with st.spinner("Discovering BPMN model..."):
            html = render_bpmn_interactive(analyzer.log, height=GRAPH_HEIGHT)
        components.html(html, height=GRAPH_HEIGHT + 10, scrolling=False)

        # Export buttons
        col_png, col_bpmn = st.columns(2)
        with col_png:
            png_bytes = render_bpmn(analyzer.log)
            st.download_button(
                "Export PNG",
                data=png_bytes,
                file_name="bpmn_model.png",
                mime="image/png",
            )
        with col_bpmn:
            bpmn_xml = export_bpmn_xml(analyzer.log)
            st.download_button(
                "Export .bpmn",
                data=bpmn_xml,
                file_name="process_model.bpmn",
                mime="application/xml",
            )
    except ImportError:
        st.warning(
            "BPMN visualization requires the pm4py package. "
            "Install it with: pip install pm4py"
        )
    except Exception as e:
        st.exception(e)

# ── Petri Net tab ────────────────────────────────────────────────────────────
with tab_petri:
    try:
        from viz.process_maps import render_petri_net_interactive, render_petri_net

        with st.spinner("Discovering Petri net..."):
            html = render_petri_net_interactive(analyzer.log, height=GRAPH_HEIGHT)
        components.html(html, height=GRAPH_HEIGHT + 10, scrolling=False)

        png_bytes = render_petri_net(analyzer.log)
        st.download_button(
            "Export PNG",
            data=png_bytes,
            file_name="petri_net.png",
            mime="image/png",
        )
    except ImportError:
        st.warning(
            "Petri net visualization requires the pm4py package. "
            "Install it with: pip install pm4py"
        )
    except Exception as e:
        st.exception(e)

# ── Process Tree tab ─────────────────────────────────────────────────────────
with tab_tree:
    try:
        from viz.process_maps import render_process_tree_interactive, render_process_tree

        with st.spinner("Discovering process tree..."):
            html = render_process_tree_interactive(analyzer.log, height=GRAPH_HEIGHT)
        components.html(html, height=GRAPH_HEIGHT + 10, scrolling=False)

        png_bytes = render_process_tree(analyzer.log)
        st.download_button(
            "Export PNG",
            data=png_bytes,
            file_name="process_tree.png",
            mime="image/png",
        )
    except ImportError:
        st.warning(
            "Process tree visualization requires the pm4py package. "
            "Install it with: pip install pm4py"
        )
    except Exception as e:
        st.exception(e)
