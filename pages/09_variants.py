"""
Variant Analysis page for QuickMineLite.

Provides a Cortado-style variant explorer with colored chevron blocks,
summary metrics, charts, a detail viewer, and CSV download.
"""

import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
from viz.charts import ChartBuilder, build_variant_explorer_html
from core.helpers import format_number

# ---------------------------------------------------------------------------
# Guard: require loaded data
# ---------------------------------------------------------------------------
if st.session_state.get('event_log_df') is None:
    st.info("No data loaded. Go to Import Data.")
    st.stop()

st.header("Variant Analysis")

# ---------------------------------------------------------------------------
# Retrieve shared objects
# ---------------------------------------------------------------------------
duckdb_mgr = st.session_state['duckdb_mgr']
chart_builder = ChartBuilder()

# ---------------------------------------------------------------------------
# Load variant statistics
# ---------------------------------------------------------------------------
with st.spinner("Computing variant statistics..."):
    variants_df = duckdb_mgr.get_variant_statistics()

if variants_df.empty:
    st.warning("No variant data available.")
    st.stop()

# Add cumulative percentage
variants_df = variants_df.reset_index(drop=True)
variants_df['cumulative_%'] = variants_df['percentage'].cumsum().round(2)

# ---------------------------------------------------------------------------
# Summary metrics
# ---------------------------------------------------------------------------
total_variants = len(variants_df)
top_variant_pct = variants_df.iloc[0]['percentage'] if total_variants > 0 else 0
top5_coverage = variants_df.head(5)['percentage'].sum() if total_variants >= 5 else variants_df['percentage'].sum()

col1, col2, col3 = st.columns(3)
col1.metric("Total Variants", format_number(total_variants))
col2.metric("Top Variant %", f"{top_variant_pct:.1f}%")
col3.metric("Top 5 Coverage", f"{top5_coverage:.1f}%")

# ---------------------------------------------------------------------------
# Cortado-style Variant Explorer
# ---------------------------------------------------------------------------
st.subheader("Variant Explorer")

if total_variants <= 5:
    max_display = total_variants
else:
    max_display = st.slider(
        "Number of variants to display",
        min_value=5,
        max_value=min(total_variants, 100),
        value=min(total_variants, 30),
        step=5,
    )

explorer_html, explorer_height = build_variant_explorer_html(variants_df, max_rows=max_display)
components.html(explorer_html, height=explorer_height, scrolling=False)

# ---------------------------------------------------------------------------
# Data table (collapsible)
# ---------------------------------------------------------------------------
with st.expander("Variant Data Table"):
    display_df = variants_df.copy()
    display_df.index = display_df.index + 1
    display_df.index.name = '#'
    st.dataframe(
        display_df[['variant', 'count', 'percentage', 'cumulative_%']],
        use_container_width=True,
    )

# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------
chart_tab1, chart_tab2 = st.tabs(["Top 20 Bar Chart", "Distribution Pie"])

with chart_tab1:
    top20 = variants_df.head(20).copy()
    top20['variant_label'] = top20['variant'].str[:60] + top20['variant'].apply(
        lambda v: '...' if len(str(v)) > 60 else ''
    )
    fig_bar = chart_builder.create_bar_chart(
        top20, 'variant_label', 'count',
        "Top 20 Variants by Case Count",
        x_label="Variant", y_label="Count",
    )
    st.plotly_chart(fig_bar, use_container_width=True)

with chart_tab2:
    PIE_LIMIT = 10
    if total_variants > PIE_LIMIT:
        top_pie = variants_df.head(PIE_LIMIT).copy()
        other_count = variants_df.iloc[PIE_LIMIT:]['count'].sum()
        other_row = pd.DataFrame([{
            'variant': 'Other',
            'count': other_count,
            'percentage': variants_df.iloc[PIE_LIMIT:]['percentage'].sum(),
        }])
        pie_df = pd.concat([top_pie[['variant', 'count', 'percentage']], other_row], ignore_index=True)
    else:
        pie_df = variants_df[['variant', 'count', 'percentage']].copy()

    pie_df['variant_label'] = pie_df['variant'].str[:50] + pie_df['variant'].apply(
        lambda v: '...' if len(str(v)) > 50 else ''
    )
    fig_pie = chart_builder.create_pie_chart(
        pie_df, 'variant_label', 'count',
        "Variant Distribution",
    )
    st.plotly_chart(fig_pie, use_container_width=True)

# ---------------------------------------------------------------------------
# Variant detail viewer
# ---------------------------------------------------------------------------
st.subheader("Variant Detail")
variant_options = variants_df['variant'].tolist()
variant_previews = [
    f"#{i+1} ({row['count']} cases, {row['percentage']}%) -- {str(row['variant'])[:80]}"
    for i, row in variants_df.iterrows()
]
selected_idx = st.selectbox(
    "Select a variant to view its full activity sequence",
    range(len(variant_previews)),
    format_func=lambda i: variant_previews[i],
)

if selected_idx is not None:
    full_variant = variant_options[selected_idx]
    activities = full_variant.split(' -> ')
    st.markdown(f"**Case count:** {format_number(variants_df.iloc[selected_idx]['count'])}")
    st.markdown(f"**Percentage:** {variants_df.iloc[selected_idx]['percentage']}%")
    st.markdown(f"**Number of activities:** {len(activities)}")
    st.markdown("**Activity sequence:**")
    for step_num, activity in enumerate(activities, 1):
        st.markdown(f"  {step_num}. {activity}")

# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------
st.divider()
csv_data = variants_df.to_csv(index=False).encode('utf-8')
st.download_button(
    label="Download variant data as CSV",
    data=csv_data,
    file_name="variant_analysis.csv",
    mime="text/csv",
)
