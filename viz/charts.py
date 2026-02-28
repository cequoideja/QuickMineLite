"""
Chart builder module for creating interactive Plotly visualizations.

Adapted from the original QuickMineAnalytics ChartBuilder, simplified for
Streamlit with native theming support and a clean plotly_white base template.
"""

import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from typing import List, Optional


# Fixed color palette for consistent chart styling
COLOR_PALETTE = [
    '#0066FF',  # Blue
    '#00C851',  # Green
    '#ff6b6b',  # Red / Coral
    '#feca57',  # Yellow
    '#48dbfb',  # Light blue
    '#ff9ff3',  # Pink
    '#54a0ff',  # Soft blue
    '#00d2d3',  # Teal
]


class ChartBuilder:
    """Build interactive charts using Plotly with a clean light theme."""

    def __init__(self):
        self.template = self._create_template()
        self.colors = COLOR_PALETTE

    def _create_template(self) -> dict:
        """Create a clean Plotly template based on plotly_white."""
        return {
            'layout': {
                'template': 'plotly_white',
                'colorway': COLOR_PALETTE,
                'font': {'family': 'Inter, system-ui, sans-serif', 'size': 13},
                'title': {'font': {'size': 16}},
                'xaxis': {
                    'gridcolor': '#e8e8e8',
                    'zerolinecolor': '#e8e8e8',
                },
                'yaxis': {
                    'gridcolor': '#e8e8e8',
                    'zerolinecolor': '#e8e8e8',
                },
                'margin': {'l': 60, 'r': 30, 't': 50, 'b': 50},
            }
        }

    def create_bar_chart(
        self,
        df: pd.DataFrame,
        x_col: str,
        y_col: str,
        title: str,
        x_label: Optional[str] = None,
        y_label: Optional[str] = None,
        orientation: str = 'v',
    ) -> go.Figure:
        """
        Create a bar chart.

        Args:
            df: DataFrame with data.
            x_col: Column for x-axis (categories).
            y_col: Column for y-axis (values).
            title: Chart title.
            x_label: Custom x-axis label (defaults to column name).
            y_label: Custom y-axis label (defaults to column name).
            orientation: 'v' for vertical, 'h' for horizontal.

        Returns:
            A Plotly Figure object.
        """
        fig = go.Figure()

        if orientation == 'v':
            fig.add_trace(go.Bar(
                x=df[x_col],
                y=df[y_col],
                marker_color=self.colors[0],
                hovertemplate='%{x}<br>%{y:,}<extra></extra>',
            ))
        else:
            fig.add_trace(go.Bar(
                x=df[y_col],
                y=df[x_col],
                orientation='h',
                marker_color=self.colors[0],
                hovertemplate='%{y}<br>%{x:,}<extra></extra>',
            ))

        fig.update_layout(
            title=title,
            xaxis_title=x_label or x_col,
            yaxis_title=y_label or y_col,
            template=self.template,
            hovermode='closest',
        )

        return fig

    def create_histogram(
        self,
        data: pd.Series,
        title: str,
        x_label: str,
        bins: int = 30,
    ) -> go.Figure:
        """
        Create a histogram.

        Args:
            data: Series of values to bin.
            title: Chart title.
            x_label: X-axis label.
            bins: Number of histogram bins.

        Returns:
            A Plotly Figure object.
        """
        fig = go.Figure()

        fig.add_trace(go.Histogram(
            x=data,
            nbinsx=bins,
            marker_color=self.colors[0],
            hovertemplate='Range: %{x}<br>Count: %{y}<extra></extra>',
        ))

        fig.update_layout(
            title=title,
            xaxis_title=x_label,
            yaxis_title='Count',
            template=self.template,
            hovermode='closest',
        )

        return fig

    def create_time_series(
        self,
        df: pd.DataFrame,
        date_col: str,
        value_cols: List[str],
        title: str,
        y_label: str = 'Count',
    ) -> go.Figure:
        """
        Create a time series line chart.

        Args:
            df: DataFrame with date column and one or more value columns.
            date_col: Column containing date/time values.
            value_cols: List of column names to plot as separate lines.
            title: Chart title.
            y_label: Y-axis label.

        Returns:
            A Plotly Figure object.
        """
        fig = go.Figure()

        for i, col in enumerate(value_cols):
            fig.add_trace(go.Scatter(
                x=df[date_col],
                y=df[col],
                mode='lines+markers',
                name=col,
                line=dict(color=self.colors[i % len(self.colors)]),
                hovertemplate='%{x}<br>%{y:,}<extra></extra>',
            ))

        fig.update_layout(
            title=title,
            xaxis_title='Date',
            yaxis_title=y_label,
            template=self.template,
            hovermode='x unified',
        )

        return fig

    def create_pie_chart(
        self,
        df: pd.DataFrame,
        labels_col: str,
        values_col: str,
        title: str,
    ) -> go.Figure:
        """
        Create a pie chart.

        Args:
            df: DataFrame with label and value columns.
            labels_col: Column for slice labels.
            values_col: Column for slice values.
            title: Chart title.

        Returns:
            A Plotly Figure object.
        """
        fig = go.Figure()

        fig.add_trace(go.Pie(
            labels=df[labels_col],
            values=df[values_col],
            marker=dict(colors=self.colors),
            hovertemplate='%{label}<br>%{value:,} (%{percent})<extra></extra>',
        ))

        fig.update_layout(
            title=title,
            template=self.template,
        )

        return fig

    def create_scatter_plot(
        self,
        df: pd.DataFrame,
        x_col: str,
        y_col: str,
        title: str,
        color_col: Optional[str] = None,
        size_col: Optional[str] = None,
    ) -> go.Figure:
        """
        Create a scatter plot.

        Args:
            df: DataFrame with data.
            x_col: Column for x-axis.
            y_col: Column for y-axis.
            title: Chart title.
            color_col: Optional column to color points by category.
            size_col: Optional column to size points by value.

        Returns:
            A Plotly Figure object.
        """
        fig = px.scatter(
            df,
            x=x_col,
            y=y_col,
            color=color_col,
            size=size_col,
            title=title,
            color_discrete_sequence=self.colors,
        )

        fig.update_layout(template=self.template)
        fig.update_traces(
            marker=dict(line=dict(width=0.5, color='#333333')),
        )

        return fig

    def create_heatmap(
        self,
        df: pd.DataFrame,
        x_col: str,
        y_col: str,
        value_col: str,
        title: str,
    ) -> go.Figure:
        """
        Create a heatmap.

        Args:
            df: DataFrame with columns for x, y, and the value to visualize.
            x_col: Column for x-axis categories.
            y_col: Column for y-axis categories.
            value_col: Column for cell values.
            title: Chart title.

        Returns:
            A Plotly Figure object.
        """
        pivot = df.pivot(index=y_col, columns=x_col, values=value_col)

        fig = go.Figure()

        fig.add_trace(go.Heatmap(
            z=pivot.values,
            x=pivot.columns.tolist(),
            y=pivot.index.tolist(),
            colorscale='Blues',
            hovertemplate='%{x}<br>%{y}<br>%{z}<extra></extra>',
        ))

        fig.update_layout(
            title=title,
            template=self.template,
        )

        return fig

    def create_box_plot(
        self,
        df: pd.DataFrame,
        y_col: str,
        x_col: Optional[str] = None,
        title: str = '',
    ) -> go.Figure:
        """
        Create a box plot.

        Args:
            df: DataFrame with data.
            y_col: Column for the values axis.
            x_col: Optional column to group boxes by category.
            title: Chart title.

        Returns:
            A Plotly Figure object.
        """
        fig = go.Figure()

        if x_col:
            categories = df[x_col].unique()
            for i, category in enumerate(categories):
                data = df[df[x_col] == category][y_col]
                fig.add_trace(go.Box(
                    y=data,
                    name=str(category),
                    marker_color=self.colors[i % len(self.colors)],
                ))
        else:
            fig.add_trace(go.Box(
                y=df[y_col],
                marker_color=self.colors[0],
            ))

        fig.update_layout(
            title=title,
            template=self.template,
            showlegend=bool(x_col),
        )

        return fig


# ---------------------------------------------------------------------------
# Cortado-style Variant Explorer (HTML component)
# ---------------------------------------------------------------------------

# Vivid color palette for activity chevrons on dark background
_ACTIVITY_PALETTE = [
    '#4CAF50', '#2196F3', '#FF9800', '#9C27B0', '#F44336',
    '#00BCD4', '#FFEB3B', '#E91E63', '#3F51B5', '#009688',
    '#FF5722', '#8BC34A', '#795548', '#607D8B', '#CDDC39',
    '#03A9F4', '#673AB7', '#FFC107', '#76FF03', '#FF4081',
    '#B388FF', '#84FFFF', '#CCFF90', '#FFD180', '#FF80AB',
]


def _text_color_for_bg(hex_color: str) -> str:
    """Return white or black text depending on background luminance."""
    h = hex_color.lstrip('#')
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    luminance = (0.299 * r + 0.587 * g + 0.114 * b) / 255
    return '#111' if luminance > 0.55 else '#fff'


def _html_escape(text: str) -> str:
    return text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')


def build_variant_explorer_html(
    variants_df: pd.DataFrame,
    max_rows: int = 50,
) -> tuple:
    """
    Build a Cortado-style variant explorer as self-contained HTML.

    Args:
        variants_df: DataFrame with columns 'variant', 'count', 'percentage'.
                     variant is a string like "A -> B -> C".
        max_rows: Maximum number of variant rows to display.

    Returns:
        Tuple of (html_string, recommended_iframe_height_px).
    """
    df = variants_df.head(max_rows)

    # Collect unique activities in order of first appearance
    all_activities: list[str] = []
    seen: set[str] = set()
    for variant_str in df['variant']:
        for act in str(variant_str).split(' -> '):
            act = act.strip()
            if act and act not in seen:
                all_activities.append(act)
                seen.add(act)

    color_map = {
        act: _ACTIVITY_PALETTE[i % len(_ACTIVITY_PALETTE)]
        for i, act in enumerate(all_activities)
    }

    # --- Build rows --------------------------------------------------------
    rows_html_parts = []
    for i, (_, row) in enumerate(df.iterrows()):
        activities = [a.strip() for a in str(row['variant']).split(' -> ')]
        chevrons = []
        for act in activities:
            bg = color_map.get(act, '#666')
            fg = _text_color_for_bg(bg)
            safe = _html_escape(act)
            chevrons.append(
                f'<span class="chv" style="background:{bg};color:{fg}" title="{safe}">{safe}</span>'
            )

        pct = row['percentage']
        count = int(row['count'])
        rows_html_parts.append(
            f'<div class="vr">'
            f'<div class="vno">{i + 1}.</div>'
            f'<div class="vinf"><span class="vp">{pct:.2f}%</span><br>'
            f'<span class="vc">({count:,})</span></div>'
            f'<div class="vchv">{"".join(chevrons)}</div>'
            f'</div>'
        )

    # --- Legend -------------------------------------------------------------
    legend_parts = []
    for act in all_activities:
        bg = color_map[act]
        fg = _text_color_for_bg(bg)
        safe = _html_escape(act)
        legend_parts.append(
            f'<span class="lg-it" style="background:{bg};color:{fg}">{safe}</span>'
        )

    n_activities = len(all_activities)
    row_h = 38
    header_h = 40
    legend_h = 16 + 30 * max(1, (n_activities + 7) // 8)
    content_h = header_h + len(df) * row_h + legend_h
    max_h = 640
    container_h = min(content_h, max_h)

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:transparent;font-family:'Segoe UI',system-ui,-apple-system,sans-serif}}

.ve{{background:#1e1e2e;border-radius:8px;overflow:hidden;display:flex;
    flex-direction:column;height:{container_h}px}}

.ve-hd{{display:flex;align-items:center;padding:10px 12px;
    border-bottom:2px solid #333;color:#888;font-size:11px;font-weight:600;
    text-transform:uppercase;letter-spacing:.5px;background:#1e1e2e;flex-shrink:0}}
.ve-hd .cn{{width:44px}}.ve-hd .ci{{width:90px}}.ve-hd .cv{{flex:1}}

.ve-sc{{overflow-y:auto;flex:1}}

.vr{{display:flex;align-items:center;padding:5px 12px;
    border-bottom:1px solid #2a2a3a;min-height:{row_h}px;transition:background .15s}}
.vr:hover{{background:#2a2a3e}}

.vno{{width:44px;color:#888;font-size:13px;font-weight:500;flex-shrink:0}}
.vinf{{width:90px;font-size:12px;line-height:1.4;flex-shrink:0}}
.vp{{font-weight:600;color:#eee}}.vc{{color:#888;font-size:11px}}

.vchv{{flex:1;display:flex;align-items:center;overflow:hidden}}

.chv{{display:inline-flex;align-items:center;justify-content:center;
    height:26px;min-width:32px;max-width:150px;
    padding:0 16px 0 18px;font-size:11px;font-weight:500;
    white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
    margin-left:-6px;flex-shrink:0;cursor:default;
    clip-path:polygon(0 0,calc(100% - 10px) 0,100% 50%,calc(100% - 10px) 100%,0 100%,10px 50%)}}
.chv:first-child{{margin-left:0;padding-left:10px;
    clip-path:polygon(0 0,calc(100% - 10px) 0,100% 50%,calc(100% - 10px) 100%,0 100%);
    border-radius:3px 0 0 3px}}

.lg{{display:flex;flex-wrap:wrap;gap:6px;padding:10px 12px;
    border-top:2px solid #333;background:#1e1e2e;flex-shrink:0}}
.lg-it{{display:inline-block;padding:3px 10px;border-radius:4px;
    font-size:11px;font-weight:500}}
</style></head><body>
<div class="ve">
  <div class="ve-hd">
    <div class="cn">No.</div><div class="ci">Info</div>
    <div class="cv">Variant ({n_activities} activities)</div>
  </div>
  <div class="ve-sc">{"".join(rows_html_parts)}</div>
  <div class="lg">{"".join(legend_parts)}</div>
</div>
</body></html>"""

    return html, container_h + 16
