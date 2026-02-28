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
