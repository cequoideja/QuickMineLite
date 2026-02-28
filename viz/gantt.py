"""
Gantt chart module for visualizing case timelines.

Creates a Plotly Gantt-style chart (horizontal bars) showing each activity
in a single case as a bar spanning from its timestamp to the next event's
timestamp. Adapted from the original QuickMineAnalytics CaseGanttDialog.
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Optional


# Color palette for activity bars
_ACTIVITY_COLORS = [
    '#0066FF', '#00C851', '#ff6b6b', '#feca57',
    '#48dbfb', '#ff9ff3', '#54a0ff', '#00d2d3',
    '#5f27cd', '#ee5a24', '#01a3a4', '#f368e0',
    '#10ac84', '#341f97', '#c8d6e5', '#ffa502',
]


def _format_duration(total_seconds: float) -> str:
    """
    Format a duration in seconds into a human-readable string.

    Examples: "2d 5h 30m 15s", "1h 23m", "45s".

    Args:
        total_seconds: Duration in seconds.

    Returns:
        Formatted duration string.
    """
    if total_seconds < 0:
        total_seconds = 0

    days = int(total_seconds // 86400)
    hours = int((total_seconds % 86400) // 3600)
    minutes = int((total_seconds % 3600) // 60)
    seconds = int(total_seconds % 60)

    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if seconds > 0 or len(parts) == 0:
        parts.append(f"{seconds}s")

    return " ".join(parts)


def create_gantt_chart(
    case_events_df: pd.DataFrame,
    color_col: Optional[str] = None,
    title: Optional[str] = None,
) -> go.Figure:
    """
    Create a Gantt-style timeline chart for events in a single case.

    Each event becomes a horizontal bar from its timestamp to the next
    event's timestamp. The last event gets a default duration of 1 hour.
    Bars are colored by activity name (concept:name) by default, or by
    a custom column if specified.

    A minimum display width is enforced so that very short activities
    remain visible. The hover tooltip always shows the real duration.

    Args:
        case_events_df: DataFrame containing events for one case.
            Required columns:
                - concept:name: Activity name.
                - time:timestamp: Event timestamp.
            Optional columns:
                - org:resource: Resource performing the activity.
                - Any additional attribute columns.
        color_col: Column to use for coloring bars. Defaults to
            'concept:name' (color by activity).
        title: Chart title. Defaults to "Case Event Timeline".

    Returns:
        A Plotly Figure object with the Gantt chart.
    """
    if case_events_df.empty:
        fig = go.Figure()
        fig.update_layout(
            title=title or "Case Event Timeline",
            annotations=[dict(
                text="No events to display",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="#999999"),
            )],
        )
        return fig

    df = case_events_df.copy()
    df = df.sort_values('time:timestamp').reset_index(drop=True)

    # Determine the color column
    if color_col is None:
        color_col = 'concept:name'

    # Calculate the total case duration for minimum display width
    case_start = df['time:timestamp'].min()
    case_end = df['time:timestamp'].max()
    case_duration_seconds = (case_end - case_start).total_seconds()
    # Minimum display width: at least 5 minutes or 1% of total case duration
    min_display_seconds = max(300, case_duration_seconds * 0.01)

    # Build Gantt data rows
    gantt_rows = []
    for idx in range(len(df)):
        row = df.iloc[idx]
        start_time = row['time:timestamp']

        # Determine end time: next event's timestamp, or +1 hour for last event
        if idx < len(df) - 1:
            next_time = df.iloc[idx + 1]['time:timestamp']
        else:
            next_time = start_time + pd.Timedelta(hours=1)

        # Real duration for hover
        real_duration_seconds = (next_time - start_time).total_seconds()
        duration_str = _format_duration(real_duration_seconds)

        # Display duration with minimum width for visibility
        display_seconds = max(real_duration_seconds, min_display_seconds)
        display_end = start_time + pd.Timedelta(seconds=display_seconds)

        activity = row.get('concept:name', 'Unknown')
        resource = row.get('org:resource', None)
        color_value = str(row.get(color_col, 'Unknown'))

        gantt_entry = {
            'Task': activity,
            'Start': start_time,
            'Finish': display_end,
            'Duration': duration_str,
            'ColorGroup': color_value,
        }

        if resource is not None and pd.notna(resource):
            gantt_entry['Resource'] = str(resource)

        gantt_rows.append(gantt_entry)

    df_gantt = pd.DataFrame(gantt_rows)

    # Build hover data columns
    hover_cols = ['Duration']
    if 'Resource' in df_gantt.columns:
        hover_cols.append('Resource')

    # Create the timeline figure
    fig = px.timeline(
        df_gantt,
        x_start='Start',
        x_end='Finish',
        y='Task',
        color='ColorGroup',
        hover_data=hover_cols,
        title=title or 'Case Event Timeline',
        text='Duration',
        color_discrete_sequence=_ACTIVITY_COLORS,
    )

    # Update trace styling
    fig.update_traces(
        textposition='inside',
        textfont=dict(size=10, color='white'),
        insidetextanchor='middle',
        marker=dict(line=dict(width=0.5, color='#cccccc')),
    )

    # Maintain event order (first event at top)
    fig.update_yaxes(
        autorange='reversed',
        categoryorder='array',
        categoryarray=df_gantt['Task'].tolist(),
        tickfont=dict(size=11),
        tickmode='linear',
    )

    # Readable date format on x-axis
    fig.update_xaxes(
        tickformat='%Y-%m-%d\n%H:%M:%S',
        tickfont=dict(size=10),
    )

    # Dynamic height based on number of events
    dynamic_height = max(400, len(df_gantt) * 40 + 100)

    fig.update_layout(
        height=dynamic_height,
        showlegend=True,
        xaxis_title='Time',
        yaxis_title='Activity',
        template='plotly_white',
        hoverlabel=dict(
            font_size=12,
            font_family='monospace',
        ),
        margin=dict(l=150, r=50, t=50, b=50),
        bargap=0.2,
        legend_title_text=color_col,
    )

    return fig
