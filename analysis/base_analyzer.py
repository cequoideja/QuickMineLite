"""
Base analyzer module with core data preparation and utilities
"""
import pandas as pd
from typing import Optional, List
from pm4py.objects.conversion.log import converter as log_converter


class BaseAnalyzer:
    """Base analyzer with core data preparation and utilities"""

    def __init__(self, event_log_df: pd.DataFrame):
        """
        Initialize analyzer with event log

        Args:
            event_log_df: Event log DataFrame with standard column names
        """
        self.df = event_log_df.copy()
        self.log = None
        self._prepare_log()

    def set_data(self, event_log_df: pd.DataFrame):
        """
        Set new event log data

        Args:
            event_log_df: Event log DataFrame with standard column names
        """
        self.df = event_log_df.copy()
        self._prepare_log()

    def _prepare_log(self):
        """Convert DataFrame to pm4py event log format"""
        # Ensure timestamp is datetime
        if not pd.api.types.is_datetime64_any_dtype(self.df['time:timestamp']):
            self.df['time:timestamp'] = pd.to_datetime(self.df['time:timestamp'])

        # Sort by case and timestamp
        self.df = self.df.sort_values(['case:concept:name', 'time:timestamp'])

        # Convert to pm4py event log
        self.log = log_converter.apply(
            self.df,
            variant=log_converter.Variants.TO_EVENT_LOG
        )

    def filter_by_timeframe(self, start_date: Optional[pd.Timestamp], end_date: Optional[pd.Timestamp]):
        """
        Filter event log by timeframe

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
        """
        if start_date:
            self.df = self.df[self.df['time:timestamp'] >= start_date]
        if end_date:
            self.df = self.df[self.df['time:timestamp'] <= end_date]

        self._prepare_log()

    def filter_by_activities(self, activities: List[str]):
        """
        Filter to keep only specified activities

        Args:
            activities: List of activity names to keep
        """
        self.df = self.df[self.df['concept:name'].isin(activities)]
        self._prepare_log()

    @staticmethod
    def _format_duration(seconds: float) -> str:
        """Format duration in seconds to human-readable format"""
        if pd.isna(seconds):
            return "N/A"

        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f}m"
        elif seconds < 86400:
            hours = seconds / 3600
            return f"{hours:.1f}h"
        else:
            days = seconds / 86400
            return f"{days:.1f}d"

    def _calculate_case_durations(self) -> pd.Series:
        """Calculate duration for each case in seconds"""
        case_times = self.df.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
        durations = (case_times['max'] - case_times['min']).dt.total_seconds()
        return durations
