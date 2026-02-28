"""
Performance analyzer module for duration and performance metrics
"""
import pandas as pd
from typing import Dict
from analysis.base_analyzer import BaseAnalyzer


class PerformanceAnalyzer(BaseAnalyzer):
    """Analyzer for performance metrics and durations"""

    def calculate_performance_dfg(self) -> Dict:
        """
        Calculate performance (average duration) for each edge in DFG

        Returns:
            Dictionary mapping (source, target) to average duration in seconds
        """
        performance = {}

        # Create next event for each row
        df_perf = self.df.copy()
        df_perf = df_perf.sort_values(['case:concept:name', 'time:timestamp'])

        # Shift to get next activity and timestamp within same case
        df_perf['next_activity'] = df_perf.groupby('case:concept:name')['concept:name'].shift(-1)
        df_perf['next_timestamp'] = df_perf.groupby('case:concept:name')['time:timestamp'].shift(-1)

        # Remove last event of each case (no next activity)
        df_perf = df_perf.dropna(subset=['next_activity'])

        # Calculate duration
        df_perf['duration'] = (
            df_perf['next_timestamp'] - df_perf['time:timestamp']
        ).dt.total_seconds()

        # Group by edge and calculate mean duration
        edge_durations = df_perf.groupby(['concept:name', 'next_activity'])['duration'].mean()

        # Convert to dictionary
        for (source, target), duration in edge_durations.items():
            performance[(source, target)] = duration

        return performance

    def _get_activity_durations(self) -> pd.DataFrame:
        """Calculate average duration for each activity"""
        df_perf = self.df.copy()
        df_perf = df_perf.sort_values(['case:concept:name', 'time:timestamp'])

        # Get next timestamp within same case
        df_perf['next_timestamp'] = df_perf.groupby('case:concept:name')['time:timestamp'].shift(-1)
        df_perf = df_perf.dropna(subset=['next_timestamp'])

        # Calculate duration
        df_perf['duration'] = (df_perf['next_timestamp'] - df_perf['time:timestamp']).dt.total_seconds()

        # Group by activity
        activity_durations = df_perf.groupby('concept:name')['duration'].agg([
            'count', 'mean', 'median', 'std', 'min', 'max'
        ]).round(2)
        activity_durations.columns = ['count', 'avg_duration', 'median_duration', 'std_duration', 'min_duration', 'max_duration']
        activity_durations = activity_durations.sort_values('avg_duration', ascending=False)

        return activity_durations
