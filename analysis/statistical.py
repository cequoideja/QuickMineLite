"""
Statistical analyzer module for summary statistics and distributions
"""
import pandas as pd
import numpy as np
from typing import Dict
from analysis.base_analyzer import BaseAnalyzer


class StatisticalAnalyzer(BaseAnalyzer):
    """Analyzer for statistical metrics and distributions"""

    def get_summary_statistics(self) -> Dict[str, any]:
        """
        Calculate summary statistics for the event log

        Returns:
            Dictionary with various statistics
        """
        stats = {}

        # Basic counts
        stats['total_events'] = len(self.df)
        stats['total_cases'] = self.df['case:concept:name'].nunique()
        stats['total_activities'] = self.df['concept:name'].nunique()

        # Date range
        stats['start_date'] = self.df['time:timestamp'].min()
        stats['end_date'] = self.df['time:timestamp'].max()
        stats['duration'] = stats['end_date'] - stats['start_date']

        # Case duration statistics
        case_durations = self._calculate_case_durations()
        stats['avg_case_duration'] = case_durations.mean()
        stats['median_case_duration'] = case_durations.median()
        stats['min_case_duration'] = case_durations.min()
        stats['max_case_duration'] = case_durations.max()
        stats['std_case_duration'] = case_durations.std()

        # Events per case statistics
        events_per_case = self.df.groupby('case:concept:name').size()
        stats['avg_events_per_case'] = events_per_case.mean()
        stats['median_events_per_case'] = events_per_case.median()
        stats['min_events_per_case'] = events_per_case.min()
        stats['max_events_per_case'] = events_per_case.max()

        # Activity frequency
        activity_counts = self.df['concept:name'].value_counts()
        stats['most_common_activity'] = activity_counts.index[0]
        stats['most_common_activity_count'] = activity_counts.iloc[0]

        return stats

    def get_activity_distribution(self) -> pd.DataFrame:
        """
        Get activity frequency distribution

        Returns:
            DataFrame with activity, count, and percentage
        """
        activity_counts = self.df['concept:name'].value_counts().reset_index()
        activity_counts.columns = ['activity', 'count']
        activity_counts['percentage'] = (activity_counts['count'] / activity_counts['count'].sum() * 100).round(2)

        return activity_counts

    def get_case_duration_distribution(self, bins: int = 20) -> pd.DataFrame:
        """
        Get case duration distribution

        Args:
            bins: Number of bins for histogram

        Returns:
            DataFrame with duration ranges and counts
        """
        durations = self._calculate_case_durations()

        # Create histogram
        counts, bin_edges = np.histogram(durations, bins=bins)

        # Create DataFrame
        distribution = pd.DataFrame({
            'duration_min': bin_edges[:-1],
            'duration_max': bin_edges[1:],
            'count': counts
        })

        distribution['duration_label'] = distribution.apply(
            lambda row: f"{self._format_duration(row['duration_min'])} - {self._format_duration(row['duration_max'])}",
            axis=1
        )

        return distribution

    def get_events_over_time(self, freq: str = 'D') -> pd.DataFrame:
        """
        Get number of events/cases over time

        Args:
            freq: Frequency for grouping (D=day, W=week, M=month)

        Returns:
            DataFrame with date, event_count, and case_count
        """
        df_time = self.df.copy()
        df_time['date'] = df_time['time:timestamp'].dt.to_period(freq).dt.to_timestamp()

        # Count events per period
        events = df_time.groupby('date').size().reset_index(name='event_count')

        # Count unique cases per period
        cases = df_time.groupby('date')['case:concept:name'].nunique().reset_index(name='case_count')

        # Merge
        result = pd.merge(events, cases, on='date')

        return result

    def get_events_per_case_distribution(self) -> pd.DataFrame:
        """
        Get distribution of number of events per case

        Returns:
            DataFrame with num_events and case_count
        """
        events_per_case = self.df.groupby('case:concept:name').size()
        distribution = events_per_case.value_counts().sort_index().reset_index()
        distribution.columns = ['num_events', 'case_count']
        return distribution
