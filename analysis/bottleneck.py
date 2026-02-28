"""
Bottleneck Analysis Module

This module provides bottleneck detection functionality for process mining analysis.
It identifies performance issues including:
- Activity duration bottlenecks
- Waiting time bottlenecks
- Frequency-based bottlenecks
- Resource bottlenecks
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional


class BottleneckAnalyzer:
    """Analyzer for detecting bottlenecks in process execution"""

    def __init__(self, event_log: pd.DataFrame):
        """
        Initialize bottleneck analyzer

        Args:
            event_log: Event log DataFrame with standard columns
        """
        self.event_log = event_log
        self.case_col = 'case:concept:name'
        self.activity_col = 'concept:name'
        self.timestamp_col = 'time:timestamp'

        # Auto-detect resource column with multiple possible names
        self.resource_col = self._detect_resource_column(event_log)

    def _detect_resource_column(self, event_log: pd.DataFrame) -> Optional[str]:
        """
        Detect resource column from common naming patterns

        Args:
            event_log: Event log DataFrame

        Returns:
            Name of resource column or None if not found
        """
        # List of common resource column names (case-insensitive)
        resource_patterns = [
            'org:resource',
            'resource',
            'Resource',
            'org:group',
            'user',
            'User',
            'actor',
            'Actor',
            'performer',
            'Performer'
        ]

        # First try exact match
        for pattern in resource_patterns:
            if pattern in event_log.columns:
                return pattern

        # Then try case-insensitive match
        columns_lower = {col.lower(): col for col in event_log.columns}
        for pattern in resource_patterns:
            if pattern.lower() in columns_lower:
                return columns_lower[pattern.lower()]

        return None

    def analyze_activity_duration_bottlenecks(self, top_n: int = 10) -> pd.DataFrame:
        """
        Identify activities with the longest average duration

        Args:
            top_n: Number of top bottlenecks to return

        Returns:
            DataFrame with activity duration statistics
        """
        # Calculate duration for each activity within cases
        df = self.event_log.sort_values([self.case_col, self.timestamp_col]).copy()

        # Add next timestamp within each case
        df['next_timestamp'] = df.groupby(self.case_col)[self.timestamp_col].shift(-1)

        # Calculate duration (time until next event in the case)
        df['duration'] = (df['next_timestamp'] - df[self.timestamp_col]).dt.total_seconds()

        # Remove last event of each case (no duration)
        df = df[df['duration'].notna()]

        # Aggregate by activity
        activity_stats = df.groupby(self.activity_col).agg({
            'duration': ['mean', 'median', 'std', 'min', 'max', 'count']
        }).reset_index()

        activity_stats.columns = ['activity', 'mean_duration', 'median_duration',
                                  'std_duration', 'min_duration', 'max_duration', 'count']

        # Calculate bottleneck score (combination of mean duration and frequency)
        # Normalize both metrics to 0-1 range
        max_mean = activity_stats['mean_duration'].max()
        max_count = activity_stats['count'].max()

        if max_mean > 0 and max_count > 0:
            activity_stats['normalized_duration'] = activity_stats['mean_duration'] / max_mean
            activity_stats['normalized_frequency'] = activity_stats['count'] / max_count
            activity_stats['bottleneck_score'] = (
                activity_stats['normalized_duration'] * 0.7 +
                activity_stats['normalized_frequency'] * 0.3
            )
        else:
            activity_stats['bottleneck_score'] = 0

        # Sort by bottleneck score and return top N
        result = activity_stats.sort_values('bottleneck_score', ascending=False).head(top_n)

        # Convert durations to human-readable format
        for col in ['mean_duration', 'median_duration', 'std_duration', 'min_duration', 'max_duration']:
            result[f'{col}_formatted'] = result[col].apply(self._format_duration)

        return result

    def analyze_waiting_time_bottlenecks(self, top_n: int = 10) -> pd.DataFrame:
        """
        Identify activity transitions with the longest waiting times

        Args:
            top_n: Number of top bottlenecks to return

        Returns:
            DataFrame with transition waiting time statistics
        """
        df = self.event_log.sort_values([self.case_col, self.timestamp_col]).copy()

        # Create activity pairs (from -> to)
        df['from_activity'] = df.groupby(self.case_col)[self.activity_col].shift(1)
        df['from_timestamp'] = df.groupby(self.case_col)[self.timestamp_col].shift(1)

        # Calculate waiting time
        df['waiting_time'] = (df[self.timestamp_col] - df['from_timestamp']).dt.total_seconds()

        # Remove first event of each case
        df = df[df['from_activity'].notna()]

        # Create transition label
        df['transition'] = df['from_activity'] + ' \u2192 ' + df[self.activity_col]

        # Aggregate by transition
        transition_stats = df.groupby('transition').agg({
            'waiting_time': ['mean', 'median', 'std', 'min', 'max', 'count']
        }).reset_index()

        transition_stats.columns = ['transition', 'mean_waiting', 'median_waiting',
                                    'std_waiting', 'min_waiting', 'max_waiting', 'count']

        # Calculate bottleneck score
        max_mean = transition_stats['mean_waiting'].max()
        max_count = transition_stats['count'].max()

        if max_mean > 0 and max_count > 0:
            transition_stats['normalized_waiting'] = transition_stats['mean_waiting'] / max_mean
            transition_stats['normalized_frequency'] = transition_stats['count'] / max_count
            transition_stats['bottleneck_score'] = (
                transition_stats['normalized_waiting'] * 0.7 +
                transition_stats['normalized_frequency'] * 0.3
            )
        else:
            transition_stats['bottleneck_score'] = 0

        # Sort by bottleneck score
        result = transition_stats.sort_values('bottleneck_score', ascending=False).head(top_n)

        # Format durations
        for col in ['mean_waiting', 'median_waiting', 'std_waiting', 'min_waiting', 'max_waiting']:
            result[f'{col}_formatted'] = result[col].apply(self._format_duration)

        return result

    def analyze_frequency_bottlenecks(self, top_n: int = 10) -> pd.DataFrame:
        """
        Identify activities that occur most frequently (potential congestion points)

        Args:
            top_n: Number of top bottlenecks to return

        Returns:
            DataFrame with activity frequency statistics
        """
        # Count activity occurrences
        activity_counts = self.event_log[self.activity_col].value_counts().reset_index()
        activity_counts.columns = ['activity', 'total_occurrences']

        # Calculate occurrences per case
        total_cases = self.event_log[self.case_col].nunique()
        activity_counts['avg_per_case'] = activity_counts['total_occurrences'] / total_cases

        # Calculate percentage of total events
        total_events = len(self.event_log)
        activity_counts['percentage'] = (activity_counts['total_occurrences'] / total_events * 100).round(2)

        # Calculate cases where activity appears
        cases_with_activity = self.event_log.groupby(self.activity_col)[self.case_col].nunique().reset_index()
        cases_with_activity.columns = ['activity', 'cases_with_activity']

        # Merge
        result = activity_counts.merge(cases_with_activity, on='activity')
        result['case_coverage_%'] = (result['cases_with_activity'] / total_cases * 100).round(2)

        # Calculate bottleneck score based on frequency
        max_occurrences = result['total_occurrences'].max()
        if max_occurrences > 0:
            result['bottleneck_score'] = result['total_occurrences'] / max_occurrences
        else:
            result['bottleneck_score'] = 0

        return result.head(top_n)

    def analyze_resource_bottlenecks(self, top_n: int = 10) -> Optional[pd.DataFrame]:
        """
        Identify resources with the highest workload

        Args:
            top_n: Number of top bottlenecks to return

        Returns:
            DataFrame with resource workload statistics, or None if no resource data
        """
        if self.resource_col is None or self.resource_col not in self.event_log.columns:
            return None

        # Count events per resource
        resource_stats = self.event_log.groupby(self.resource_col).agg({
            self.case_col: 'nunique',
            self.activity_col: 'count'
        }).reset_index()

        resource_stats.columns = ['resource', 'unique_cases', 'total_events']

        # Calculate average events per case
        resource_stats['avg_events_per_case'] = (
            resource_stats['total_events'] / resource_stats['unique_cases']
        ).round(2)

        # Calculate activity diversity
        activity_diversity = self.event_log.groupby(self.resource_col)[self.activity_col].nunique().reset_index()
        activity_diversity.columns = ['resource', 'unique_activities']

        result = resource_stats.merge(activity_diversity, on='resource')

        # Calculate percentage of total events
        total_events = len(self.event_log)
        result['workload_%'] = (result['total_events'] / total_events * 100).round(2)

        # Calculate bottleneck score based on workload
        max_events = result['total_events'].max()
        if max_events > 0:
            result['bottleneck_score'] = result['total_events'] / max_events
        else:
            result['bottleneck_score'] = 0

        return result.sort_values('bottleneck_score', ascending=False).head(top_n)

    def get_bottleneck_summary(self) -> Dict:
        """
        Get a comprehensive summary of all bottleneck analyses

        Returns:
            Dictionary containing all bottleneck analysis results
        """
        summary = {
            'activity_duration': self.analyze_activity_duration_bottlenecks(top_n=10),
            'waiting_time': self.analyze_waiting_time_bottlenecks(top_n=10),
            'frequency': self.analyze_frequency_bottlenecks(top_n=10),
            'resource': self.analyze_resource_bottlenecks(top_n=10)
        }

        return summary

    def _format_duration(self, seconds: float) -> str:
        """
        Format duration in seconds to human-readable string

        Args:
            seconds: Duration in seconds

        Returns:
            Formatted duration string
        """
        if pd.isna(seconds):
            return 'N/A'

        if seconds < 0:
            return 'Invalid'

        # Convert to appropriate unit
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f}m"
        elif seconds < 86400:
            hours = seconds / 3600
            return f"{hours:.1f}h"
        else:
            days = seconds / 86400
            return f"{days:.1f}d"

    def get_recommendations(self) -> List[Dict[str, str]]:
        """
        Generate recommendations based on bottleneck analysis

        Returns:
            List of recommendation dictionaries
        """
        recommendations = []

        # Analyze activity durations
        duration_bottlenecks = self.analyze_activity_duration_bottlenecks(top_n=3)
        if not duration_bottlenecks.empty:
            top_activity = duration_bottlenecks.iloc[0]
            recommendations.append({
                'type': 'Activity Duration',
                'severity': 'High' if top_activity['bottleneck_score'] > 0.7 else 'Medium',
                'description': f"Activity '{top_activity['activity']}' has an average duration of {top_activity['mean_duration_formatted']}",
                'recommendation': 'Consider optimizing this activity or parallelizing it with other tasks.'
            })

        # Analyze waiting times
        waiting_bottlenecks = self.analyze_waiting_time_bottlenecks(top_n=3)
        if not waiting_bottlenecks.empty:
            top_transition = waiting_bottlenecks.iloc[0]
            recommendations.append({
                'type': 'Waiting Time',
                'severity': 'High' if top_transition['bottleneck_score'] > 0.7 else 'Medium',
                'description': f"Transition '{top_transition['transition']}' has an average waiting time of {top_transition['mean_waiting_formatted']}",
                'recommendation': 'Consider reducing handoff time or automating the transition.'
            })

        # Analyze frequency
        frequency_bottlenecks = self.analyze_frequency_bottlenecks(top_n=3)
        if not frequency_bottlenecks.empty:
            top_freq = frequency_bottlenecks.iloc[0]
            if top_freq['percentage'] > 20:
                recommendations.append({
                    'type': 'High Frequency',
                    'severity': 'Medium',
                    'description': f"Activity '{top_freq['activity']}' represents {top_freq['percentage']}% of all events",
                    'recommendation': 'Consider streamlining or automating this frequently occurring activity.'
                })

        # Analyze resources
        resource_bottlenecks = self.analyze_resource_bottlenecks(top_n=3)
        if resource_bottlenecks is not None and not resource_bottlenecks.empty:
            top_resource = resource_bottlenecks.iloc[0]
            if top_resource['workload_%'] > 20:
                recommendations.append({
                    'type': 'Resource Overload',
                    'severity': 'High' if top_resource['workload_%'] > 30 else 'Medium',
                    'description': f"Resource '{top_resource['resource']}' handles {top_resource['workload_%']}% of all events",
                    'recommendation': 'Consider load balancing or adding additional resources.'
                })

        return recommendations
