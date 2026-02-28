"""
Correlation analyzer module for analyzing correlations between attributes
"""
import pandas as pd
import numpy as np
from typing import Dict, Optional
from analysis.performance import PerformanceAnalyzer


class CorrelationAnalyzer(PerformanceAnalyzer):
    """Analyzer for correlations between different attributes"""

    def analyze_correlations(self) -> Dict:
        """
        Analyze correlations between different attributes

        Returns:
            Dictionary with correlation analyses
        """
        results = {}

        # 1. Correlation between numeric attributes
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns.tolist()
        # Exclude standard columns
        numeric_cols = [col for col in numeric_cols if col not in ['case:concept:name', 'concept:name']]

        if len(numeric_cols) >= 2:
            correlation_matrix = self.df[numeric_cols].corr()
            results['numeric_correlations'] = correlation_matrix
        else:
            results['numeric_correlations'] = None

        # 2. Activity duration correlation
        activity_durations = self._get_activity_durations()
        if not activity_durations.empty:
            results['activity_duration_stats'] = activity_durations

        # 3. Case duration vs number of events correlation
        case_stats = self.df.groupby('case:concept:name').agg({
            'time:timestamp': ['min', 'max', 'count']
        })
        case_stats.columns = ['start_time', 'end_time', 'num_events']
        case_stats['duration_seconds'] = (case_stats['end_time'] - case_stats['start_time']).dt.total_seconds()

        if len(case_stats) > 1:
            correlation_events_duration = case_stats[['num_events', 'duration_seconds']].corr().iloc[0, 1]
            results['events_duration_correlation'] = correlation_events_duration
            results['case_stats'] = case_stats
        else:
            results['events_duration_correlation'] = None
            results['case_stats'] = None

        # 4. Resource-based correlation (if resource column exists)
        resource_col = self._find_resource_column()
        if resource_col:
            resource_performance = self._analyze_resource_performance(resource_col)
            results['resource_performance'] = resource_performance
        else:
            results['resource_performance'] = None

        return results

    def _find_resource_column(self) -> Optional[str]:
        """Find resource column if it exists"""
        possible_names = ['resource', 'org:resource', 'Resource', 'RESOURCE', 'user', 'User']
        for col in self.df.columns:
            if col in possible_names:
                return col
        return None

    def _analyze_resource_performance(self, resource_col: str) -> pd.DataFrame:
        """Analyze performance by resource"""
        df_perf = self.df.copy()
        df_perf = df_perf.sort_values(['case:concept:name', 'time:timestamp'])

        df_perf['next_timestamp'] = df_perf.groupby('case:concept:name')['time:timestamp'].shift(-1)
        df_perf = df_perf.dropna(subset=['next_timestamp'])

        df_perf['duration'] = (df_perf['next_timestamp'] - df_perf['time:timestamp']).dt.total_seconds()

        # Group by resource
        resource_stats = df_perf.groupby(resource_col).agg({
            'duration': ['count', 'mean', 'median', 'std'],
            'case:concept:name': 'nunique'
        }).round(2)
        resource_stats.columns = ['num_activities', 'avg_duration', 'median_duration', 'std_duration', 'num_cases']
        resource_stats = resource_stats.sort_values('num_activities', ascending=False)

        return resource_stats
