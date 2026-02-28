"""
Process analyzer module using pm4py for process mining analysis

This module provides a unified ProcessAnalyzer class that combines all specialized analyzers:
- StatisticalAnalyzer: Summary statistics and distributions
- DFGAnalyzer: Directly-Follows Graph analysis and variants
- PerformanceAnalyzer: Performance metrics and durations
- CorrelationAnalyzer: Correlation analysis between attributes
- QualityAnalyzer: Data quality assessment

The ProcessAnalyzer class maintains backward compatibility with the original monolithic implementation
while providing better organization and maintainability through specialized analyzer modules.
"""

from analysis.base_analyzer import BaseAnalyzer
from analysis.statistical import StatisticalAnalyzer
from analysis.dfg_analyzer import DFGAnalyzer
from analysis.performance import PerformanceAnalyzer
from analysis.correlation import CorrelationAnalyzer
from analysis.quality import QualityAnalyzer

from core.sampling import EventLogSampler
from typing import Optional


class ProcessAnalyzer(
    StatisticalAnalyzer,
    DFGAnalyzer,
    CorrelationAnalyzer,
    QualityAnalyzer
):
    """
    Unified analyzer combining all specialized analyzers with sampling support

    Analyze event logs using pm4py with comprehensive analysis capabilities:
    - Statistical analysis (summary statistics, distributions)
    - Process flow analysis (DFG discovery and filtering)
    - Performance metrics (duration analysis)
    - Correlation analysis (attribute correlations, resource performance)
    - Data quality assessment (completeness, consistency, anomalies)
    - Automatic sampling for large datasets (> 200K events)

    This class inherits from all specialized analyzers to provide a unified interface
    while maintaining backward compatibility with the original ProcessAnalyzer implementation.

    Sampling:
    - Automatic warning for datasets > 200K events
    - Multiple sampling strategies: simple, stratified, systematic
    - Preserves case integrity (never cuts cases)
    - Sample info available via get_sample_info()
    """

    def __init__(self,
                 event_log_df,
                 auto_sample: bool = False,
                 sampling_config: Optional[dict] = None):
        """
        Initialize analyzer with event log and optional sampling

        Args:
            event_log_df: Event log DataFrame with standard column names
                         (case:concept:name, concept:name, time:timestamp)
            auto_sample: Enable automatic sampling for large datasets (default: False)
            sampling_config: Optional dictionary with sampling configuration:
                           - method: 'simple', 'stratified', or 'systematic' (default: 'stratified')
                           - max_events: Maximum events threshold (default: 500000)
                           - max_cases: Maximum cases in sample (default: 50000)
                           - random_state: Random seed (default: 42)
                           - show_warning: Show warning for large datasets (default: True)
        """
        # Store original dataset info
        self.original_num_events = len(event_log_df)
        self.original_num_cases = event_log_df['case:concept:name'].nunique()

        # Initialize sampler
        sampler = EventLogSampler(event_log_df)

        # Get recommendation
        recommendation = sampler.get_recommendation()

        # Initialize sampling config
        if sampling_config is None:
            sampling_config = {}

        show_warning = sampling_config.get('show_warning', True)
        method = sampling_config.get('method', 'stratified')
        max_events = sampling_config.get('max_events', 500_000)
        max_cases = sampling_config.get('max_cases', 50_000)
        random_state = sampling_config.get('random_state', 42)

        # Check if sampling is needed
        self.is_sampled = False
        self.sample_info = None
        self.warning_info = None

        # Mandatory warning for large datasets
        if self.original_num_events >= EventLogSampler.WARNING_THRESHOLD:
            # Store warning info for UI display
            self.warning_info = {
                'num_events': self.original_num_events,
                'num_cases': self.original_num_cases,
                'recommendation': recommendation,
                'show_warning': show_warning
            }

            # Auto-sample if enabled
            if auto_sample and recommendation['should_sample']:
                # Get progress callback if provided in config
                progress_callback = sampling_config.get('progress_callback', None)

                sampled_log = sampler.smart_sample(
                    max_events=max_events,
                    max_cases=max_cases,
                    method=method,
                    random_state=random_state,
                    progress_callback=progress_callback
                )

                self.is_sampled = True
                self.sample_info = sampler.get_sample_info(sampled_log)
                self.sample_info['method'] = method  # Add method to info

                # Generate sampling report
                self.sampling_report = sampler.generate_sampling_report(
                    sampled_log, method, max_cases
                )

                event_log_df = sampled_log
            else:
                self.sampling_report = None
        else:
            self.sampling_report = None

        # Initialize base analyzer (via MRO)
        super().__init__(event_log_df)

    def get_sampling_report(self) -> Optional[dict]:
        """
        Get detailed sampling report if sampling was applied

        Returns:
            Dictionary with comprehensive sampling statistics or None if not sampled
        """
        return self.sampling_report if hasattr(self, 'sampling_report') else None

    def get_sample_info(self) -> Optional[dict]:
        """
        Get information about sampling if applied

        Returns:
            Dictionary with sample information or None if not sampled
        """
        if not self.is_sampled:
            return {
                'is_sampled': False,
                'original_events': self.original_num_events,
                'original_cases': self.original_num_cases,
                'current_events': self.original_num_events,
                'current_cases': self.original_num_cases
            }

        return {
            'is_sampled': True,
            **self.sample_info
        }


# Export all classes for direct import
__all__ = [
    'ProcessAnalyzer',
    'BaseAnalyzer',
    'StatisticalAnalyzer',
    'DFGAnalyzer',
    'PerformanceAnalyzer',
    'CorrelationAnalyzer',
    'QualityAnalyzer'
]
