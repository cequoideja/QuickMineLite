"""
Sampling utilities for large event logs

This module provides various sampling strategies for process mining analysis:
- Simple case-based sampling
- Stratified sampling by variant
- Systematic sampling
- Time-based sampling

All sampling methods preserve case integrity (never cut cases in the middle).
"""
import pandas as pd
import numpy as np
from typing import Optional, Literal, Dict, Any
import warnings


class EventLogSampler:
    """Sampler for event logs with various sampling strategies"""

    # Thresholds for automatic warnings
    WARNING_THRESHOLD = 200_000  # Events
    CRITICAL_THRESHOLD = 1_000_000  # Events

    def __init__(self, event_log: pd.DataFrame):
        """
        Initialize sampler with event log

        Args:
            event_log: Event log DataFrame with standard columns
        """
        self.event_log = event_log
        self.num_events = len(event_log)
        self.num_cases = event_log['case:concept:name'].nunique()

    def should_sample(self, threshold: int = WARNING_THRESHOLD) -> bool:
        """
        Check if sampling is recommended

        Args:
            threshold: Number of events threshold

        Returns:
            True if sampling is recommended
        """
        return self.num_events > threshold

    def detect_optimal_method(self) -> str:
        """
        Automatically detect the optimal sampling method based on dataset characteristics

        Returns:
            Recommended sampling method: 'stratified', 'simple', or 'systematic'
        """
        # Calculate variant diversity
        case_variants = self.event_log.groupby('case:concept:name')['concept:name'].apply(
            lambda x: ' -> '.join(x)
        )
        num_variants = case_variants.nunique()
        variant_concentration = num_variants / self.num_cases if self.num_cases > 0 else 0

        # Calculate average events per case
        avg_events_per_case = self.num_events / self.num_cases if self.num_cases > 0 else 0

        # Decision logic
        # High variant diversity (> 30% unique variants) -> stratified is best
        if variant_concentration > 0.3:
            return 'stratified'

        # Many variants but lower concentration -> stratified still good
        if num_variants > 100:
            return 'stratified'

        # Very uniform process (< 5% unique variants) and regular pattern -> systematic
        if variant_concentration < 0.05 and avg_events_per_case > 5:
            return 'systematic'

        # Low variant diversity and small dataset -> simple random is fine
        if num_variants < 50 and self.num_cases < 10_000:
            return 'simple'

        # Default to stratified (safest choice)
        return 'stratified'

    def calculate_adaptive_max_cases(self) -> int:
        """
        Automatically calculate optimal max_cases based on dataset size and characteristics

        Returns:
            Recommended maximum number of cases for sampling
        """
        # Base calculation on dataset size
        if self.num_events <= 200_000:
            # No sampling needed
            return self.num_cases
        elif self.num_events <= 500_000:
            # Keep 20-30% of cases, minimum 10k, maximum 50k
            target = int(self.num_cases * 0.25)
            return max(10_000, min(50_000, target))
        elif self.num_events <= 1_000_000:
            # Keep 10-20% of cases, minimum 20k, maximum 50k
            target = int(self.num_cases * 0.15)
            return max(20_000, min(50_000, target))
        elif self.num_events <= 5_000_000:
            # Keep 5-10% of cases, minimum 30k, maximum 100k
            target = int(self.num_cases * 0.08)
            return max(30_000, min(100_000, target))
        else:
            # Very large datasets: keep 2-5%, minimum 50k, maximum 150k
            target = int(self.num_cases * 0.03)
            return max(50_000, min(150_000, target))

    def get_recommendation(self) -> Dict[str, Any]:
        """
        Get sampling recommendation based on dataset size

        Returns:
            Dictionary with recommendation details
        """
        if self.num_events <= 100_000:
            return {
                'should_sample': False,
                'reason': 'Dataset size is optimal',
                'recommended_size': self.num_cases,
                'recommended_method': self.detect_optimal_method(),
                'performance': 'excellent'
            }
        elif self.num_events < self.WARNING_THRESHOLD:
            return {
                'should_sample': False,
                'reason': 'Dataset size is acceptable',
                'recommended_size': self.num_cases,
                'recommended_method': self.detect_optimal_method(),
                'performance': 'good',
                'note': 'Consider sampling for faster analysis'
            }
        elif self.num_events <= 500_000:
            recommended_cases = self.calculate_adaptive_max_cases()
            return {
                'should_sample': True,
                'reason': f'Large dataset ({self.num_events:,} events)',
                'recommended_size': recommended_cases,
                'recommended_method': self.detect_optimal_method(),
                'performance': 'slow without sampling',
                'expected_reduction': f"{(1 - recommended_cases / self.num_cases) * 100:.0f}%"
            }
        elif self.num_events <= self.CRITICAL_THRESHOLD:
            recommended_cases = self.calculate_adaptive_max_cases()
            return {
                'should_sample': True,
                'reason': f'Very large dataset ({self.num_events:,} events)',
                'recommended_size': recommended_cases,
                'recommended_method': self.detect_optimal_method(),
                'performance': 'very slow without sampling',
                'warning': 'Sampling strongly recommended',
                'expected_reduction': f"{(1 - recommended_cases / self.num_cases) * 100:.0f}%"
            }
        else:
            recommended_cases = self.calculate_adaptive_max_cases()
            return {
                'should_sample': True,
                'reason': f'Extremely large dataset ({self.num_events:,} events)',
                'recommended_size': recommended_cases,
                'recommended_method': self.detect_optimal_method(),
                'performance': 'critical - sampling required',
                'warning': 'CRITICAL: Dataset too large for efficient analysis',
                'expected_reduction': f"{(1 - recommended_cases / self.num_cases) * 100:.0f}%"
            }

    def sample_by_cases(self,
                       target_cases: Optional[int] = None,
                       sample_ratio: Optional[float] = None,
                       random_state: int = 42) -> pd.DataFrame:
        """
        Simple case-based sampling - preserves complete cases

        Args:
            target_cases: Number of cases to sample (mutually exclusive with sample_ratio)
            sample_ratio: Ratio of cases to sample (0.0-1.0)
            random_state: Random seed for reproducibility

        Returns:
            Sampled DataFrame with complete cases
        """
        if target_cases is None and sample_ratio is None:
            raise ValueError("Either target_cases or sample_ratio must be specified")

        if target_cases is not None and sample_ratio is not None:
            raise ValueError("Only one of target_cases or sample_ratio should be specified")

        # Calculate target
        if sample_ratio is not None:
            target_cases = int(self.num_cases * sample_ratio)

        target_cases = min(target_cases, self.num_cases)

        # Get all cases
        all_cases = self.event_log['case:concept:name'].unique()

        # Sample cases
        np.random.seed(random_state)
        sampled_cases = np.random.choice(all_cases, size=target_cases, replace=False)

        # Filter log to keep only sampled cases
        sampled_log = self.event_log[
            self.event_log['case:concept:name'].isin(sampled_cases)
        ].copy()

        return sampled_log

    def sample_stratified(self,
                         target_cases: int,
                         random_state: int = 42,
                         progress_callback=None) -> pd.DataFrame:
        """
        Stratified sampling by process variant - preserves variant distribution

        Args:
            target_cases: Number of cases to sample
            random_state: Random seed for reproducibility
            progress_callback: Optional callback function(message) for progress updates

        Returns:
            Sampled DataFrame with preserved variant distribution
        """
        target_cases = min(target_cases, self.num_cases)

        # Calculate variants for each case
        if progress_callback:
            progress_callback(f"Calculating variants for {self.num_cases:,} cases...")

        case_variants = self.event_log.groupby('case:concept:name')['concept:name'].apply(
            lambda x: ' -> '.join(x)
        )

        # Create DataFrame with case and variant
        if progress_callback:
            progress_callback(f"Processing {len(case_variants):,} variants...")

        case_variant_df = case_variants.reset_index()
        case_variant_df.columns = ['case', 'variant']

        # Calculate proportion of each variant
        variant_counts = case_variant_df['variant'].value_counts()
        variant_proportions = variant_counts / variant_counts.sum()

        if progress_callback:
            progress_callback(f"Sampling from {len(variant_counts):,} unique variants...")

        # Sample proportionally from each variant
        np.random.seed(random_state)
        sampled_cases = []

        for variant, proportion in variant_proportions.items():
            # Number of cases to sample for this variant
            n_cases = int(target_cases * proportion)

            # Ensure at least 1 case for variants that should be represented
            if n_cases == 0 and proportion > 0 and len(sampled_cases) < target_cases:
                n_cases = 1

            if n_cases > 0:
                variant_cases = case_variant_df[
                    case_variant_df['variant'] == variant
                ]['case'].values

                sample_size = min(n_cases, len(variant_cases))
                sampled = np.random.choice(variant_cases, size=sample_size, replace=False)
                sampled_cases.extend(sampled)

        # Adjust if we oversampled
        if len(sampled_cases) > target_cases:
            sampled_cases = np.random.choice(sampled_cases, size=target_cases, replace=False)

        # Filter log
        sampled_log = self.event_log[
            self.event_log['case:concept:name'].isin(sampled_cases)
        ].copy()

        return sampled_log

    def sample_systematic(self,
                         sample_ratio: float,
                         random_state: int = 42) -> pd.DataFrame:
        """
        Systematic sampling - takes every Nth case

        Args:
            sample_ratio: Ratio of cases to sample (0.0-1.0)
            random_state: Random seed for starting point

        Returns:
            Sampled DataFrame
        """
        if not 0 < sample_ratio <= 1:
            raise ValueError("sample_ratio must be between 0 and 1")

        all_cases = self.event_log['case:concept:name'].unique()

        # Calculate step
        step = int(1 / sample_ratio)

        # Random starting point
        np.random.seed(random_state)
        start = np.random.randint(0, min(step, len(all_cases)))

        # Take every Nth case
        sampled_cases = all_cases[start::step]

        # Filter log
        sampled_log = self.event_log[
            self.event_log['case:concept:name'].isin(sampled_cases)
        ].copy()

        return sampled_log

    def sample_time_period(self,
                          start_date: Optional[pd.Timestamp] = None,
                          end_date: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        """
        Sample by time period

        Args:
            start_date: Start of period (inclusive)
            end_date: End of period (inclusive)

        Returns:
            Filtered DataFrame for the time period
        """
        filtered = self.event_log.copy()

        if start_date is not None:
            filtered = filtered[filtered['time:timestamp'] >= start_date]

        if end_date is not None:
            filtered = filtered[filtered['time:timestamp'] <= end_date]

        return filtered

    def smart_sample(self,
                    max_events: int = 500_000,
                    max_cases: int = 50_000,
                    method: Literal['simple', 'stratified', 'systematic'] = 'stratified',
                    random_state: int = 42,
                    progress_callback=None) -> pd.DataFrame:
        """
        Intelligent sampling with automatic size determination

        Args:
            max_events: Maximum number of events in sample
            max_cases: Maximum number of cases in sample
            method: Sampling method to use
            random_state: Random seed for reproducibility
            progress_callback: Optional callback function(message) for progress updates

        Returns:
            Sampled DataFrame
        """
        # Check if sampling is needed
        if self.num_events <= max_events and self.num_cases <= max_cases:
            return self.event_log

        # Calculate target size
        target_cases = min(max_cases, self.num_cases)

        # Apply sampling method
        if method == 'stratified':
            return self.sample_stratified(target_cases, random_state, progress_callback)
        elif method == 'systematic':
            sample_ratio = target_cases / self.num_cases
            return self.sample_systematic(sample_ratio, random_state)
        else:  # simple
            return self.sample_by_cases(target_cases=target_cases, random_state=random_state)

    def get_sample_info(self, sampled_log: pd.DataFrame) -> Dict[str, Any]:
        """
        Get information about a sample

        Args:
            sampled_log: Sampled DataFrame

        Returns:
            Dictionary with sample statistics
        """
        sampled_events = len(sampled_log)
        sampled_cases = sampled_log['case:concept:name'].nunique()

        return {
            'original_events': self.num_events,
            'original_cases': self.num_cases,
            'sampled_events': sampled_events,
            'sampled_cases': sampled_cases,
            'events_reduction': f"{(1 - sampled_events / self.num_events) * 100:.1f}%",
            'cases_reduction': f"{(1 - sampled_cases / self.num_cases) * 100:.1f}%",
            'sample_ratio': sampled_cases / self.num_cases
        }

    def generate_sampling_report(self, sampled_log: pd.DataFrame, method: str,
                                max_cases: int) -> Dict[str, Any]:
        """
        Generate comprehensive sampling report with statistics

        Args:
            sampled_log: Sampled DataFrame
            method: Sampling method used
            max_cases: Maximum cases parameter used

        Returns:
            Dictionary with detailed sampling report
        """
        sampled_events = len(sampled_log)
        sampled_cases = sampled_log['case:concept:name'].nunique()

        # Calculate variant preservation
        original_variants = self.event_log.groupby('case:concept:name')['concept:name'].apply(
            lambda x: ' -> '.join(x)
        ).unique()
        sampled_variants = sampled_log.groupby('case:concept:name')['concept:name'].apply(
            lambda x: ' -> '.join(x)
        ).unique()

        num_original_variants = len(original_variants)
        num_sampled_variants = len(sampled_variants)
        variant_preservation = (num_sampled_variants / num_original_variants * 100) if num_original_variants > 0 else 0

        # Calculate activity preservation
        original_activities = self.event_log['concept:name'].unique()
        sampled_activities = sampled_log['concept:name'].unique()
        num_original_activities = len(original_activities)
        num_sampled_activities = len(sampled_activities)
        activity_preservation = (num_sampled_activities / num_original_activities * 100) if num_original_activities > 0 else 0

        # Calculate time span preservation
        original_start = self.event_log['time:timestamp'].min()
        original_end = self.event_log['time:timestamp'].max()
        sampled_start = sampled_log['time:timestamp'].min()
        sampled_end = sampled_log['time:timestamp'].max()
        original_duration = (original_end - original_start).total_seconds() / 86400  # days
        sampled_duration = (sampled_end - sampled_start).total_seconds() / 86400  # days

        # Calculate average case length
        original_avg_length = self.event_log.groupby('case:concept:name').size().mean()
        sampled_avg_length = sampled_log.groupby('case:concept:name').size().mean()

        # Detect optimal settings
        optimal_method = self.detect_optimal_method()
        adaptive_max_cases = self.calculate_adaptive_max_cases()

        report = {
            'sampling_metadata': {
                'method_used': method,
                'max_cases_parameter': max_cases,
                'optimal_method_detected': optimal_method,
                'adaptive_max_cases_recommended': adaptive_max_cases,
                'timestamp': pd.Timestamp.now().isoformat()
            },
            'dataset_statistics': {
                'original': {
                    'events': self.num_events,
                    'cases': self.num_cases,
                    'variants': num_original_variants,
                    'activities': num_original_activities,
                    'avg_case_length': round(original_avg_length, 2),
                    'time_span_days': round(original_duration, 2),
                    'start_date': original_start.isoformat(),
                    'end_date': original_end.isoformat()
                },
                'sampled': {
                    'events': sampled_events,
                    'cases': sampled_cases,
                    'variants': num_sampled_variants,
                    'activities': num_sampled_activities,
                    'avg_case_length': round(sampled_avg_length, 2),
                    'time_span_days': round(sampled_duration, 2),
                    'start_date': sampled_start.isoformat(),
                    'end_date': sampled_end.isoformat()
                }
            },
            'reduction_metrics': {
                'events_reduction_pct': round((1 - sampled_events / self.num_events) * 100, 2),
                'cases_reduction_pct': round((1 - sampled_cases / self.num_cases) * 100, 2),
                'events_kept': sampled_events,
                'cases_kept': sampled_cases,
                'sample_ratio': round(sampled_cases / self.num_cases, 4)
            },
            'preservation_metrics': {
                'variant_preservation_pct': round(variant_preservation, 2),
                'activity_preservation_pct': round(activity_preservation, 2),
                'variants_preserved': num_sampled_variants,
                'variants_lost': num_original_variants - num_sampled_variants,
                'activities_preserved': num_sampled_activities,
                'activities_lost': num_original_activities - num_sampled_activities
            },
            'quality_indicators': {
                'avg_case_length_similarity': round((sampled_avg_length / original_avg_length * 100) if original_avg_length > 0 else 100, 2),
                'time_span_coverage_pct': round((sampled_duration / original_duration * 100) if original_duration > 0 else 100, 2),
                'overall_quality_score': self._calculate_quality_score(
                    variant_preservation, activity_preservation,
                    sampled_avg_length / original_avg_length if original_avg_length > 0 else 1
                )
            }
        }

        return report

    def _calculate_quality_score(self, variant_pres: float, activity_pres: float,
                                 case_length_ratio: float) -> str:
        """
        Calculate overall quality score based on preservation metrics

        Args:
            variant_pres: Variant preservation percentage
            activity_pres: Activity preservation percentage
            case_length_ratio: Ratio of sampled to original average case length

        Returns:
            Quality score: 'excellent', 'good', 'fair', or 'poor'
        """
        # Weighted score: variants (50%), activities (30%), case length (20%)
        score = (variant_pres * 0.5 + activity_pres * 0.3 +
                min(case_length_ratio, 1.0) * 100 * 0.2)

        if score >= 95:
            return 'excellent'
        elif score >= 85:
            return 'good'
        elif score >= 70:
            return 'fair'
        else:
            return 'poor'


def print_sampling_warning(num_events: int, num_cases: int, recommendation: Dict[str, Any]):
    """
    Print formatted warning about large dataset

    Args:
        num_events: Number of events in dataset
        num_cases: Number of cases in dataset
        recommendation: Recommendation dictionary from get_recommendation()
    """
    print("\n" + "=" * 70)
    print("LARGE DATASET WARNING")
    print("=" * 70)
    print(f"Dataset size: {num_events:,} events, {num_cases:,} cases")
    print(f"Reason: {recommendation['reason']}")

    if 'warning' in recommendation:
        print(f"\nWARNING: {recommendation['warning']}")

    print(f"\nRecommendation:")
    print(f"  - Should sample: {'YES' if recommendation['should_sample'] else 'NO'}")

    if recommendation['should_sample']:
        print(f"  - Recommended method: {recommendation.get('recommended_method', 'simple')}")
        print(f"  - Recommended size: {recommendation['recommended_size']:,} cases")
        print(f"  - Expected reduction: {recommendation.get('expected_reduction', 'N/A')}")
        print(f"  - Performance: {recommendation['performance']}")

    if 'note' in recommendation:
        print(f"\nNote: {recommendation['note']}")

    print("=" * 70 + "\n")


def validate_sampling_params(target_cases: Optional[int] = None,
                            sample_ratio: Optional[float] = None,
                            method: Optional[str] = None) -> None:
    """
    Validate sampling parameters

    Args:
        target_cases: Target number of cases
        sample_ratio: Sample ratio
        method: Sampling method

    Raises:
        ValueError: If parameters are invalid
    """
    if target_cases is not None and target_cases <= 0:
        raise ValueError("target_cases must be positive")

    if sample_ratio is not None and not 0 < sample_ratio <= 1:
        raise ValueError("sample_ratio must be between 0 and 1")

    if method is not None:
        valid_methods = ['simple', 'stratified', 'systematic', 'time']
        if method not in valid_methods:
            raise ValueError(f"method must be one of {valid_methods}")


def export_sampling_report_json(report: Dict[str, Any], file_path: str) -> bool:
    """
    Export sampling report to JSON file

    Args:
        report: Sampling report dictionary from generate_sampling_report()
        file_path: Path to save JSON file

    Returns:
        True if successful, False otherwise
    """
    import json
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"Error exporting sampling report to JSON: {e}")
        return False


def export_sampling_report_text(report: Dict[str, Any], file_path: str) -> bool:
    """
    Export sampling report to human-readable text file

    Args:
        report: Sampling report dictionary from generate_sampling_report()
        file_path: Path to save text file

    Returns:
        True if successful, False otherwise
    """
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("SAMPLING REPORT\n")
            f.write("=" * 80 + "\n\n")

            # Metadata section
            f.write("SAMPLING METADATA\n")
            f.write("-" * 80 + "\n")
            meta = report['sampling_metadata']
            f.write(f"Method used: {meta['method_used']}\n")
            f.write(f"Max cases parameter: {meta['max_cases_parameter']:,}\n")
            f.write(f"Optimal method detected: {meta['optimal_method_detected']}\n")
            f.write(f"Adaptive max cases recommended: {meta['adaptive_max_cases_recommended']:,}\n")
            f.write(f"Timestamp: {meta['timestamp']}\n\n")

            # Dataset statistics
            f.write("DATASET STATISTICS\n")
            f.write("-" * 80 + "\n")
            orig = report['dataset_statistics']['original']
            samp = report['dataset_statistics']['sampled']

            f.write("Original Dataset:\n")
            f.write(f"  Events: {orig['events']:,}\n")
            f.write(f"  Cases: {orig['cases']:,}\n")
            f.write(f"  Variants: {orig['variants']:,}\n")
            f.write(f"  Activities: {orig['activities']:,}\n")
            f.write(f"  Average case length: {orig['avg_case_length']}\n")
            f.write(f"  Time span: {orig['time_span_days']:.2f} days\n")
            f.write(f"  Date range: {orig['start_date']} to {orig['end_date']}\n\n")

            f.write("Sampled Dataset:\n")
            f.write(f"  Events: {samp['events']:,}\n")
            f.write(f"  Cases: {samp['cases']:,}\n")
            f.write(f"  Variants: {samp['variants']:,}\n")
            f.write(f"  Activities: {samp['activities']:,}\n")
            f.write(f"  Average case length: {samp['avg_case_length']}\n")
            f.write(f"  Time span: {samp['time_span_days']:.2f} days\n")
            f.write(f"  Date range: {samp['start_date']} to {samp['end_date']}\n\n")

            # Reduction metrics
            f.write("REDUCTION METRICS\n")
            f.write("-" * 80 + "\n")
            red = report['reduction_metrics']
            f.write(f"Events reduction: {red['events_reduction_pct']:.2f}%\n")
            f.write(f"Cases reduction: {red['cases_reduction_pct']:.2f}%\n")
            f.write(f"Events kept: {red['events_kept']:,} / {orig['events']:,}\n")
            f.write(f"Cases kept: {red['cases_kept']:,} / {orig['cases']:,}\n")
            f.write(f"Sample ratio: {red['sample_ratio']:.4f}\n\n")

            # Preservation metrics
            f.write("PRESERVATION METRICS\n")
            f.write("-" * 80 + "\n")
            pres = report['preservation_metrics']
            f.write(f"Variant preservation: {pres['variant_preservation_pct']:.2f}%\n")
            f.write(f"  Preserved: {pres['variants_preserved']:,}\n")
            f.write(f"  Lost: {pres['variants_lost']:,}\n")
            f.write(f"Activity preservation: {pres['activity_preservation_pct']:.2f}%\n")
            f.write(f"  Preserved: {pres['activities_preserved']:,}\n")
            f.write(f"  Lost: {pres['activities_lost']:,}\n\n")

            # Quality indicators
            f.write("QUALITY INDICATORS\n")
            f.write("-" * 80 + "\n")
            qual = report['quality_indicators']
            f.write(f"Average case length similarity: {qual['avg_case_length_similarity']:.2f}%\n")
            f.write(f"Time span coverage: {qual['time_span_coverage_pct']:.2f}%\n")
            f.write(f"Overall quality score: {qual['overall_quality_score'].upper()}\n\n")

            f.write("=" * 80 + "\n")
            f.write("END OF REPORT\n")
            f.write("=" * 80 + "\n")

        return True
    except Exception as e:
        print(f"Error exporting sampling report to text: {e}")
        return False


def print_sampling_report_summary(report: Dict[str, Any]) -> None:
    """
    Print a concise summary of the sampling report to console

    Args:
        report: Sampling report dictionary from generate_sampling_report()
    """
    print("\n" + "=" * 70)
    print("SAMPLING REPORT SUMMARY")
    print("=" * 70)

    meta = report['sampling_metadata']
    print(f"\nMethod: {meta['method_used']} (Optimal: {meta['optimal_method_detected']})")

    orig = report['dataset_statistics']['original']
    samp = report['dataset_statistics']['sampled']
    print(f"\nDataset Size:")
    print(f"  {orig['events']:,} -> {samp['events']:,} events ({report['reduction_metrics']['events_reduction_pct']:.1f}% reduction)")
    print(f"  {orig['cases']:,} -> {samp['cases']:,} cases ({report['reduction_metrics']['cases_reduction_pct']:.1f}% reduction)")

    pres = report['preservation_metrics']
    print(f"\nPreservation:")
    print(f"  Variants: {pres['variant_preservation_pct']:.1f}% ({pres['variants_preserved']}/{orig['variants']})")
    print(f"  Activities: {pres['activity_preservation_pct']:.1f}% ({pres['activities_preserved']}/{orig['activities']})")

    qual = report['quality_indicators']
    print(f"\nQuality Score: {qual['overall_quality_score'].upper()}")

    print("=" * 70 + "\n")
