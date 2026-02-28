"""
Quality analyzer module for data quality assessment
"""
import pandas as pd
from typing import Dict
from difflib import SequenceMatcher
from analysis.base_analyzer import BaseAnalyzer


class QualityAnalyzer(BaseAnalyzer):
    """Analyzer for data quality issues and metrics"""

    def analyze_data_quality(self) -> Dict:
        """
        Analyze data quality issues

        Returns:
            Dictionary with data quality metrics and issues
        """
        results = {
            'total_rows': len(self.df),
            'issues': []
        }

        # 1. Missing values
        missing_values = self.df.isnull().sum()
        missing_pct = (missing_values / len(self.df) * 100).round(2)
        missing_df = pd.DataFrame({
            'column': missing_values.index,
            'missing_count': missing_values.values,
            'missing_percentage': missing_pct.values
        })
        missing_df = missing_df[missing_df['missing_count'] > 0].sort_values('missing_count', ascending=False)
        results['missing_values'] = missing_df

        if len(missing_df) > 0:
            results['issues'].append(f"Found {len(missing_df)} columns with missing values")

        # 2. Duplicate events
        duplicate_rows = self.df.duplicated().sum()
        results['duplicate_events'] = duplicate_rows
        if duplicate_rows > 0:
            results['issues'].append(f"Found {duplicate_rows} duplicate events")

        # 3. Timestamp issues
        timestamp_issues = self._check_timestamp_issues()
        results['timestamp_issues'] = timestamp_issues
        if timestamp_issues['negative_durations'] > 0:
            results['issues'].append(f"Found {timestamp_issues['negative_durations']} cases with negative durations")
        if timestamp_issues['out_of_order'] > 0:
            results['issues'].append(f"Found {timestamp_issues['out_of_order']} events with out-of-order timestamps")

        # 4. Case consistency
        case_consistency = self._check_case_consistency()
        results['case_consistency'] = case_consistency
        if case_consistency['incomplete_cases'] > 0:
            results['issues'].append(f"Found {case_consistency['incomplete_cases']} potentially incomplete cases")

        # 5. Activity name consistency
        activity_issues = self._check_activity_consistency()
        results['activity_consistency'] = activity_issues
        if activity_issues['potential_typos']:
            results['issues'].append(f"Found {len(activity_issues['potential_typos'])} potential activity name typos")

        # 6. Value distribution anomalies
        anomalies = self._detect_value_anomalies()
        results['value_anomalies'] = anomalies
        if anomalies['outlier_cases']:
            results['issues'].append(f"Found {len(anomalies['outlier_cases'])} cases with anomalous durations")

        # Overall quality score (0-100)
        quality_score = self._calculate_quality_score(results)
        results['quality_score'] = quality_score

        return results

    def _check_timestamp_issues(self) -> Dict:
        """Check for timestamp-related issues"""
        issues = {
            'negative_durations': 0,
            'out_of_order': 0,
            'problematic_cases': []
        }

        for case_id, group in self.df.groupby('case:concept:name'):
            sorted_group = group.sort_values('time:timestamp')

            # Check if timestamps are in order
            if not sorted_group['time:timestamp'].is_monotonic_increasing:
                issues['out_of_order'] += len(group)
                issues['problematic_cases'].append(str(case_id))

            # Check for negative durations
            duration = (sorted_group['time:timestamp'].max() - sorted_group['time:timestamp'].min()).total_seconds()
            if duration < 0:
                issues['negative_durations'] += 1
                if str(case_id) not in issues['problematic_cases']:
                    issues['problematic_cases'].append(str(case_id))

        return issues

    def _check_case_consistency(self) -> Dict:
        """Check case-level consistency"""
        consistency = {
            'incomplete_cases': 0,
            'suspicious_cases': []
        }

        # Get case statistics
        case_events = self.df.groupby('case:concept:name').size()
        mean_events = case_events.mean()
        std_events = case_events.std()

        # Cases with very few events (less than 2)
        incomplete = case_events[case_events < 2]
        consistency['incomplete_cases'] = len(incomplete)

        # Cases with unusual number of events (more than 2 std from mean)
        if std_events > 0:
            threshold = mean_events - 2 * std_events
            suspicious = case_events[case_events < threshold]
            consistency['suspicious_cases'] = [str(case_id) for case_id in suspicious.index[:10]]  # Limit to 10

        return consistency

    def _check_activity_consistency(self) -> Dict:
        """Check activity name consistency"""
        consistency = {
            'unique_activities': 0,
            'potential_typos': []
        }

        activities = self.df['concept:name'].unique()
        consistency['unique_activities'] = len(activities)

        # Look for similar activity names (potential typos)
        checked = set()
        for i, act1 in enumerate(activities):
            for act2 in activities[i+1:]:
                if (act1, act2) not in checked and (act2, act1) not in checked:
                    similarity = SequenceMatcher(None, act1.lower(), act2.lower()).ratio()
                    if 0.7 < similarity < 1.0:  # Similar but not identical
                        consistency['potential_typos'].append((act1, act2, round(similarity, 2)))
                    checked.add((act1, act2))

        # Sort by similarity
        consistency['potential_typos'].sort(key=lambda x: x[2], reverse=True)
        consistency['potential_typos'] = consistency['potential_typos'][:10]  # Limit to top 10

        return consistency

    def _detect_value_anomalies(self) -> Dict:
        """Detect anomalous values in durations"""
        anomalies = {
            'outlier_cases': [],
            'zero_duration_cases': 0
        }

        case_durations = self._calculate_case_durations()

        # Detect outliers using IQR method
        Q1 = case_durations.quantile(0.25)
        Q3 = case_durations.quantile(0.75)
        IQR = Q3 - Q1

        lower_bound = Q1 - 3 * IQR
        upper_bound = Q3 + 3 * IQR

        outliers = case_durations[(case_durations < lower_bound) | (case_durations > upper_bound)]
        anomalies['outlier_cases'] = [str(case_id) for case_id in outliers.index[:10]]  # Limit to 10

        # Zero duration cases
        zero_duration = case_durations[case_durations == 0]
        anomalies['zero_duration_cases'] = len(zero_duration)

        return anomalies

    def _calculate_quality_score(self, results: Dict) -> float:
        """Calculate overall quality score (0-100)"""
        score = 100.0
        total_rows = results['total_rows']

        # Deduct points for missing values
        if not results['missing_values'].empty:
            missing_ratio = results['missing_values']['missing_count'].sum() / (total_rows * len(self.df.columns))
            score -= min(missing_ratio * 100, 20)  # Max 20 points deduction

        # Deduct points for duplicates
        if results['duplicate_events'] > 0:
            duplicate_ratio = results['duplicate_events'] / total_rows
            score -= min(duplicate_ratio * 100, 15)  # Max 15 points deduction

        # Deduct points for timestamp issues
        ts_issues = results['timestamp_issues']
        if ts_issues['negative_durations'] > 0 or ts_issues['out_of_order'] > 0:
            total_ts_issues = ts_issues['negative_durations'] + ts_issues['out_of_order']
            ts_ratio = total_ts_issues / total_rows
            score -= min(ts_ratio * 100, 25)  # Max 25 points deduction

        # Deduct points for case consistency issues
        case_issues = results['case_consistency']
        total_cases = self.df['case:concept:name'].nunique()
        if case_issues['incomplete_cases'] > 0:
            incomplete_ratio = case_issues['incomplete_cases'] / total_cases
            score -= min(incomplete_ratio * 100, 15)  # Max 15 points deduction

        # Deduct points for activity inconsistencies
        if results['activity_consistency']['potential_typos']:
            score -= min(len(results['activity_consistency']['potential_typos']) * 2, 10)  # Max 10 points

        # Deduct points for anomalies
        if results['value_anomalies']['outlier_cases']:
            outlier_ratio = len(results['value_anomalies']['outlier_cases']) / total_cases
            score -= min(outlier_ratio * 100, 15)  # Max 15 points deduction

        return max(0, round(score, 2))
