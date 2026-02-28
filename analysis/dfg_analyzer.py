"""
DFG (Directly-Follows Graph) analyzer module for process flow analysis
"""
import pandas as pd
from typing import Dict, Tuple
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
from pm4py.algo.filtering.dfg import dfg_filtering
from pm4py.statistics.start_activities.log import get as start_activities_module
from pm4py.statistics.end_activities.log import get as end_activities_module
from analysis.base_analyzer import BaseAnalyzer


class DFGAnalyzer(BaseAnalyzer):
    """Analyzer for Directly-Follows Graphs and process variants"""

    def discover_dfg(self) -> Tuple[Dict, Dict, Dict]:
        """
        Discover Directly-Follows Graph

        Returns:
            Tuple of (dfg, start_activities, end_activities)
        """
        # Compute DFG
        dfg = dfg_discovery.apply(self.log)
        start_activities = start_activities_module.get_start_activities(self.log)
        end_activities = end_activities_module.get_end_activities(self.log)

        result = (dfg, start_activities, end_activities)

        return result

    def filter_dfg_by_frequency(
        self,
        dfg: Dict,
        start_activities: Dict,
        end_activities: Dict,
        percentage: float
    ) -> Tuple[Dict, Dict, Dict]:
        """
        Filter DFG by keeping only most frequent paths

        Args:
            dfg: Original DFG
            start_activities: Start activities
            end_activities: End activities
            percentage: Percentage of paths to keep (0.0-1.0)

        Returns:
            Filtered (dfg, start_activities, end_activities)
        """
        # Check if input DFG is empty
        if not dfg or len(dfg) == 0:
            return {}, {}, {}

        # Calculate activities count
        activities_count = {}
        for activity in self.df['concept:name'].unique():
            activities_count[activity] = len(self.df[self.df['concept:name'] == activity])

        # Check if activities_count is empty
        if not activities_count:
            return {}, {}, {}

        try:
            # Filter DFG with all required parameters
            # The function returns a tuple of 4 elements:
            # (dfg, start_activities, end_activities, activities_count)
            result = dfg_filtering.filter_dfg_on_paths_percentage(
                dfg,
                start_activities,
                end_activities,
                activities_count,
                percentage
            )

            # Check if result is a tuple or just a dict
            if isinstance(result, tuple):
                # Handle different tuple lengths for compatibility
                if len(result) == 4:
                    # New version: returns (dfg, start, end, activities_count)
                    filtered_dfg, filtered_start, filtered_end, _ = result
                elif len(result) == 3:
                    # Older version: returns (dfg, start, end)
                    filtered_dfg, filtered_start, filtered_end = result
                else:
                    # Unknown format, try to extract what we can
                    filtered_dfg = result[0]
                    filtered_start = result[1] if len(result) > 1 else start_activities
                    filtered_end = result[2] if len(result) > 2 else end_activities
            else:
                # Fallback: if it returns just the DFG, filter manually
                filtered_dfg = result
                activities_in_dfg = set()
                for (act1, act2) in filtered_dfg.keys():
                    activities_in_dfg.add(act1)
                    activities_in_dfg.add(act2)

                filtered_start = {k: v for k, v in start_activities.items() if k in activities_in_dfg}
                filtered_end = {k: v for k, v in end_activities.items() if k in activities_in_dfg}

            return filtered_dfg, filtered_start, filtered_end

        except (ValueError, IndexError) as e:
            # Handle errors from pm4py filtering (e.g., "min() iterable argument is empty")
            # Return empty results if filtering fails
            if "min()" in str(e) or "empty" in str(e):
                return {}, {}, {}
            # Re-raise other errors
            raise

    def get_dfg_statistics(self, dfg: Dict) -> pd.DataFrame:
        """
        Get statistics about DFG edges

        Args:
            dfg: DFG dictionary

        Returns:
            DataFrame with edge statistics
        """
        edges = []
        for (source, target), frequency in dfg.items():
            edges.append({
                'source': source,
                'target': target,
                'frequency': frequency
            })

        df_edges = pd.DataFrame(edges)
        df_edges = df_edges.sort_values('frequency', ascending=False)

        # Calculate percentages
        total_freq = df_edges['frequency'].sum()
        df_edges['percentage'] = (df_edges['frequency'] / total_freq * 100).round(2)

        return df_edges

    def get_variant_statistics(self) -> pd.DataFrame:
        """
        Get process variant statistics

        Returns:
            DataFrame with variants and their frequencies
        """
        # Group by case to get sequences
        case_sequences = self.df.groupby('case:concept:name')['concept:name'].apply(
            lambda x: ' -> '.join(x)
        )

        # Count variants
        variant_counts = case_sequences.value_counts().reset_index()
        variant_counts.columns = ['variant', 'count']

        # Calculate percentages
        variant_counts['percentage'] = (
            variant_counts['count'] / variant_counts['count'].sum() * 100
        ).round(2)

        # Add cumulative percentage
        variant_counts['cumulative_percentage'] = variant_counts['percentage'].cumsum().round(2)

        return variant_counts

    def get_start_activities(self) -> pd.DataFrame:
        """Get start activities distribution"""
        start_acts = start_activities_module.get_start_activities(self.log)
        df = pd.DataFrame(list(start_acts.items()), columns=['activity', 'count'])
        df = df.sort_values('count', ascending=False)
        df['percentage'] = (df['count'] / df['count'].sum() * 100).round(2)
        return df

    def get_end_activities(self) -> pd.DataFrame:
        """Get end activities distribution"""
        end_acts = end_activities_module.get_end_activities(self.log)
        df = pd.DataFrame(list(end_acts.items()), columns=['activity', 'count'])
        df = df.sort_values('count', ascending=False)
        df['percentage'] = (df['count'] / df['count'].sum() * 100).round(2)
        return df
