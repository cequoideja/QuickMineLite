"""
Filter Engine - Unified filtering for event log data

Combines filter management (FilterCriteria, TimeFilter, FilterManager) with
filter strategy pattern (FilterStrategy classes and FilterStrategyFactory).

Adapted from the PyQt6 app's filter_manager.py and filter_strategies.py,
with all PyQt dependencies removed.
"""
import pandas as pd
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Set
from datetime import datetime


# =============================================================================
# Filter Strategy Pattern - Concrete implementations for different operators
# =============================================================================

class FilterStrategy(ABC):
    """Abstract base class for filter strategies"""

    @abstractmethod
    def apply(self, series: pd.Series, value: Any) -> pd.Series:
        """
        Apply the filter strategy to a pandas Series

        Args:
            series: The pandas Series to filter
            value: The value to compare against

        Returns:
            Boolean Series indicating which rows match the filter
        """
        pass

    @abstractmethod
    def get_operator_name(self) -> str:
        """Get the operator name for this strategy"""
        pass


class EqualsStrategy(FilterStrategy):
    """Strategy for equality comparison"""

    def apply(self, series: pd.Series, value: Any) -> pd.Series:
        return series == value

    def get_operator_name(self) -> str:
        return 'equals'


class NotEqualsStrategy(FilterStrategy):
    """Strategy for inequality comparison"""

    def apply(self, series: pd.Series, value: Any) -> pd.Series:
        return series != value

    def get_operator_name(self) -> str:
        return 'not_equals'


class ContainsStrategy(FilterStrategy):
    """Strategy for substring matching (case-insensitive)"""

    def apply(self, series: pd.Series, value: Any) -> pd.Series:
        return series.astype(str).str.contains(str(value), case=False, na=False)

    def get_operator_name(self) -> str:
        return 'contains'


class NotContainsStrategy(FilterStrategy):
    """Strategy for negative substring matching"""

    def apply(self, series: pd.Series, value: Any) -> pd.Series:
        return ~series.astype(str).str.contains(str(value), case=False, na=False)

    def get_operator_name(self) -> str:
        return 'not_contains'


class InListStrategy(FilterStrategy):
    """Strategy for checking if value is in a list"""

    def apply(self, series: pd.Series, value: Any) -> pd.Series:
        return series.isin(value)

    def get_operator_name(self) -> str:
        return 'in'


class NotInListStrategy(FilterStrategy):
    """Strategy for checking if value is not in a list"""

    def apply(self, series: pd.Series, value: Any) -> pd.Series:
        return ~series.isin(value)

    def get_operator_name(self) -> str:
        return 'not_in'


class GreaterThanStrategy(FilterStrategy):
    """Strategy for greater than comparison"""

    def apply(self, series: pd.Series, value: Any) -> pd.Series:
        return series > value

    def get_operator_name(self) -> str:
        return 'greater_than'


class LessThanStrategy(FilterStrategy):
    """Strategy for less than comparison"""

    def apply(self, series: pd.Series, value: Any) -> pd.Series:
        return series < value

    def get_operator_name(self) -> str:
        return 'less_than'


class GreaterEqualStrategy(FilterStrategy):
    """Strategy for greater than or equal comparison"""

    def apply(self, series: pd.Series, value: Any) -> pd.Series:
        return series >= value

    def get_operator_name(self) -> str:
        return 'greater_equal'


class LessEqualStrategy(FilterStrategy):
    """Strategy for less than or equal comparison"""

    def apply(self, series: pd.Series, value: Any) -> pd.Series:
        return series <= value

    def get_operator_name(self) -> str:
        return 'less_equal'


class BetweenStrategy(FilterStrategy):
    """Strategy for range checking (between two values)"""

    def apply(self, series: pd.Series, value: Any) -> pd.Series:
        if isinstance(value, (list, tuple)) and len(value) == 2:
            return (series >= value[0]) & (series <= value[1])
        else:
            # Invalid value format, return all True (no filtering)
            return pd.Series([True] * len(series), index=series.index)

    def get_operator_name(self) -> str:
        return 'between'


class IsNullStrategy(FilterStrategy):
    """Strategy for checking null values"""

    def apply(self, series: pd.Series, value: Any) -> pd.Series:
        return series.isna()

    def get_operator_name(self) -> str:
        return 'is_null'


class NotNullStrategy(FilterStrategy):
    """Strategy for checking non-null values"""

    def apply(self, series: pd.Series, value: Any) -> pd.Series:
        return series.notna()

    def get_operator_name(self) -> str:
        return 'not_null'


class StartsWithStrategy(FilterStrategy):
    """Strategy for checking if string starts with a value"""

    def apply(self, series: pd.Series, value: Any) -> pd.Series:
        return series.astype(str).str.startswith(str(value), na=False)

    def get_operator_name(self) -> str:
        return 'starts_with'


class EndsWithStrategy(FilterStrategy):
    """Strategy for checking if string ends with a value"""

    def apply(self, series: pd.Series, value: Any) -> pd.Series:
        return series.astype(str).str.endswith(str(value), na=False)

    def get_operator_name(self) -> str:
        return 'ends_with'


class RegexMatchStrategy(FilterStrategy):
    """Strategy for regex pattern matching"""

    def apply(self, series: pd.Series, value: Any) -> pd.Series:
        try:
            return series.astype(str).str.match(str(value), na=False)
        except Exception:
            # Invalid regex, return all False
            return pd.Series([False] * len(series), index=series.index)

    def get_operator_name(self) -> str:
        return 'regex'


class FilterStrategyFactory:
    """Factory for creating filter strategies based on operator name"""

    # Registry of available strategies
    _strategies = {
        'equals': EqualsStrategy(),
        'not_equals': NotEqualsStrategy(),
        'contains': ContainsStrategy(),
        'not_contains': NotContainsStrategy(),
        'in': InListStrategy(),
        'not_in': NotInListStrategy(),
        'greater_than': GreaterThanStrategy(),
        'less_than': LessThanStrategy(),
        'greater_equal': GreaterEqualStrategy(),
        'less_equal': LessEqualStrategy(),
        'between': BetweenStrategy(),
        'is_null': IsNullStrategy(),
        'not_null': NotNullStrategy(),
        'starts_with': StartsWithStrategy(),
        'ends_with': EndsWithStrategy(),
        'regex': RegexMatchStrategy(),
    }

    @classmethod
    def get_strategy(cls, operator: str) -> FilterStrategy:
        """
        Get the appropriate filter strategy for an operator

        Args:
            operator: The operator name

        Returns:
            FilterStrategy instance for the operator

        Raises:
            ValueError: If operator is not supported
        """
        if operator not in cls._strategies:
            raise ValueError(f"Unsupported operator: {operator}. Available operators: {list(cls._strategies.keys())}")

        return cls._strategies[operator]

    @classmethod
    def get_available_operators(cls) -> list:
        """Get list of all available operators"""
        return list(cls._strategies.keys())

    @classmethod
    def register_strategy(cls, strategy: FilterStrategy):
        """
        Register a new custom strategy

        Args:
            strategy: The custom FilterStrategy to register
        """
        operator_name = strategy.get_operator_name()
        cls._strategies[operator_name] = strategy

    @classmethod
    def has_operator(cls, operator: str) -> bool:
        """Check if an operator is supported"""
        return operator in cls._strategies


# =============================================================================
# Filter Data Classes
# =============================================================================

class FilterCriteria:
    """Represents a single filter criterion"""

    def __init__(self, column: str, operator: str, value: Any, filter_type: str = 'event'):
        """
        Initialize filter criterion

        Args:
            column: Column name to filter on
            operator: Comparison operator ('equals', 'not_equals', 'contains', 'in', 'not_in',
                     'greater_than', 'less_than', 'between')
            value: Value or list of values to compare against
            filter_type: Type of filter - 'event' or 'case'
        """
        self.column = column
        self.operator = operator
        self.value = value
        self.filter_type = filter_type
        self.enabled = True

    def __repr__(self):
        return f"FilterCriteria({self.column} {self.operator} {self.value}, type={self.filter_type})"


class TimeFilter:
    """Represents a time period filter"""

    def __init__(self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None):
        """
        Initialize time filter

        Args:
            start_time: Start of time period (inclusive)
            end_time: End of time period (inclusive)
        """
        self.start_time = start_time
        self.end_time = end_time
        self.enabled = True

    def is_active(self) -> bool:
        """Check if filter has any active criteria"""
        return self.enabled and (self.start_time is not None or self.end_time is not None)

    def __repr__(self):
        return f"TimeFilter({self.start_time} to {self.end_time})"


# =============================================================================
# Filter Manager
# =============================================================================

class FilterManager:
    """Manages all filters and applies them to event log data"""

    def __init__(self):
        self.original_df: Optional[pd.DataFrame] = None
        self.filtered_df: Optional[pd.DataFrame] = None

        # Filter storage
        self.event_filters: List[FilterCriteria] = []
        self.case_filters: List[FilterCriteria] = []
        self.time_filter: TimeFilter = TimeFilter()

        # Cache for available columns
        self.event_columns: List[str] = []
        self.case_columns: List[str] = []

    def set_data(self, df: pd.DataFrame):
        """
        Set the original data and detect available columns

        Args:
            df: Original event log DataFrame
        """
        self.original_df = df.copy()
        self.filtered_df = df.copy()
        self._detect_columns()

    def _detect_columns(self):
        """Detect available event and case columns"""
        if self.original_df is None:
            return

        # Standard pm4py columns
        standard_cols = {'case:concept:name', 'concept:name', 'time:timestamp'}

        # All other columns are potential filter columns
        all_cols = set(self.original_df.columns)
        custom_cols = all_cols - standard_cols

        # Separate into case-level and event-level columns
        self.case_columns = []
        self.event_columns = []

        for col in custom_cols:
            # Check if column has constant value per case
            grouped = self.original_df.groupby('case:concept:name')[col].nunique()
            if (grouped == 1).all():
                # All cases have only one unique value for this column -> Case attribute
                self.case_columns.append(col)
            else:
                # At least one case has multiple values -> Event attribute
                self.event_columns.append(col)

        # Sort for consistent display
        self.case_columns = sorted(self.case_columns)
        self.event_columns = sorted(self.event_columns)

    def get_unique_values(self, column: str) -> List[Any]:
        """
        Get unique values for a column

        Args:
            column: Column name

        Returns:
            List of unique values
        """
        if self.original_df is None or column not in self.original_df.columns:
            return []

        return sorted(self.original_df[column].dropna().unique().tolist())

    def add_event_filter(self, column: str, operator: str, value: Any) -> FilterCriteria:
        """
        Add an event-level filter

        Args:
            column: Column to filter on
            operator: Comparison operator
            value: Value to compare

        Returns:
            Created FilterCriteria
        """
        filter_criterion = FilterCriteria(column, operator, value, 'event')
        self.event_filters.append(filter_criterion)
        self._apply_filters()
        return filter_criterion

    def add_case_filter(self, column: str, operator: str, value: Any) -> FilterCriteria:
        """
        Add a case-level filter

        Args:
            column: Column to filter on
            operator: Comparison operator
            value: Value to compare

        Returns:
            Created FilterCriteria
        """
        filter_criterion = FilterCriteria(column, operator, value, 'case')
        self.case_filters.append(filter_criterion)
        self._apply_filters()
        return filter_criterion

    def set_time_filter(self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None):
        """
        Set time period filter

        Args:
            start_time: Start of period
            end_time: End of period
        """
        self.time_filter.start_time = start_time
        self.time_filter.end_time = end_time
        self._apply_filters()

    def remove_filter(self, filter_criterion: FilterCriteria):
        """
        Remove a filter criterion

        Args:
            filter_criterion: Filter to remove
        """
        if filter_criterion.filter_type == 'event':
            if filter_criterion in self.event_filters:
                self.event_filters.remove(filter_criterion)
        else:
            if filter_criterion in self.case_filters:
                self.case_filters.remove(filter_criterion)

        self._apply_filters()

    def clear_all_filters(self):
        """Clear all filters"""
        self.event_filters.clear()
        self.case_filters.clear()
        self.time_filter = TimeFilter()
        self._apply_filters()

    def toggle_filter(self, filter_criterion: FilterCriteria, enabled: bool):
        """
        Enable or disable a filter without removing it

        Args:
            filter_criterion: Filter to toggle
            enabled: Whether to enable or disable
        """
        filter_criterion.enabled = enabled
        self._apply_filters()

    def _apply_filters(self):
        """Apply all active filters to the data"""
        if self.original_df is None:
            return

        df = self.original_df.copy()

        # Apply time filter
        if self.time_filter.is_active():
            df = self._apply_time_filter(df)

        # Apply event filters
        for filter_criterion in self.event_filters:
            if filter_criterion.enabled:
                df = self._apply_filter_criterion(df, filter_criterion)

        # Apply case filters
        if self.case_filters:
            # Get cases that match all case filters
            valid_cases = self._get_valid_cases(df)
            df = df[df['case:concept:name'].isin(valid_cases)]

        self.filtered_df = df

    def _apply_time_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply time period filter"""
        if self.time_filter.start_time:
            df = df[df['time:timestamp'] >= self.time_filter.start_time]
        if self.time_filter.end_time:
            df = df[df['time:timestamp'] <= self.time_filter.end_time]
        return df

    def _apply_filter_criterion(self, df: pd.DataFrame, criterion: FilterCriteria) -> pd.DataFrame:
        """
        Apply a single filter criterion using Strategy Pattern

        Args:
            df: DataFrame to filter
            criterion: FilterCriteria to apply

        Returns:
            Filtered DataFrame
        """
        if criterion.column not in df.columns:
            return df

        col = df[criterion.column]

        try:
            # Get the appropriate strategy from factory
            strategy = FilterStrategyFactory.get_strategy(criterion.operator)

            # Apply the strategy
            mask = strategy.apply(col, criterion.value)

            return df[mask]

        except ValueError as e:
            # Unknown operator, log warning and don't filter
            print(f"Warning: {e}. Skipping filter.")
            return df

        except Exception as e:
            # Any other error, log and don't filter
            print(f"Error applying filter {criterion}: {e}. Skipping filter.")
            return df

    def _get_valid_cases(self, df: pd.DataFrame) -> Set[str]:
        """Get case IDs that match all case-level filters"""
        if not self.case_filters:
            return set(df['case:concept:name'].unique())

        # Start with all cases
        valid_cases = set(df['case:concept:name'].unique())

        # Apply each case filter
        for criterion in self.case_filters:
            if not criterion.enabled:
                continue

            # Get one row per case (since case attributes are constant per case)
            case_df = df.groupby('case:concept:name').first().reset_index()

            # Apply filter
            filtered_case_df = self._apply_filter_criterion(case_df, criterion)

            # Intersect with valid cases
            valid_cases = valid_cases.intersection(set(filtered_case_df['case:concept:name'].unique()))

        return valid_cases

    def get_filtered_data(self) -> pd.DataFrame:
        """
        Get the filtered DataFrame

        Returns:
            Filtered DataFrame
        """
        if self.filtered_df is None:
            return pd.DataFrame()
        return self.filtered_df.copy()

    def get_filter_summary(self) -> Dict[str, Any]:
        """
        Get summary of active filters

        Returns:
            Dictionary with filter counts and statistics
        """
        active_event_filters = sum(1 for f in self.event_filters if f.enabled)
        active_case_filters = sum(1 for f in self.case_filters if f.enabled)
        time_active = self.time_filter.is_active()

        original_events = len(self.original_df) if self.original_df is not None else 0
        filtered_events = len(self.filtered_df) if self.filtered_df is not None else 0

        original_cases = self.original_df['case:concept:name'].nunique() if self.original_df is not None else 0
        filtered_cases = self.filtered_df['case:concept:name'].nunique() if self.filtered_df is not None else 0

        return {
            'total_filters': active_event_filters + active_case_filters + (1 if time_active else 0),
            'event_filters': active_event_filters,
            'case_filters': active_case_filters,
            'time_filter_active': time_active,
            'original_events': original_events,
            'filtered_events': filtered_events,
            'original_cases': original_cases,
            'filtered_cases': filtered_cases,
            'events_filtered_out': original_events - filtered_events,
            'cases_filtered_out': original_cases - filtered_cases,
            'filter_percentage': (filtered_events / original_events * 100) if original_events > 0 else 100
        }

    def get_all_filters(self) -> List[FilterCriteria]:
        """Get all filters (event and case)"""
        return self.event_filters + self.case_filters

    def clear_filters(self):
        """Clear all filters"""
        self.event_filters.clear()
        self.case_filters.clear()
        self.time_filter = TimeFilter()

        # Reset filtered data to original
        if self.original_df is not None:
            self.filtered_df = self.original_df.copy()

    def restore_filters(self, filters_data: dict):
        """
        Restore filters from saved data

        Args:
            filters_data: Dictionary containing saved filter state
        """
        # Clear existing filters
        self.event_filters.clear()
        self.case_filters.clear()

        # Restore event filters
        if 'event_filters' in filters_data:
            for filter_dict in filters_data['event_filters']:
                if isinstance(filter_dict, dict):
                    criterion = FilterCriteria(
                        column=filter_dict['column'],
                        operator=filter_dict['operator'],
                        value=filter_dict['value'],
                        filter_type='event'
                    )
                    criterion.enabled = filter_dict.get('enabled', True)
                    self.event_filters.append(criterion)

        # Restore case filters
        if 'case_filters' in filters_data:
            for filter_dict in filters_data['case_filters']:
                if isinstance(filter_dict, dict):
                    criterion = FilterCriteria(
                        column=filter_dict['column'],
                        operator=filter_dict['operator'],
                        value=filter_dict['value'],
                        filter_type='case'
                    )
                    criterion.enabled = filter_dict.get('enabled', True)
                    self.case_filters.append(criterion)

        # Restore time filter
        if 'time_filter' in filters_data:
            time_dict = filters_data['time_filter']
            self.time_filter = TimeFilter(
                start_time=time_dict.get('start_time'),
                end_time=time_dict.get('end_time')
            )
            self.time_filter.enabled = time_dict.get('enabled', True)

        # Apply filters
        self._apply_filters()

    def get_all_filters_serializable(self) -> dict:
        """
        Get all filters in a serializable format for saving

        Returns:
            Dictionary containing all filter states
        """
        return {
            'event_filters': [
                {
                    'column': f.column,
                    'operator': f.operator,
                    'value': f.value,
                    'enabled': f.enabled
                }
                for f in self.event_filters
            ],
            'case_filters': [
                {
                    'column': f.column,
                    'operator': f.operator,
                    'value': f.value,
                    'enabled': f.enabled
                }
                for f in self.case_filters
            ],
            'time_filter': {
                'start_time': self.time_filter.start_time,
                'end_time': self.time_filter.end_time,
                'enabled': self.time_filter.enabled
            }
        }
