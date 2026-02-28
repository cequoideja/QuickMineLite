"""
Helper utilities for QuickMineLite
"""
import pandas as pd


class ColumnNameMapper:
    """Map between technical pm4py column names and user-friendly names"""

    TECHNICAL_TO_FRIENDLY = {
        'case:concept:name': 'Case ID',
        'concept:name': 'Activity',
        'time:timestamp': 'Timestamp',
        'org:resource': 'Resource'
    }

    FRIENDLY_TO_TECHNICAL = {v: k for k, v in TECHNICAL_TO_FRIENDLY.items()}

    @classmethod
    def to_friendly(cls, technical_name: str) -> str:
        return cls.TECHNICAL_TO_FRIENDLY.get(technical_name, technical_name)

    @classmethod
    def to_technical(cls, friendly_name: str) -> str:
        return cls.FRIENDLY_TO_TECHNICAL.get(friendly_name, friendly_name)

    @classmethod
    def is_standard_column(cls, column_name: str) -> bool:
        return column_name in cls.TECHNICAL_TO_FRIENDLY


def format_duration(seconds: float) -> str:
    """Format duration in seconds to human-readable format"""
    if pd.isna(seconds):
        return "N/A"
    if seconds < 0:
        return "Invalid"
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds / 60:.1f}m"
    elif seconds < 86400:
        return f"{seconds / 3600:.1f}h"
    else:
        return f"{seconds / 86400:.1f}d"


def format_number(num) -> str:
    """Format large numbers with thousand separators"""
    return f"{num:,}"


def classify_columns(df: pd.DataFrame) -> dict:
    """
    Classify DataFrame columns as case-level or event-level attributes.

    A **case-level** attribute has at most one unique value per case
    (e.g. customer region, priority).
    An **event-level** attribute can vary across events within the same case
    (e.g. resource, cost).

    Returns dict with keys:
        standard_columns  - pm4py standard columns present in the data
        event_columns     - custom columns classified as event-level
        case_columns      - custom columns classified as case-level
    """
    STANDARD = {'case:concept:name', 'concept:name', 'time:timestamp'}
    present_standard = sorted(STANDARD & set(df.columns))
    custom_cols = [c for c in df.columns if c not in STANDARD]

    if not custom_cols:
        return {
            'standard_columns': present_standard,
            'event_columns': [],
            'case_columns': [],
        }

    # Compute max number of unique values per case for every custom column
    # (single groupby call -- efficient even for large datasets)
    max_nunique = df.groupby('case:concept:name')[custom_cols].nunique().max()

    case_columns = sorted(c for c in custom_cols if max_nunique[c] <= 1)
    event_columns = sorted(c for c in custom_cols if max_nunique[c] > 1)

    return {
        'standard_columns': present_standard,
        'event_columns': event_columns,
        'case_columns': case_columns,
    }
