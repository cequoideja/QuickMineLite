"""
Data loader module for importing and validating event logs
"""
import pandas as pd
import numpy as np
from typing import Tuple, Dict, List, Optional
from datetime import datetime
import os
from core.config import Config


class EventLogLoader:
    """Handle loading and validation of event logs from CSV files"""

    def __init__(self):
        self.df: Optional[pd.DataFrame] = None
        self.file_path: Optional[str] = None
        self.case_id_col: Optional[str] = None
        self.activity_col: Optional[str] = None
        self.timestamp_col: Optional[str] = None
        self.resource_col: Optional[str] = None  # Optional resource column
        self.errors: List[str] = []

    def load_csv(self, file_path: str, encoding: str = None, max_rows: Optional[int] = None) -> Tuple[bool, str]:
        """
        Load CSV file and perform initial validation

        Args:
            file_path: Path to CSV file
            encoding: File encoding (default: None, uses Config.DEFAULT_CSV_ENCODING)
            max_rows: Maximum number of rows to load (default: None, load all)

        Returns:
            (success, message)
        """
        # Use default encoding from Config if not specified
        if encoding is None:
            encoding = Config.DEFAULT_CSV_ENCODING

        try:
            self.file_path = file_path
            self.errors = []

            # Try reading CSV
            if max_rows:
                self.df = pd.read_csv(file_path, encoding=encoding, nrows=max_rows, low_memory=False)
                message = f"Successfully loaded {len(self.df)} rows (limited to {max_rows}) and {len(self.df.columns)} columns"
            else:
                self.df = pd.read_csv(file_path, encoding=encoding, low_memory=False)
                message = f"Successfully loaded {len(self.df)} rows and {len(self.df.columns)} columns"

            if self.df.empty:
                return False, "The CSV file is empty"

            if len(self.df.columns) < 3:
                return False, "CSV must have at least 3 columns (Case ID, Activity, Timestamp)"

            return True, message

        except UnicodeDecodeError:
            # Try fallback encoding from Config
            try:
                if max_rows:
                    self.df = pd.read_csv(file_path, encoding=Config.FALLBACK_CSV_ENCODING, nrows=max_rows)
                    return True, f"Successfully loaded {len(self.df)} rows (limited to {max_rows}, encoding: {Config.FALLBACK_CSV_ENCODING})"
                else:
                    self.df = pd.read_csv(file_path, encoding=Config.FALLBACK_CSV_ENCODING)
                    return True, f"Successfully loaded {len(self.df)} rows (encoding: {Config.FALLBACK_CSV_ENCODING})"
            except Exception as e:
                return False, f"Encoding error: {str(e)}"

        except FileNotFoundError:
            return False, f"File not found: {file_path}"

        except pd.errors.EmptyDataError:
            return False, "The CSV file is empty"

        except Exception as e:
            return False, f"Error loading CSV: {str(e)}"

    def load_from_uploaded_file(self, uploaded_file, encoding: str = None, max_rows: Optional[int] = None) -> Tuple[bool, str]:
        """
        Load data from a Streamlit UploadedFile object

        Args:
            uploaded_file: Streamlit UploadedFile object (file-like with .name attribute)
            encoding: File encoding (default: None, uses Config.DEFAULT_CSV_ENCODING)
            max_rows: Maximum number of rows to load (default: None, load all)

        Returns:
            (success, message)
        """
        if encoding is None:
            encoding = Config.DEFAULT_CSV_ENCODING

        try:
            self.file_path = getattr(uploaded_file, 'name', 'uploaded_file.csv')
            self.errors = []

            # Try reading from the uploaded file object
            if max_rows:
                self.df = pd.read_csv(uploaded_file, encoding=encoding, nrows=max_rows, low_memory=False)
                message = f"Successfully loaded {len(self.df)} rows (limited to {max_rows}) and {len(self.df.columns)} columns"
            else:
                self.df = pd.read_csv(uploaded_file, encoding=encoding, low_memory=False)
                message = f"Successfully loaded {len(self.df)} rows and {len(self.df.columns)} columns"

            if self.df.empty:
                return False, "The CSV file is empty"

            if len(self.df.columns) < 3:
                return False, "CSV must have at least 3 columns (Case ID, Activity, Timestamp)"

            return True, message

        except UnicodeDecodeError:
            # Try fallback encoding from Config
            try:
                # Reset file position for re-read
                uploaded_file.seek(0)
                if max_rows:
                    self.df = pd.read_csv(uploaded_file, encoding=Config.FALLBACK_CSV_ENCODING, nrows=max_rows)
                    return True, f"Successfully loaded {len(self.df)} rows (limited to {max_rows}, encoding: {Config.FALLBACK_CSV_ENCODING})"
                else:
                    self.df = pd.read_csv(uploaded_file, encoding=Config.FALLBACK_CSV_ENCODING)
                    return True, f"Successfully loaded {len(self.df)} rows (encoding: {Config.FALLBACK_CSV_ENCODING})"
            except Exception as e:
                return False, f"Encoding error: {str(e)}"

        except pd.errors.EmptyDataError:
            return False, "The CSV file is empty"

        except Exception as e:
            return False, f"Error loading CSV: {str(e)}"

    def get_columns(self) -> List[str]:
        """Get list of column names from loaded CSV"""
        if self.df is None:
            return []
        return list(self.df.columns)

    def get_preview(self, n_rows: int = None) -> pd.DataFrame:
        """Get first n rows as preview"""
        if n_rows is None:
            n_rows = Config.TABLE_PREVIEW_ROWS
        if self.df is None:
            return pd.DataFrame()
        return self.df.head(n_rows)

    def detect_column_mapping(self) -> Dict[str, Optional[str]]:
        """
        Auto-detect which columns might be Case ID, Activity, Timestamp, and Resource

        Returns:
            Dictionary with suggested column mappings
        """
        if self.df is None:
            return {'case_id': None, 'activity': None, 'timestamp': None, 'resource': None}

        columns = [col.lower() for col in self.df.columns]
        mapping = {'case_id': None, 'activity': None, 'timestamp': None, 'resource': None}

        # Case ID detection
        case_patterns = ['case', 'caseid', 'case_id', 'id', 'trace', 'traceid']
        for pattern in case_patterns:
            for i, col in enumerate(columns):
                if pattern in col:
                    mapping['case_id'] = self.df.columns[i]
                    break
            if mapping['case_id']:
                break

        # Activity detection
        activity_patterns = ['activity', 'event', 'task', 'action', 'step']
        for pattern in activity_patterns:
            for i, col in enumerate(columns):
                if pattern in col:
                    mapping['activity'] = self.df.columns[i]
                    break
            if mapping['activity']:
                break

        # Timestamp detection
        timestamp_patterns = ['timestamp', 'time', 'date', 'datetime', 'start', 'end']
        for pattern in timestamp_patterns:
            for i, col in enumerate(columns):
                if pattern in col:
                    mapping['timestamp'] = self.df.columns[i]
                    break
            if mapping['timestamp']:
                break

        # Resource detection (optional)
        resource_patterns = ['resource', 'user', 'actor', 'person', 'agent', 'org:resource']
        for pattern in resource_patterns:
            for i, col in enumerate(columns):
                if pattern in col:
                    mapping['resource'] = self.df.columns[i]
                    break
            if mapping['resource']:
                break

        return mapping

    def set_column_mapping(self, case_id: str, activity: str, timestamp: str, resource: Optional[str] = None) -> Tuple[bool, str]:
        """
        Set column mapping and validate

        Args:
            case_id: Column name for Case ID
            activity: Column name for Activity
            timestamp: Column name for Timestamp
            resource: Column name for Resource (optional)

        Returns:
            (success, message)
        """
        if self.df is None:
            return False, "No data loaded"

        # Validate required columns exist
        missing_cols = []
        for col_name, col_value in [('Case ID', case_id), ('Activity', activity), ('Timestamp', timestamp)]:
            if col_value not in self.df.columns:
                missing_cols.append(f"{col_name} ({col_value})")

        # Validate optional resource column if provided
        if resource and resource not in self.df.columns:
            missing_cols.append(f"Resource ({resource})")

        if missing_cols:
            return False, f"Columns not found: {', '.join(missing_cols)}"

        self.case_id_col = case_id
        self.activity_col = activity
        self.timestamp_col = timestamp
        self.resource_col = resource  # Store resource column (may be None)

        # Validate data
        validation_result, validation_msg = self.validate_data()
        if not validation_result:
            return False, validation_msg

        return True, "Column mapping validated successfully"

    def validate_data(self) -> Tuple[bool, str]:
        """
        Validate the mapped data for process mining requirements

        Returns:
            (success, message with warnings/errors)
        """
        if self.df is None or not all([self.case_id_col, self.activity_col, self.timestamp_col]):
            return False, "Data or column mapping not set"

        self.errors = []
        warnings = []

        # Check for null values
        null_cases = self.df[self.case_id_col].isnull().sum()
        null_activities = self.df[self.activity_col].isnull().sum()
        null_timestamps = self.df[self.timestamp_col].isnull().sum()

        if null_cases > 0:
            self.errors.append(f"{null_cases} rows have null Case IDs")
        if null_activities > 0:
            self.errors.append(f"{null_activities} rows have null Activities")
        if null_timestamps > 0:
            self.errors.append(f"{null_timestamps} rows have null Timestamps")

        if self.errors:
            return False, "Validation failed: " + "; ".join(self.errors)

        # Parse timestamps
        try:
            self.df[self.timestamp_col] = pd.to_datetime(self.df[self.timestamp_col])
        except Exception as e:
            # Show sample of problematic values
            sample_values = self.df[self.timestamp_col].head(3).tolist()
            return False, f"Cannot parse timestamps: {str(e)}\n\nSample values found: {sample_values}\n\nExpected format: '2024-01-15 09:00:00' or similar date/time format."

        # Warnings (non-critical)
        if self.df[self.case_id_col].nunique() < 2:
            warnings.append("Only 1 unique case ID found")

        if self.df[self.activity_col].nunique() < 2:
            warnings.append("Only 1 unique activity found")

        # Check for duplicate events
        duplicates = self.df.duplicated(subset=[self.case_id_col, self.activity_col, self.timestamp_col]).sum()
        if duplicates > 0:
            warnings.append(f"{duplicates} duplicate events found")

        message = "Validation successful"
        if warnings:
            message += " (Warnings: " + "; ".join(warnings) + ")"

        return True, message

    def prepare_event_log(self) -> pd.DataFrame:
        """
        Prepare event log in standard format for pm4py

        Returns:
            DataFrame with standardized columns
        """
        if self.df is None or not all([self.case_id_col, self.activity_col, self.timestamp_col]):
            raise ValueError("Data not properly loaded and mapped")

        # Create a copy with standardized column names
        event_log = self.df.copy()

        # Rename core columns to standard names
        rename_map = {
            self.case_id_col: 'case:concept:name',
            self.activity_col: 'concept:name',
            self.timestamp_col: 'time:timestamp'
        }

        # Add resource column if selected
        if self.resource_col:
            rename_map[self.resource_col] = 'org:resource'

        event_log = event_log.rename(columns=rename_map)

        # Sort by case and timestamp
        event_log = event_log.sort_values(['case:concept:name', 'time:timestamp'])

        # Reset index
        event_log = event_log.reset_index(drop=True)

        return event_log

    def detect_attribute_types(self, event_log: pd.DataFrame) -> Dict[str, List[str]]:
        """
        Detect which attributes are case-level vs event-level

        Args:
            event_log: Prepared event log DataFrame

        Returns:
            Dictionary with 'case_attributes' and 'event_attributes' lists
        """
        # Get all custom columns (excluding standard pm4py columns)
        standard_cols = {'case:concept:name', 'concept:name', 'time:timestamp'}
        custom_cols = [col for col in event_log.columns if col not in standard_cols]

        case_attributes = []
        event_attributes = []

        for col in custom_cols:
            # Check if column has constant value per case
            grouped = event_log.groupby('case:concept:name')[col].nunique()
            if (grouped == 1).all():
                # All cases have only one unique value for this column -> Case attribute
                case_attributes.append(col)
            else:
                # At least one case has multiple values -> Event attribute
                event_attributes.append(col)

        return {
            'case_attributes': sorted(case_attributes),
            'event_attributes': sorted(event_attributes)
        }

    def get_attribute_info(self) -> Dict[str, Dict[str, any]]:
        """
        Get detailed information about each attribute

        Returns:
            Dictionary with detailed attribute information
        """
        if self.df is None or not all([self.case_id_col, self.activity_col, self.timestamp_col]):
            return {}

        event_log = self.prepare_event_log()
        attribute_types = self.detect_attribute_types(event_log)

        attribute_info = {}

        # Analyze case attributes
        for col in attribute_types['case_attributes']:
            # Get unique values (one per case)
            case_values = event_log.groupby('case:concept:name')[col].first()

            attribute_info[col] = {
                'type': 'case',
                'unique_values': case_values.nunique(),
                'null_count': case_values.isnull().sum(),
                'data_type': str(event_log[col].dtype),
                'sample_values': case_values.dropna().unique()[:5].tolist()
            }

        # Analyze event attributes
        for col in attribute_types['event_attributes']:
            attribute_info[col] = {
                'type': 'event',
                'unique_values': event_log[col].nunique(),
                'null_count': event_log[col].isnull().sum(),
                'data_type': str(event_log[col].dtype),
                'sample_values': event_log[col].dropna().unique()[:5].tolist()
            }

        return attribute_info

    def get_metadata(self) -> Dict[str, any]:
        """
        Get metadata about the loaded event log

        Returns:
            Dictionary with metadata
        """
        if self.df is None or not all([self.case_id_col, self.activity_col, self.timestamp_col]):
            return {}

        event_log = self.prepare_event_log()
        attribute_types = self.detect_attribute_types(event_log)

        metadata = {
            'file_name': os.path.basename(self.file_path) if self.file_path else 'unknown',
            'case_id_column': self.case_id_col,
            'activity_column': self.activity_col,
            'timestamp_column': self.timestamp_col,
            'resource_column': self.resource_col,  # Include resource column
            'total_events': len(event_log),
            'total_cases': event_log['case:concept:name'].nunique(),
            'total_activities': event_log['concept:name'].nunique(),
            'total_resources': event_log['org:resource'].nunique() if self.resource_col else 0,
            'start_date': event_log['time:timestamp'].min().isoformat(),
            'end_date': event_log['time:timestamp'].max().isoformat(),
            'additional_columns': [
                col for col in event_log.columns
                if col not in ['case:concept:name', 'concept:name', 'time:timestamp', 'org:resource']
            ],
            'case_attributes': attribute_types['case_attributes'],
            'event_attributes': attribute_types['event_attributes']
        }

        return metadata

    def compute_derived_case_attributes(self, event_log: pd.DataFrame,
                                       event_attributes: List[str],
                                       aggregations: Dict[str, List[str]] = None) -> pd.DataFrame:
        """
        Compute derived case attributes from event attributes using aggregation functions

        Args:
            event_log: Prepared event log DataFrame
            event_attributes: List of event attribute names to aggregate
            aggregations: Dictionary mapping attribute names to list of aggregation functions
                         Supported: 'last', 'first', 'max', 'min', 'sum', 'avg', 'count'
                         If None, defaults to 'last' for all event attributes

        Returns:
            DataFrame with one row per case and derived attributes as columns
        """
        if aggregations is None:
            # Default: compute 'last' for all event attributes
            aggregations = {attr: ['last'] for attr in event_attributes}

        # Group by case
        case_groups = event_log.groupby('case:concept:name')

        derived_data = []

        for case_id, group in case_groups:
            # Sort by timestamp to ensure correct order
            group_sorted = group.sort_values('time:timestamp')

            case_attrs = {'case:concept:name': case_id}

            # Compute aggregations for each attribute
            for attr, agg_funcs in aggregations.items():
                if attr not in group.columns:
                    continue

                for agg_func in agg_funcs:
                    # Generate column name (e.g., "Last resource", "First amount")
                    col_name = f"{agg_func.capitalize()} {attr}"

                    try:
                        if agg_func == 'last':
                            # Get last non-null value based on timestamp
                            non_null_values = group_sorted[attr].dropna()
                            if len(non_null_values) > 0:
                                case_attrs[col_name] = non_null_values.iloc[-1]
                            else:
                                case_attrs[col_name] = None

                        elif agg_func == 'first':
                            # Get first non-null value based on timestamp
                            non_null_values = group_sorted[attr].dropna()
                            if len(non_null_values) > 0:
                                case_attrs[col_name] = non_null_values.iloc[0]
                            else:
                                case_attrs[col_name] = None

                        elif agg_func == 'max':
                            # Try numeric max, fallback to None for non-numeric
                            try:
                                case_attrs[col_name] = pd.to_numeric(group[attr], errors='coerce').max()
                            except:
                                case_attrs[col_name] = None

                        elif agg_func == 'min':
                            # Try numeric min, fallback to None for non-numeric
                            try:
                                case_attrs[col_name] = pd.to_numeric(group[attr], errors='coerce').min()
                            except:
                                case_attrs[col_name] = None

                        elif agg_func == 'sum':
                            # Try numeric sum, fallback to None for non-numeric
                            try:
                                case_attrs[col_name] = pd.to_numeric(group[attr], errors='coerce').sum()
                            except:
                                case_attrs[col_name] = None

                        elif agg_func == 'avg':
                            # Try numeric mean, fallback to None for non-numeric
                            try:
                                case_attrs[col_name] = pd.to_numeric(group[attr], errors='coerce').mean()
                            except:
                                case_attrs[col_name] = None

                        elif agg_func == 'count':
                            case_attrs[col_name] = group[attr].count()

                    except Exception as e:
                        # If any aggregation fails, set to None
                        case_attrs[col_name] = None

            derived_data.append(case_attrs)

        return pd.DataFrame(derived_data)


def validate_event_log_format(df: pd.DataFrame) -> Tuple[bool, str]:
    """
    Validate that a DataFrame is in proper event log format

    Args:
        df: DataFrame to validate

    Returns:
        (is_valid, message)
    """
    required_columns = ['case:concept:name', 'concept:name', 'time:timestamp']

    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        return False, f"Missing required columns: {', '.join(missing_columns)}"

    if df.empty:
        return False, "Event log is empty"

    return True, "Event log format is valid"
