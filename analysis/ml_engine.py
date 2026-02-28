"""
Machine Learning Engine for Process Mining

Pure-Python ML engine providing training and prediction functions for:
- Next activity prediction
- Remaining time prediction
- Case outcome prediction

Uses scikit-learn RandomForest models with process-aware feature engineering.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, mean_absolute_error, r2_score


def _build_event_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build process-aware features for each event in the log.

    Features created per event:
    - activity_encoded: LabelEncoded current activity
    - position_in_case: event index / total events in case (0.0 to 1.0)
    - elapsed_time: seconds since case start
    - events_so_far: number of events up to and including this one

    Args:
        df: Event log DataFrame with standard pm4py columns

    Returns:
        DataFrame with added feature columns and metadata columns
    """
    df = df.copy()

    # Ensure sorted
    df = df.sort_values(['case:concept:name', 'time:timestamp']).reset_index(drop=True)

    # Encode activity
    le_activity = LabelEncoder()
    df['activity_encoded'] = le_activity.fit_transform(df['concept:name'])

    # Calculate case-level aggregates
    case_sizes = df.groupby('case:concept:name')['concept:name'].transform('count')
    case_starts = df.groupby('case:concept:name')['time:timestamp'].transform('min')

    # Event index within case (1-based)
    df['event_index'] = df.groupby('case:concept:name').cumcount() + 1

    # Position in case (0.0 to 1.0)
    df['position_in_case'] = df['event_index'] / case_sizes

    # Elapsed time since case start (in seconds)
    df['elapsed_time'] = (df['time:timestamp'] - case_starts).dt.total_seconds()

    # Number of events so far (same as event_index)
    df['events_so_far'] = df['event_index']

    # Store case size for convenience
    df['case_size'] = case_sizes

    # Store the label encoder for later use
    df.attrs['activity_label_encoder'] = le_activity

    return df


def train_next_activity_model(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Train a model to predict the next activity in a case.

    For each event (except the last in each case), the target is the
    immediately following activity.

    Features:
    - activity_encoded: current activity (LabelEncoded)
    - position_in_case: relative position in the case
    - elapsed_time: seconds since case start
    - events_so_far: number of events completed

    Args:
        df: Event log DataFrame with columns:
            case:concept:name, concept:name, time:timestamp

    Returns:
        Dictionary containing:
        - model: trained RandomForestClassifier
        - accuracy: accuracy on test set
        - features: feature matrix (X) used for training
        - label_encoder: LabelEncoder for activity names
        - feature_names: list of feature column names
    """
    # Build features
    featured_df = _build_event_features(df)
    le_activity = featured_df.attrs['activity_label_encoder']

    # Create target: next activity in the same case
    featured_df['next_activity'] = featured_df.groupby('case:concept:name')['concept:name'].shift(-1)

    # Drop last event of each case (no next activity)
    train_df = featured_df.dropna(subset=['next_activity']).copy()

    if len(train_df) < 10:
        raise ValueError("Not enough data to train next activity model (need at least 10 transitions)")

    # Encode target
    le_target = LabelEncoder()
    train_df['next_activity_encoded'] = le_target.fit_transform(train_df['next_activity'])

    # Define features
    feature_names = ['activity_encoded', 'position_in_case', 'elapsed_time', 'events_so_far']
    X = train_df[feature_names].values
    y = train_df['next_activity_encoded'].values

    # Split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y if len(np.unique(y)) > 1 else None
    )

    # Train
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=15,
        min_samples_split=5,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1
    )
    model.fit(X_train, y_train)

    # Evaluate
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)

    return {
        'model': model,
        'accuracy': accuracy,
        'features': X,
        'label_encoder': le_target,
        'activity_label_encoder': le_activity,
        'feature_names': feature_names
    }


def train_remaining_time_model(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Train a model to predict the remaining time in a case.

    For each event, the target is: total_case_duration - elapsed_time.

    Features:
    - activity_encoded: current activity (LabelEncoded)
    - position_in_case: relative position in the case
    - elapsed_time: seconds since case start
    - events_so_far: number of events completed

    Args:
        df: Event log DataFrame with columns:
            case:concept:name, concept:name, time:timestamp

    Returns:
        Dictionary containing:
        - model: trained RandomForestRegressor
        - mae: mean absolute error on test set (in seconds)
        - r2: R-squared score on test set
        - features: feature matrix (X) used for training
        - feature_names: list of feature column names
    """
    # Build features
    featured_df = _build_event_features(df)
    le_activity = featured_df.attrs['activity_label_encoder']

    # Calculate total case duration for each case
    case_durations = df.groupby('case:concept:name')['time:timestamp'].agg(['min', 'max'])
    case_durations['total_duration'] = (case_durations['max'] - case_durations['min']).dt.total_seconds()

    # Merge total duration into featured_df
    featured_df = featured_df.merge(
        case_durations[['total_duration']],
        left_on='case:concept:name',
        right_index=True,
        how='left'
    )

    # Target: remaining time = total duration - elapsed time
    featured_df['remaining_time'] = featured_df['total_duration'] - featured_df['elapsed_time']

    # Remove rows where remaining time is negative (shouldn't happen but safety check)
    train_df = featured_df[featured_df['remaining_time'] >= 0].copy()

    if len(train_df) < 10:
        raise ValueError("Not enough data to train remaining time model (need at least 10 events)")

    # Define features
    feature_names = ['activity_encoded', 'position_in_case', 'elapsed_time', 'events_so_far']
    X = train_df[feature_names].values
    y = train_df['remaining_time'].values

    # Split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Train
    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=15,
        min_samples_split=5,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1
    )
    model.fit(X_train, y_train)

    # Evaluate
    y_pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    return {
        'model': model,
        'mae': mae,
        'r2': r2,
        'features': X,
        'activity_label_encoder': le_activity,
        'feature_names': feature_names
    }


def train_outcome_model(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Train a model to predict the case outcome (last activity in the case).

    For each event, the target is the last activity that will occur in
    that event's case.

    Features:
    - activity_encoded: current activity (LabelEncoded)
    - position_in_case: relative position in the case
    - elapsed_time: seconds since case start
    - events_so_far: number of events completed

    Args:
        df: Event log DataFrame with columns:
            case:concept:name, concept:name, time:timestamp

    Returns:
        Dictionary containing:
        - model: trained RandomForestClassifier
        - accuracy: accuracy on test set
        - features: feature matrix (X) used for training
        - label_encoder: LabelEncoder for outcome activity names
        - feature_names: list of feature column names
    """
    # Build features
    featured_df = _build_event_features(df)
    le_activity = featured_df.attrs['activity_label_encoder']

    # Determine last activity for each case
    df_sorted = df.sort_values(['case:concept:name', 'time:timestamp'])
    last_activities = df_sorted.groupby('case:concept:name')['concept:name'].last()

    # Merge outcome into featured_df
    featured_df = featured_df.merge(
        last_activities.rename('outcome'),
        left_on='case:concept:name',
        right_index=True,
        how='left'
    )

    # Exclude the last event of each case from training
    # (at the last event, we already know the outcome -- it's trivially the current activity)
    train_df = featured_df[featured_df['event_index'] < featured_df['case_size']].copy()

    if len(train_df) < 10:
        raise ValueError("Not enough data to train outcome model (need at least 10 non-final events)")

    # Encode target
    le_target = LabelEncoder()
    train_df['outcome_encoded'] = le_target.fit_transform(train_df['outcome'])

    # Define features
    feature_names = ['activity_encoded', 'position_in_case', 'elapsed_time', 'events_so_far']
    X = train_df[feature_names].values
    y = train_df['outcome_encoded'].values

    # Only stratify if we have enough samples per class
    unique_classes, class_counts = np.unique(y, return_counts=True)
    can_stratify = len(unique_classes) > 1 and all(c >= 2 for c in class_counts)

    # Split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42,
        stratify=y if can_stratify else None
    )

    # Train
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=15,
        min_samples_split=5,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1
    )
    model.fit(X_train, y_train)

    # Evaluate
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)

    return {
        'model': model,
        'accuracy': accuracy,
        'features': X,
        'label_encoder': le_target,
        'activity_label_encoder': le_activity,
        'feature_names': feature_names
    }
