"""
ML Predictions page for QuickMineLite.

Provides training and evaluation of three process-mining ML models:
- Next Activity prediction (classification)
- Remaining Time prediction (regression)
- Case Outcome prediction (classification)

Uses scikit-learn RandomForest models from analysis.ml_engine with
process-aware feature engineering.
"""

import streamlit as st
import pandas as pd
import numpy as np
from analysis.ml_engine import (
    train_next_activity_model,
    train_remaining_time_model,
    train_outcome_model,
)
from viz.charts import ChartBuilder
from core.helpers import format_duration, format_number

# ---------------------------------------------------------------------------
# Guard: require loaded data
# ---------------------------------------------------------------------------
if st.session_state.get('event_log_df') is None:
    st.info("No data loaded. Go to Import Data.")
    st.stop()

st.header("ML Predictions")

# ---------------------------------------------------------------------------
# Retrieve shared objects
# ---------------------------------------------------------------------------
filtered_df: pd.DataFrame = st.session_state['filtered_df']
chart_builder = ChartBuilder()


def _show_feature_importance(model_result: dict, title: str):
    """Display feature importance bar chart for a trained model."""
    model = model_result['model']
    feature_names = model_result['feature_names']
    importances = model.feature_importances_

    imp_df = pd.DataFrame({
        'feature': feature_names,
        'importance': importances,
    }).sort_values('importance', ascending=False)

    fig = chart_builder.create_bar_chart(
        imp_df, 'feature', 'importance',
        title,
        x_label="Feature", y_label="Importance",
    )
    st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab_next, tab_time, tab_outcome = st.tabs(
    ["Next Activity", "Remaining Time", "Case Outcome"]
)

# ---- Next Activity Tab ----------------------------------------------------
with tab_next:
    st.subheader("Next Activity Prediction")
    st.markdown(
        "Predicts the **next activity** that will occur in a running case "
        "based on the current activity, position in the case, elapsed time, "
        "and number of events so far. Uses a Random Forest classifier."
    )

    # Check if model already trained
    model_key = 'ml_next_activity'
    if model_key in st.session_state and st.session_state[model_key] is not None:
        result = st.session_state[model_key]
        st.success("Model already trained. Showing stored results.")
        st.metric("Accuracy", f"{result['accuracy']:.3f}")
        _show_feature_importance(result, "Feature Importance -- Next Activity")
        if st.button("Retrain Model", key="retrain_next"):
            st.session_state[model_key] = None
            st.rerun()
    else:
        if st.button("Train Model", key="train_next", type="primary"):
            with st.spinner("Training Next Activity model..."):
                try:
                    result = train_next_activity_model(filtered_df)
                    st.session_state[model_key] = result
                    st.success("Model trained successfully.")
                    st.metric("Accuracy", f"{result['accuracy']:.3f}")
                    _show_feature_importance(result, "Feature Importance -- Next Activity")
                except ValueError as e:
                    st.error(f"Training failed: {e}")
                except Exception as e:
                    st.error(f"An unexpected error occurred: {e}")

# ---- Remaining Time Tab ---------------------------------------------------
with tab_time:
    st.subheader("Remaining Time Prediction")
    st.markdown(
        "Predicts the **remaining time** until case completion for each event "
        "in a running case. Uses a Random Forest regressor. Evaluated with "
        "Mean Absolute Error (MAE) and R-squared (R2)."
    )

    model_key = 'ml_remaining_time'
    if model_key in st.session_state and st.session_state[model_key] is not None:
        result = st.session_state[model_key]
        st.success("Model already trained. Showing stored results.")
        col1, col2 = st.columns(2)
        col1.metric("MAE", format_duration(result['mae']))
        col2.metric("R2 Score", f"{result['r2']:.3f}")
        _show_feature_importance(result, "Feature Importance -- Remaining Time")
        if st.button("Retrain Model", key="retrain_time"):
            st.session_state[model_key] = None
            st.rerun()
    else:
        if st.button("Train Model", key="train_time", type="primary"):
            with st.spinner("Training Remaining Time model..."):
                try:
                    result = train_remaining_time_model(filtered_df)
                    st.session_state[model_key] = result
                    st.success("Model trained successfully.")
                    col1, col2 = st.columns(2)
                    col1.metric("MAE", format_duration(result['mae']))
                    col2.metric("R2 Score", f"{result['r2']:.3f}")
                    _show_feature_importance(result, "Feature Importance -- Remaining Time")
                except ValueError as e:
                    st.error(f"Training failed: {e}")
                except Exception as e:
                    st.error(f"An unexpected error occurred: {e}")

# ---- Case Outcome Tab -----------------------------------------------------
with tab_outcome:
    st.subheader("Case Outcome Prediction")
    st.markdown(
        "Predicts the **final activity** (outcome) of a case based on "
        "information available at each event in the case. Uses a Random "
        "Forest classifier. Useful for predicting how a case will end "
        "while it is still in progress."
    )

    model_key = 'ml_outcome'
    if model_key in st.session_state and st.session_state[model_key] is not None:
        result = st.session_state[model_key]
        st.success("Model already trained. Showing stored results.")
        st.metric("Accuracy", f"{result['accuracy']:.3f}")
        _show_feature_importance(result, "Feature Importance -- Case Outcome")
        if st.button("Retrain Model", key="retrain_outcome"):
            st.session_state[model_key] = None
            st.rerun()
    else:
        if st.button("Train Model", key="train_outcome", type="primary"):
            with st.spinner("Training Case Outcome model..."):
                try:
                    result = train_outcome_model(filtered_df)
                    st.session_state[model_key] = result
                    st.success("Model trained successfully.")
                    st.metric("Accuracy", f"{result['accuracy']:.3f}")
                    _show_feature_importance(result, "Feature Importance -- Case Outcome")
                except ValueError as e:
                    st.error(f"Training failed: {e}")
                except Exception as e:
                    st.error(f"An unexpected error occurred: {e}")
