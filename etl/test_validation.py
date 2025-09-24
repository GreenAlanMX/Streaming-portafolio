"""
Unit tests for validation rules in the ETL pipeline.
Run with: pytest test_validation.py
"""

import pandas as pd
import pytest
from etl_pipeline_enhanced import validate_users, validate_sessions, validate_content

def test_validate_users_age():
    df = pd.DataFrame({"age": [12], "total_watch_time_hours": [100]})
    with pytest.raises(AssertionError):
        validate_users(df)

def test_validate_users_watch_time():
    df = pd.DataFrame({"age": [20], "total_watch_time_hours": [-5]})
    with pytest.raises(AssertionError):
        validate_users(df)

def test_validate_sessions_completion():
    df = pd.DataFrame({"completion_percentage": [105], "watch_duration_minutes": [50]})
    with pytest.raises(AssertionError):
        validate_sessions(df)

def test_validate_sessions_duration():
    df = pd.DataFrame({"completion_percentage": [90], "watch_duration_minutes": [-10]})
    with pytest.raises(AssertionError):
        validate_sessions(df)

def test_validate_content_year():
    df = pd.DataFrame({"release_year": [1800], "rating": [4]})
    with pytest.raises(AssertionError):
        validate_content(df)

def test_validate_content_rating():
    df = pd.DataFrame({"release_year": [2023], "rating": [6]})
    with pytest.raises(AssertionError):
        validate_content(df)
