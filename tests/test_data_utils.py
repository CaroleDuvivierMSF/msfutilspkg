# tests/test_sync_dataframes.py
import pandas as pd
import numpy as np
from msfutilspkg.utils.data_utils import sync_dataframes_with_old_new, enforce_schema, pandas_to_spark_schema

def test_basic_diff():
    old = pd.DataFrame({"id": [1,2], "name": ["A","B"], "status": ["x","y"]})
    new = pd.DataFrame({"id": [2,3], "name": ["B","C"], "status": ["y","z"]})
    result = sync_dataframes_with_old_new(new, old, key=["id"], showChangedCol=True)
    assert not result["to_create"].empty
    assert "to_update" in result

def test_enforce_schema_for_lakehouse():
    # Sample input DataFrame with various bad data
    df = pd.DataFrame({
        "positionNumber": [1, 2, np.nan, np.inf, -np.inf],
        "assignment_duration": [12, np.nan, np.inf, -np.inf, 6],
        "is_opportunity_post": [True, False, None, True, False],
        "position_closed": [False, None, True, False, None],
        "date_start_effective": ["2026-01-01", "not a date", None, "2026-01-03", "2026-01-04"],
        "project_code": ["ABC123", None, "DEF456", "GHI789", np.nan],
        "post_reference": [1001, 1002, np.nan, np.inf, -np.inf],
        "irffg_code": ["IRF1", "IRF2", None, "IRF4", "IRF5"],
        "irffg_title": ["Title1", "Title2", "Title3", None, "Title5"],
        "post_title": ["Post1", None, "Post3", "Post4", "Post5"],
        "is_first_mission": [True, None, False, True, False],
        "is_family_position": [None, True, False, None, True]
    })

    schema = {
        "positionNumber":"Int64",
        "assignment_duration":"Int64",
        "is_opportunity_post":"boolean",
        "position_closed":"boolean",
        "date_start_effective":"datetime64[ns]",
        "project_code":"str",
        "post_reference":"Int64",
        "irffg_code":"str",
        "irffg_title":"str",
        "post_title":"str",
        "is_first_mission":"boolean",
        "is_family_position":"boolean"
    }
    # Apply schema enforcement
    df_clean = enforce_schema(df, schema)

    # ------------------------------
    # Int64 columns: all NaN/Inf/-Inf become pd.NA
    # ------------------------------
    for col in ["positionNumber", "assignment_duration", "post_reference"]:
        assert df_clean[col].dtype.name == "Int64"
        # Check that no remaining inf or nan values exist
        assert not df_clean[col].isin([np.inf, -np.inf]).any()
        # pd.NA is accepted in nullable Int64, so there may be missing values
        assert df_clean[col].isna().sum() > 0

    # ------------------------------
    # Boolean columns
    # ------------------------------
    for col in ["is_opportunity_post", "position_closed", "is_first_mission", "is_family_position"]:
        assert df_clean[col].dtype.name == "boolean"

    # ------------------------------
    # Datetime columns
    # ------------------------------
    assert pd.api.types.is_datetime64_ns_dtype(df_clean["date_start_effective"])
    # Check invalid date was converted to NaT
    assert pd.isna(df_clean["date_start_effective"].iloc[1])

    # ------------------------------
    # String columns
    # ------------------------------
    for col in ["project_code", "irffg_code", "irffg_title", "post_title"]:
        assert df_clean[col].dtype.name in ["string", "object"]
        # Ensure missing values are None or pd.NA
        missing_mask = df_clean[col].isna()
        if missing_mask.any():
            assert all(df_clean.loc[missing_mask, col].isna())


import pytest
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    BooleanType,
    StringType,
    TimestampType,
)

def test_pandas_to_spark_schema_basic():
    pandas_schema = {
        "job_id": "str",
        "timestamp": "datetime64[ns]",
        "projectCode": "str",
        "positionNumber": "Int64",
        "contractLengthInMonths": "Int64",
        "isOpportunityPost": "boolean",
        "isPositionClosed": "boolean",
    }

    spark_schema = pandas_to_spark_schema(pandas_schema)

    # -----------------------
    # Basic structural checks
    # -----------------------
    assert isinstance(spark_schema, StructType)
    assert len(spark_schema.fields) == len(pandas_schema)

    # -----------------------
    # Expected Spark types
    # -----------------------
    expected = {
        "job_id": StringType,
        "timestamp": TimestampType,
        "projectCode": StringType,
        "positionNumber": LongType,
        "contractLengthInMonths": LongType,
        "isOpportunityPost": BooleanType,
        "isPositionClosed": BooleanType,
    }

    for field in spark_schema.fields:
        # Correct column name
        assert field.name in expected

        # Correct Spark datatype
        assert isinstance(field.dataType, expected[field.name]), (
            f"Column {field.name} has wrong Spark type {field.dataType}"
        )

        # Always nullable (important for Fabric!)
        assert field.nullable is True

