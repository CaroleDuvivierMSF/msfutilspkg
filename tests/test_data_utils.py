# tests/test_sync_dataframes.py
import pandas as pd
import numpy as np
from msfutilspkg.utils.data_utils import sync_dataframes_with_old_new, enforce_schema

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

    # Assertions
    # Int64 columns: all invalid values should become pd.NA
    for col in ["positionNumber", "assignment_duration", "post_reference"]:
        assert df_clean[col].dtype.name == "Int64"
        assert pd.isna(df_clean.loc[2, col])  # NaN
        assert pd.isna(df_clean.loc[3, col])  # Inf
        assert pd.isna(df_clean.loc[4, col])  # -Inf

    # Boolean columns
    for col in ["is_opportunity_post", "position_closed", "is_first_mission", "is_family_position"]:
        assert df_clean[col].dtype.name == "boolean"

    # Datetime columns
    assert pd.api.types.is_datetime64_ns_dtype(df_clean["date_start_effective"])
    assert pd.isna(df_clean.loc[1, "date_start_effective"])  # invalid date should become NaT

    # String columns
    for col in ["project_code", "irffg_code", "irffg_title", "post_title"]:
        assert df_clean[col].dtype.name == "string"
        assert df_clean.loc[2, col] in (None, pd.NA)  # None for missing strings
