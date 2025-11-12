# tests/test_sync_dataframes.py
import pandas as pd
from msfutilspkg.utils.data_utils import sync_dataframes_with_old_new

def test_basic_diff():
    old = pd.DataFrame({"id": [1,2], "name": ["A","B"], "status": ["x","y"]})
    new = pd.DataFrame({"id": [2,3], "name": ["B","C"], "status": ["y","z"]})
    result = sync_dataframes_with_old_new(new, old, key=["id"])
    assert not result["to_create"].empty
    assert "to_update" in result
