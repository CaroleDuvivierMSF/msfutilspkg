# Fabric function requirements
# packages:
#   - pandas
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def sync_dataframes_with_old_new(
    newRecords: pd.DataFrame,
    historic: pd.DataFrame,
    key: list, 
    showChangedCol: bool,
) -> dict:
    """
    Synchronize two DataFrames (new vs. historic) and detect record-level changes.

    This function compares a set of new records against an existing (historic) dataset
    using a unique key column, and classifies each record into one of four categories:
    created, updated, deleted, or unchanged.

    It is commonly used for incremental data synchronization, audit tracking, or 
    preparing database update scripts based on a DataFrame comparison.

    Parameters
    ----------
        newRecords : pandas.DataFrame
            The incoming dataset containing the most recent records.
        historic : pandas.DataFrame
            The reference or previously stored dataset to compare against.
        key : str
            The column name that uniquely identifies each record across both DataFrames.
        showChangedCol: bool, optional
            If True, the `to_update` DataFrame will include a `changed_columns` column
            listing which non-key fields were modified, as well as show the previous values. Default is False.

    Returns
    -------
        dict of {str: pandas.DataFrame}
            A dictionary with the following DataFrames:

            - **to_create** : Records present in `newRecords` but not in `historic`.
            - **to_update** : Records where the same key exists in both DataFrames but 
            at least one non-key field differs (the returned DataFrame contains the 
            *new values* from `newRecords`).
            - **to_delete** : Records present in `historic` but not in `newRecords`.
            - **to_keep** : Records identical in both DataFrames (no change detected).

    Notes
    -----
    - The comparison uses exact equality (`pandas.Series.equals`) on row level.
      Subtle type differences (e.g., `1` vs. `1.0`) may cause updates to be detected.
    - The function assumes `key` uniquely identifies each record within both DataFrames.
      Duplicate key values can lead to ambiguous comparisons.
    - Returned DataFrames are *independent* slices of the originals.

    Examples
    --------
    >>> import pandas as pd
    >>> old = pd.DataFrame({
    ...     "id": [1, 2, 3],
    ...     "name": ["Alice", "Bob", "Charlie"],
    ...     "status": ["active", "inactive", "active"]
    ... })
    >>> new = pd.DataFrame({
    ...     "id": [2, 3, 4],
    ...     "name": ["Bob", "Charles", "Diana"],
    ...     "status": ["inactive", "active", "active"]
    ... })
    >>> result = sync_dataframes(new, old, key="id")
    >>> result["to_create"]
       id   name  status
    2   4  Diana  active
    >>> result["to_update"]
       id     name  status
    1   3  Charles  active
    >>> result["to_delete"]
       id    name    status
    0   1   Alice    active
    >>> result["to_keep"]
       id  name    status
    0   2   Bob   inactive

    See Also
    --------
    pandas.DataFrame.compare : Element-wise DataFrame comparison.
    pandas.merge : SQL-style DataFrame joins, useful for alternative diff logic.

    """
    # --- To create ---
    merged_create = newRecords.merge(historic[key], on=key, how="left", indicator=True)
    to_create = merged_create.loc[merged_create["_merge"] == "left_only"].drop(columns=["_merge"])
    to_create["changed_columns"] = None # For consistency in columns with the update case
    to_create["type_of_change"] = "Create"

    # --- To delete ---
    merged_delete = historic.merge(newRecords[key], on=key, how="left", indicator=True)
    to_delete = merged_delete.loc[merged_delete["_merge"] == "left_only"].drop(columns=["_merge"])
    to_delete["type_of_change"] = "Delete"
    to_delete["changed_columns"] = None # For consistency in columns with the update case

    # --- Merge for comparison ---
    merged = newRecords.merge(
        historic,
        on=key,
        how="inner",
        suffixes=('_new', '_old')
    )

    # Non-key columns
    non_key_cols = [col for col in newRecords.columns if col not in key]
    old_non_key_cols = ["old_" + col for col in non_key_cols]

    for col in old_non_key_cols:
        to_create[col] = None  # For consistency in columns with the update case
        to_delete[col] = None  # For consistency in columns with the update case

    if merged.empty:
        return {"to_create": to_create, "to_update": pd.DataFrame(columns = non_key_cols + old_non_key_cols), "to_delete": to_delete, "to_keep": pd.DataFrame(columns = non_key_cols + old_non_key_cols)}

    # Vectorized comparison: boolean mask of differences
    diff_mask = merged[[col + '_new' for col in non_key_cols]].values != merged[[col + '_old' for col in non_key_cols]].values
    # For each row, list of changed columns
    changed_columns = pd.DataFrame(diff_mask, columns=non_key_cols).apply(lambda row: list(row.index[row]), axis=1)
    has_change = changed_columns.apply(len) > 0
          
    # --- Construct to_keep DataFrame ---
    to_keep = merged.loc[~has_change, [*key, *[col + '_new' for col in non_key_cols]]].copy()
    to_keep.columns = [*key, *non_key_cols]
    to_keep["type_of_change"] = "No change"
    to_keep["changed_columns"] = None  # For consistency in columns with the update case

    # --- Construct to_update DataFrame with old and new values ---
    to_update = merged.loc[has_change, key].copy()

    for col in non_key_cols:
        to_update[f'old_{col}'] = merged.loc[has_change, f'{col}_old'].values
        to_update[col] = merged.loc[has_change, f'{col}_new'].values

    to_update['changed_columns'] = changed_columns[has_change].values

    to_update = to_update[key + non_key_cols if showChangedCol is False else to_update.columns]
    to_update["type_of_change"] = "Update"

    return {
        "to_create": to_create,
        "to_update": to_update,
        "to_delete": to_delete,
        "to_keep": to_keep
    }
