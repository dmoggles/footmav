import pandas as pd
from typing import List
from footmav.operations.filter_objects import Filter
from footmav.operations.pipeable import pipeable


@pipeable
def filter(input_data: pd.DataFrame, filters: List[Filter]) -> pd.DataFrame:
    """Applies selected filters to data

    Args:
        input_data (pd.DataFrame): Data to be filtered
        filters (List[Filter]): Filters to be applied

    Returns:
        pd.DataFrame: Filtered data
    """
    df = input_data.copy()
    for filter_ in filters:
        df = filter_.apply(df)
    return df
