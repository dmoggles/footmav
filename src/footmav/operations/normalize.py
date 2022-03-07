from footmav.data_definitions.derived import DerivedDataAttribute
from footmav.operations.pipeable import pipeable
import pandas as pd
from footmav.data_definitions.base import RegisteredAttributeStore
from pandas.api.types import is_numeric_dtype
from footmav.data_definitions.fbref.fbref_columns import (
    MINUTES,
)  # not elegant, but this is currently the only datasource that is not event based, and thus the only one that can be normalized.


@pipeable
def per_90(data: pd.DataFrame) -> pd.DataFrame:
    """
    Returns data normalized to per 90 minutes
    """

    if MINUTES.N not in data.columns:
        raise ValueError(f"{MINUTES.N} not in data columns")

    data = data.copy()
    columns_to_normalize = [
        c
        for c in RegisteredAttributeStore.get_registered_attributes()
        if c.N in data.columns and is_numeric_dtype(data[c.N]) and c.normalizable
    ]
    for c in columns_to_normalize:
        data[c.N] = data[c.N] / data[MINUTES.N] * 90

    columns_to_recalculate = [
        c
        for c in RegisteredAttributeStore.get_registered_attributes()
        if isinstance(c, DerivedDataAttribute)
        and c.N in data.columns
        and c.recalculate_on_aggregation
    ]
    for c in columns_to_recalculate:
        data[c.N] = c.apply(data)

    return data


@pipeable
def z_score(data: pd.DataFrame, inv: bool = False) -> pd.DataFrame:
    """
    Convert to normal distribution
    """

    def _column_norm(s: pd.Series) -> pd.Series:
        try:
            return (s.mean() - s if inv else s - s.mean()) / s.std()
        except (ValueError, TypeError):
            return s

    data = data.apply(_column_norm)
    return data
