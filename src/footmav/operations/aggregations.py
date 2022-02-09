from footmav.operations.pipeable import pipeable
from footmav.data_definitions.base import DataAttribute, RegisteredAttributeStore
from footmav.data_definitions.derived import DerivedDataAttribute
import pandas as pd
from typing import List


@pipeable
def aggregate_by(
    data: pd.DataFrame,
    aggregate_cols: List[DataAttribute],
):
    """
    Aggregates the dataframe by the given columns.
    """
    grouping = data.groupby([c.N for c in aggregate_cols])
    transforms = {
        c.N: c.agg_function
        for c in RegisteredAttributeStore.get_registered_attributes()
        if c.N in data.columns and c.agg_function is not None
    }
    df_agg = grouping.agg(transforms)
    recalcs = [
        c
        for c in RegisteredAttributeStore.get_registered_attributes()
        if c.N in data.columns
        and isinstance(c, DerivedDataAttribute)
        and c.recalculate_on_aggregation
    ]
    for c in recalcs:
        df_agg[c.N] = c.apply(df_agg)
    indx_cols = df_agg.index.names

    df_agg = df_agg[list(set(df_agg.columns) - set(indx_cols))]
    return df_agg.reset_index()


@pipeable
def rank(data: pd.DataFrame, pct: bool = True) -> pd.DataFrame:
    ranked = data.rank(pct=pct, numeric_only=True)

    missing_cols = set(data.columns) - set(ranked.columns)

    for c in missing_cols:
        ranked[c] = data[c]
    return ranked
