from typing import Callable, Union
from footmav.data_definitions.base import DataAttribute
from footmav.data_definitions.derived import DerivedDataAttribute, lambda_attribute
from footmav.data_definitions.data_sources import DataSource
from footmav.data_definitions.fbref import fbref_columns as fb
import pandas as pd


def opposition_attribute(
    attribute: DataAttribute,
) -> Union[Callable[[Callable], DerivedDataAttribute], DerivedDataAttribute]:
    def _opposition_attr_inner(data: pd.DataFrame) -> pd.Series:
        opp_data = (
            data.groupby([fb.OPPONENT.N, fb.DATE.N])
            .agg({attribute.N: "sum"})
            .reset_index()
            .rename(columns={attribute.N: "opposition_" + attribute.N})
        )
        pd_merge = pd.merge(
            data,
            opp_data,
            left_on=[fb.TEAM.N, fb.DATE.N],
            right_on=[fb.OPPONENT.N, fb.DATE.N],
            how="left",
        )
        return pd_merge["opposition_" + attribute.N]

    _opposition_attr_inner.__name__ = "opposition_" + attribute.N
    return lambda_attribute(_opposition_attr_inner, data_source=DataSource.FBREF)
