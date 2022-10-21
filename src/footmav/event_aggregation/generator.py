import pandas as pd
from footmav.event_aggregation.event_aggregator_processor import (
    EventAggregationProcessor,
)
from footmav.utils import whoscored_funcs as WF


def generate_aggregate_dataframe(
    dataframe: pd.DataFrame, persistent_only=True
) -> pd.DataFrame:
    agg_data = dataframe.copy()
    aggregated_cols = []
    for aggregator in EventAggregationProcessor.aggregators.values():
        if persistent_only and not aggregator.persistent:
            continue
        aggregator(agg_data)
        aggregated_cols.append(aggregator.name)
        for name, f in aggregator.extra_functions.items():
            aggregated_cols.append(name)
            agg_data[name] = f(agg_data)

    collected = agg_data.groupby(
        ["matchId", "match_date", "player_name", "team", "opponent"]
    ).agg({k: "sum" for k in aggregated_cols})
    collected["minutes"] = pd.merge(
        collected, WF.minutes(agg_data), on=["matchId", "player_name"], how="left"
    )["minutes"].tolist()
