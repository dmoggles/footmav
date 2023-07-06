import pandas as pd
from footmav.event_aggregation.event_aggregator_processor import (
    EventAggregationProcessor,
)
from footmav.utils import whoscored_funcs as WF


def generate_aggregate_dataframe(
    dataframe: pd.DataFrame, persistent_only=True, groups=None
) -> pd.DataFrame:
    agg_data = dataframe.copy()
    aggregated_cols = []
    for aggregator in EventAggregationProcessor.aggregators.values():
        if (persistent_only and not aggregator.persistent) or (
            groups and aggregator.group not in groups
        ):
            continue
        aggregator(agg_data)
        aggregated_cols.append((aggregator.group, aggregator.col_name))
        for name, f in aggregator.extra_functions.items():
            aggregated_cols.append((aggregator.group, name))
            agg_data[(aggregator.group, name)] = f(agg_data)

    collected = agg_data.groupby(
        [
            "matchId",
            "match_date",
            "competition",
            "season",
            "player_name",
            "team",
            "opponent",
            "position",
            "is_home_team",
        ]
    ).sum()[
        aggregated_cols
    ]  # agg({k: "sum" for k in aggregated_cols})
    minutes_df = WF.minutes_per_position(dataframe)
    _merge = pd.merge(
        collected, minutes_df, on=["matchId", "player_name", "position"], how="left"
    )

    collected[("minutes", "minutes")] = _merge["minutes"].tolist()
    collected.columns = pd.MultiIndex.from_tuples(collected.columns)
    dataframe = collected.reset_index().rename(
        columns={
            "matchId": "match_id",
            "is_home_team": "is_home",
            "competition": "comp",
        }
    )

    return dataframe
