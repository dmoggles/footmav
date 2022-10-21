from tkinter import EventType
from footmav.event_aggregation.event_aggregator_processor import event_aggregator
from footmav.utils import whoscored_funcs as WF
import pandas as pd


@event_aggregator(success="completed", vertical_areas=5)
def open_play_passes(dataframe):
    return WF.open_play_pass_attempt(dataframe)

@event_aggregator(success="completed", vertical_areas=5)
def passes(dataframe):
    return WF.pass_attempt(dataframe)


@event_aggregator(vertical_areas=5)
def touches(dataframe):
    return WF.is_touch(dataframe)


@event_aggregator
def xg(dataframe):
    return dataframe["xG"]


@event_aggregator
def npxg(dataframe):
    return dataframe["xG"] * (
        ~WF.col_has_qualifier(dataframe, display_name="Penalty")
    ).astype(int)


@event_aggregator(success="completed")
def crosses(dataframe):
    return WF.cross_attempt(dataframe)


@event_aggregator(success="completed")
def throughballs(dataframe):
    return passes(dataframe) & (WF.col_has_qualifier(dataframe, qualifier_code=4))


@event_aggregator(success="completed")
def switches(dataframe):
    return passes(dataframe) & (abs(dataframe["y"] - dataframe["endY"]) > 50)


@event_aggregator(success="completed")
def progressive_passes(dataframe):
    return passes(dataframe) & WF.is_progressive(dataframe)


@event_aggregator(success="completed")
def diagonals(dataframe):
    return progressive_passes(dataframe) & switches(dataframe)


@event_aggregator(success="completed")
def passes_into_area(dataframe):
    return passes(dataframe) & WF.in_attacking_box(dataframe, False)


@event_aggregator(suffix="")
def xa(dataframe):
    temp = dataframe.copy()
    temp["assist_id"] = (
        WF.col_get_qualifier_value(dataframe, qualifier_code=55)
        .fillna(-978)
        .astype(int)
    )
    return (
        pd.merge(
            temp,
            temp[["matchId", "teamId", "assist_id", "xG"]],
            left_on=["matchId", "teamId", "eventId"],
            right_on=["matchId", "teamId", "assist_id"],
            suffixes=("", "_assisted"),
            how="left",
        )["xG_assisted"]
        .fillna(0)
        .tolist()
    )


@event_aggregator(success="completed")
def open_play_balls_into_box(dataframe):
    return (
        (dataframe["event_type"] == EventType.Pass)
        & WF.in_attacking_box(dataframe, False).astype(int)
        & (~WF.col_has_qualifier(dataframe, qualifier_code=5))
        & (~WF.col_has_qualifier(dataframe, qualifier_code=6))
        & (~WF.col_has_qualifier(dataframe, qualifier_code=5))
        & (~WF.col_has_qualifier(dataframe, qualifier_code=6))
    )


@event_aggregator(suffix="")
def xa_in_area(dataframe):
    return xa(dataframe) * open_play_balls_into_box(dataframe).astype(int)
