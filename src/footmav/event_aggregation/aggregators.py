from footmav.data_definitions.whoscored.constants import EventType
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


@event_aggregator
def shots_on_target(dataframe):
    return (WF.is_shot_on_target(dataframe)) & (
        ~WF.col_has_qualifier(dataframe, "Penalty")
    )


@event_aggregator
def shots(dataframe):
    return (WF.is_shot(dataframe)) & (~WF.col_has_qualifier(dataframe, "Penalty"))


@event_aggregator(success="completed")
def progressive_passes(dataframe):
    return passes(dataframe) & WF.is_progressive(dataframe)


@event_aggregator(success="completed")
def diagonals(dataframe):
    return progressive_passes(dataframe) & switches(dataframe)


@event_aggregator(success="completed")
def passes_into_area(dataframe):
    return passes(dataframe) & WF.in_attacking_box(dataframe, False)


@event_aggregator
def tackles(dataframe):
    return (dataframe["event_type"] == EventType.Tackle) | (
        dataframe["event_type"] == EventType.Challenge
    )


@event_aggregator
def tackles_successful(dataframe):
    return dataframe["event_type"] == EventType.Tackle


@event_aggregator
def interceptions(dataframe):
    return dataframe["event_type"] == EventType.Interception


@event_aggregator
def fouls_won(dataframe):
    return (dataframe["event_type"] == EventType.Foul) & (dataframe["outcomeType"] == 1)


@event_aggregator
def fouls_conceded(dataframe):
    return (dataframe["event_type"] == EventType.Foul) & (dataframe["outcomeType"] == 0)


@event_aggregator(success="won")
def aerials(dataframe):
    return (dataframe["event_type"] == EventType.Aerial) | (
        (dataframe["event_type"] == EventType.Foul)
        & WF.col_has_qualifier(dataframe, qualifier_code=264)
    )


@event_aggregator
def ground_duels(dataframe):
    return (
        (dataframe["event_type"] == EventType.TakeOn)
        | (
            (dataframe["event_type"] == EventType.Foul)
            & (~WF.col_has_qualifier(dataframe, qualifier_code=264))
        )
        | (dataframe["event_type"] == EventType.Tackle)
        | (dataframe["event_type"] == EventType.Challenge)
        | (dataframe["event_type"] == EventType.Smother)
        | (dataframe["event_type"] == EventType.Dispossessed)
    )


@event_aggregator(suffix="")
def ground_duels_won(dataframe):
    return (
        (
            (dataframe["event_type"] == EventType.TakeOn)
            | (
                (dataframe["event_type"] == EventType.Foul)
                & (~WF.col_has_qualifier(dataframe, qualifier_code=264))
            )
            | (dataframe["event_type"] == EventType.Smother)
        )
        & (dataframe["outcomeType"] == 1)
    ) | (dataframe["event_type"] == EventType.Tackle)


@event_aggregator(suffix="")
def ground_duels_lost(dataframe):
    return (
        (
            (dataframe["event_type"] == EventType.TakeOn)
            | (
                (dataframe["event_type"] == EventType.Foul)
                & (~WF.col_has_qualifier(dataframe, qualifier_code=264))
            )
            | (dataframe["event_type"] == EventType.Challenge)
            | (dataframe["event_type"] == EventType.Smother)
        )
        & (dataframe["outcomeType"] == 0)
    ) | (dataframe["event_type"] == EventType.Dispossessed)


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


@event_aggregator(suffix="")
def big_chances(data):

    return WF.is_shot(data) & (xg(data) > 0.25)


@event_aggregator(suffix="")
def yellow_cards(data):
    return (data["event_type"] == EventType.Card) & WF.col_has_qualifier(
        data, qualifier_code=31
    )


@event_aggregator(suffix="")
def red_cards(data):
    return (data["event_type"] == EventType.Card) & (
        WF.col_has_qualifier(data, qualifier_code=32)
        | WF.col_has_qualifier(data, qualifier_code=33)
    )


@event_aggregator(suffix="")
def keeper_saves(dataframe):
    return (
        (dataframe["event_type"] == EventType.Save)
        & (dataframe["outcomeType"] == 1)
        & (~WF.col_has_qualifier(dataframe, qualifier_code=94))
    )


@event_aggregator(suffix="")
def npxgot(dataframe):
    return npxg(dataframe) * shots_on_target(dataframe).astype(int)
