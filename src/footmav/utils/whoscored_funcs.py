from typing import Callable, List, Tuple
import requests  # type: ignore
import pandas as pd
import numpy as np
from footmav.data_definitions.whoscored.constants import (
    BOTTOM_GOAL_COORDS,
    MIDDLE_GOAL_COORDS,
    TOP_GOAL_COORDS,
    EventType,
    PassType,
)
from typing import Dict, Any
from footmav.data_definitions.whoscored import whoscored_columns as wc
import abc
from functools import lru_cache
from footmav.utils.mplsoccer.standardizer import Standardizer


@lru_cache(10)
def get_xthreat_grid() -> List[List[float]]:
    """
    Retrieve the xthread grid from the web
    """

    r = requests.get("https://karun.in/blog/data/open_xt_12x8_v1.json")
    if r.status_code != 200:
        raise Exception("Could not retrieve xthreat grid")
    return r.json()


def has_qualifier(
    qs: Dict[str, Any], display_name: str = "", qualifier_code: int = -1
) -> bool:
    """
    Checks if a given qualifier is present in a dict of qualifiers

    Args:
        qs (Dict[str, Any]): The qualifiers dict
        display_name (str): The display name of the qualifier
        qualifier_code (int): The code of the qualifier

    Returns:
        bool: True if the qualifier is present, False otherwise

    """

    if display_name:
        try:
            next(d for d in qs if d["type"]["displayName"] == display_name)  # type: ignore
            return True
        except StopIteration:
            return False

    else:
        try:
            next(d for d in qs if d["type"]["value"] == qualifier_code)  # type: ignore

            return True
        except StopIteration:
            return False


def col_has_qualifier(
    df: pd.DataFrame, display_name: str = "", qualifier_code: int = -1
) -> pd.Series:
    """
    Checks if a given qualifier is present in each element of a  column of a dataframe

    Args:
        df (pd.DataFrame): The dataframe
        display_name (str): The display name of the qualifier
        qualifier_code (int): The code of the qualifier

    Returns:
        pd.Series: True if the qualifier is present, False otherwise

    """
    return df["qualifiers"].apply(
        lambda x: has_qualifier(x, display_name, qualifier_code)
    )


TOUCH_IDS = [
    EventType(id)
    for id in [1, 2, 3, 7, 8, 9, 10, 11, 2, 13, 14, 15, 16, 41, 42, 50, 54, 61, 73, 74]
]


def is_touch(df):
    return (df["event_type"].isin(TOUCH_IDS)) | (
        (df["event_type"] == EventType.Foul) & (df["outcomeType"] == 1)
    )


def in_rectangle(
    x: float, y: float, x1: float, y1: float, x2: float, y2: float
) -> bool:
    """
    Returns True if the point (x,y) is in the rectangle defined by (x1,y1) and (x2,y2).
    """

    return min(x1, x2) <= x <= max(x1, x2) and min(y1, y2) <= y <= max(y1, y2)


def is_goal(dataframe):
    return (dataframe["event_type"] == EventType.Goal) & (
        ~col_has_qualifier(dataframe, qualifier_code=28)
    )


def is_shot_on_target(dataframe):
    return ((is_goal(dataframe)) | (dataframe["event_type"] == EventType.SavedShot)) & (
        ~col_has_qualifier(dataframe, qualifier_code=82)
    )


def is_shot(dataframe):
    """
    Checks if each event in a dataframe is a shot, regardless of the outcome (and excluding own goals)

    Args:
        whoscored_df (pd.DataFrame): The dataframe

    Returns:
        pd.Series: True if the event is a shot, False otherwise
    """
    return (
        is_goal(dataframe)
        | (dataframe["event_type"] == EventType.SavedShot)
        | (dataframe["event_type"] == EventType.MissedShots)
        | (dataframe["event_type"] == EventType.ShotOnPost)
    )


def is_aerial_duel(dataframe):
    return (dataframe["event_type"] == EventType.Aerial) | (
        (dataframe["event_type"] == EventType.Foul)
        & (col_has_qualifier(dataframe, qualifier_code=264))
    )


def is_tackle_attempted(dataframe):
    return (dataframe["event_type"] == EventType.Tackle) | (
        dataframe["event_type"] == EventType.Challenge
    )


def col_in_rect(
    whoscored_df: pd.DataFrame,
    verticle1: Tuple[float, float],
    verticle2: Tuple[float, float],
    end_coord: bool = False,
) -> pd.Series:
    """
    Returns a boolean series indicating whether each event in a dataframe is located in the rectangle defined by verticle1 and verticle2.

    Args:
        whoscored_df (pd.DataFrame): The dataframe
        verticle1 (Tuple[float, float]): The first verticle of the rectangle
        verticle2 (Tuple[float, float]): The second verticle of the rectangle
        end_coord (bool): If True, the verticles are considered as the end coordinates of the rectangle, otherwise they are considered as the start coordinates

    Returns:
        pd.Series: True if the event is in the rectangle, False otherwise
    """
    if end_coord:
        coords = zip(whoscored_df[wc.END_X.N], whoscored_df[wc.END_Y.N])
    else:
        coords = zip(whoscored_df[wc.X.N], whoscored_df[wc.Y.N])
    x1, y1 = verticle1
    x2, y2 = verticle2
    return np.array([in_rectangle(x, y, x1, y1, x2, y2) for x, y in coords])


def is_cutback(whoscored_df: pd.DataFrame) -> pd.Series:
    """
    Returns a boolean series indicating whether each event in a dataframe is a cutback pass

    Args:
        whoscored_df (pd.DataFrame): The dataframe

    Returns:
        pd.Series: True if the event is a cutback pass, False otherwise

    """
    left_area = [(100, 100), (94, 64)]
    right_area = [(100, 0), (94, 36)]
    target_area = [(94, 64), (83, 36)]
    return (
        (whoscored_df[wc.EVENT_TYPE.N] == EventType.Pass)
        & (
            (col_in_rect(whoscored_df, left_area[0], left_area[1]))
            | (col_in_rect(whoscored_df, right_area[0], right_area[1]))
        )
        & (col_in_rect(whoscored_df, target_area[0], target_area[1], True))
        & (~col_has_qualifier(whoscored_df, display_name="CornerTaken"))
        & (~col_has_qualifier(whoscored_df, display_name="Chipped"))
    )


class Distance:
    def __init__(self):
        self._standardizer = Standardizer(pitch_from="opta", pitch_to="uefa")

    def __call__(
        self, x0: np.ndarray, y0: np.ndarray, x1: np.ndarray, y1: np.ndarray
    ) -> np.ndarray:
        if not isinstance(x0, np.ndarray):
            x0 = np.array([x0])
            y0 = np.array([y0])
            x1 = np.array([x1])
            y1 = np.array([y1])

        # xs, ys=self._standardizer.transform([x0, x1], [y0, y1])
        x0, y0 = self._standardizer.transform(x0, y0)
        x1, y1 = self._standardizer.transform(x1, y1)
        return np.sqrt(np.power(x1 - x0, 2) + np.power(y1 - y0, 2))


distance = Distance()


def distance_old(
    x: np.array, y: np.array, end_x: np.array, end_y: np.array
) -> np.array:
    """
    Returns the distance between two points, scaled by whoscored pitch coordinates

    Args:

        x (np.array): The x coordinates of the first point
        y (np.array): The y coordinates of the first point
        end_x (np.array): The x coordinates of the second point
        end_y (np.array): The y coordinates of the second point

    Returns:
        np.array: The distances between the two points
    """
    return np.sqrt(((end_x - x) * 1.2) ** 2 + ((end_y - y) * 0.8) ** 2)


def in_attacking_box(df, start=True):
    x = "x" if start else "endX"
    y = "y" if start else "endY"
    return (df[x] > 83) & (df[x] <= 100) & (df[y] > 21) & (df[y] < 78.9)


def in_defensive_box(df, start=True):
    x = "x" if start else "endX"
    y = "y" if start else "endY"
    return (df[x] < 17) & (df[x] >= 0) & (df[y] > 21) & (df[y] < 78.9)


def is_keypass(df):
    assisted_shots = df[df["shots"] & col_has_qualifier(df, qualifier_code=55)]
    assist_ids = np.array(
        [
            str(a) + str(b)
            for a, b in zip(
                assisted_shots["matchId"],
                [
                    next(
                        e["value"]
                        for e in shot["qualifiers"]
                        if e["type"]["value"] == 55
                    )
                    for _, shot in assisted_shots.iterrows()
                ],
            )
        ]
    )
    return np.in1d(
        np.array([str(a) + str(b) for a, b in (zip(df["matchId"], df["eventId"]))]),
        assist_ids,
    )


def is_progressive(whoscored_df: pd.DataFrame) -> pd.Series:
    """
    Returns a boolean series indicating whether each event in a dataframe is a progressive pass

    Args:
        whoscored_df (pd.DataFrame): The dataframe

    Returns:
        pd.Series: True if the event is a progressive pass, False otherwise

    """

    start_distance_to_goal_middle = distance(
        whoscored_df[wc.X.N],
        whoscored_df[wc.Y.N],
        MIDDLE_GOAL_COORDS[0],
        MIDDLE_GOAL_COORDS[1],
    )
    start_distance_to_goal_top = distance(
        whoscored_df[wc.X.N],
        whoscored_df[wc.Y.N],
        TOP_GOAL_COORDS[0],
        TOP_GOAL_COORDS[1],
    )
    start_distance_to_goal_bottom = distance(
        whoscored_df[wc.X.N],
        whoscored_df[wc.Y.N],
        BOTTOM_GOAL_COORDS[0],
        BOTTOM_GOAL_COORDS[1],
    )
    start_distance = np.minimum(
        start_distance_to_goal_middle,
        np.minimum(start_distance_to_goal_top, start_distance_to_goal_bottom),
    )

    end_distance_to_goal_middle = distance(
        whoscored_df[wc.END_X.N],
        whoscored_df[wc.END_Y.N],
        MIDDLE_GOAL_COORDS[0],
        MIDDLE_GOAL_COORDS[1],
    )
    end_distance_to_goal_top = distance(
        whoscored_df[wc.END_X.N],
        whoscored_df[wc.END_Y.N],
        TOP_GOAL_COORDS[0],
        TOP_GOAL_COORDS[1],
    )
    end_distance_to_goal_bottom = distance(
        whoscored_df[wc.END_X.N],
        whoscored_df[wc.END_Y.N],
        BOTTOM_GOAL_COORDS[0],
        BOTTOM_GOAL_COORDS[1],
    )
    end_distance = np.minimum(
        end_distance_to_goal_middle,
        np.minimum(end_distance_to_goal_top, end_distance_to_goal_bottom),
    )

    is_progressive = (
        (end_distance < start_distance * 0.75)
        & (whoscored_df[wc.EVENT_TYPE.N] == EventType.Pass)
        & (~col_has_qualifier(whoscored_df, display_name="CornerTaken"))
    )
    return is_progressive


def is_assist(df):
    assisted_shots = df[df["goals"] & col_has_qualifier(df, qualifier_code=55)]
    assist_ids = np.array(
        [
            str(a) + str(b)
            for a, b in zip(
                assisted_shots["matchId"],
                [
                    next(
                        e["value"]
                        for e in shot["qualifiers"]
                        if e["type"]["value"] == 55
                    )
                    for _, shot in assisted_shots.iterrows()
                ],
            )
        ]
    )
    return np.in1d(
        np.array([str(a) + str(b) for a, b in (zip(df["matchId"], df["eventId"]))]),
        assist_ids,
    )


def into_attacking_box(dataframe):
    return (in_attacking_box(dataframe, start=False)) & (
        ~in_attacking_box(dataframe, start=True)
    )


def success(dataframe):
    return dataframe["outcomeType"] == 1


def open_play_pass_attempt(dataframe):
    return (
        (dataframe["event_type"] == EventType.Pass)
        & (~col_has_qualifier(dataframe, qualifier_code=2))
        & (~col_has_qualifier(dataframe, qualifier_code=107))
        & (~col_has_qualifier(dataframe, qualifier_code=123))
    )


def cross_attempt(dataframe):
    return (
        (dataframe["event_type"] == EventType.Pass)
        & (col_has_qualifier(dataframe, qualifier_code=2))
        & (~col_has_qualifier(dataframe, qualifier_code=5))
        & (~col_has_qualifier(dataframe, qualifier_code=6))
    )


def minutes(df):
    sub_ons = df.loc[df["event_type"] == EventType.SubstitutionOn].rename(
        columns={"minute": "sub_on_minute"}
    )
    sub_offs = df.loc[df["event_type"] == EventType.SubstitutionOff].rename(
        columns={"minute": "sub_off_minute"}
    )
    last_min = df.groupby(["matchId", "period"]).agg({"minute": "max"})
    player_df = df.loc[~df["player_name"].isna()][
        ["matchId", "player_name", "period"]
    ].drop_duplicates()
    player_df = pd.merge(player_df, last_min, on=["matchId", "period"], how="left")
    player_df = pd.merge(
        player_df,
        sub_ons[["player_name", "matchId", "period", "sub_on_minute"]],
        on=["player_name", "matchId", "period"],
        how="left",
    ).fillna(0)
    player_df = pd.merge(
        player_df,
        sub_offs[["player_name", "matchId", "period", "sub_off_minute"]],
        on=["player_name", "matchId", "period"],
        how="left",
    ).fillna(10000)
    player_df["minute"] = player_df["minute"] - 45 * (player_df["period"] - 1)
    player_df["start"] = 0
    player_df["sub_on_minute"] = player_df["sub_on_minute"] - 45 * (
        player_df["period"] - 1
    )
    player_df["sub_off_minute"] = player_df["sub_off_minute"] - 45 * (
        player_df["period"] - 1
    )
    player_df["sub_on_minute"] = player_df[["sub_on_minute", "start"]].max(axis=1)
    player_df["sub_off_minute"] = player_df[["sub_off_minute", "minute"]].min(axis=1)
    player_df["minutes"] = player_df["sub_off_minute"] - player_df["sub_on_minute"]
    return player_df.groupby(["matchId", "player_name"]).agg({"minutes": "sum"})


class PassClassifier(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def get_qualifier_function(cls) -> Callable[[pd.DataFrame], pd.Series]:
        """
        Returns a function that returns a boolean series indicating whether each event in a dataframe is a pass to be classified by this particular classifier

        Returns:
            Callable[[pd.DataFrame], pd.Series]: A function that returns a boolean series indicating whether each event in a dataframe is a pass of a particular type
        """

    @classmethod
    @abc.abstractmethod
    def get_pass_classification(cls) -> PassType:
        """
        Returns the pass type that this classifier classifies

        Returns:
            PassType: The pass type that this classifier classifies
        """

    def classify(self, whoscored_df: pd.DataFrame) -> pd.Series:
        """
        Returns a boolean series indicating whether each event in a dataframe is a pass of a particular type

        Args:
            whoscored_df (pd.DataFrame): The dataframe

        Returns:
            pd.Series: True if the event is a pass of a particular type, False otherwise
        """
        return pd.Series(
            self.get_qualifier_function()(whoscored_df).values.astype(int)
            * self.get_pass_classification().value,
            index=whoscored_df.index,
        )


class ProgressivePassClassifier(PassClassifier):
    @classmethod
    def get_qualifier_function(cls) -> Callable[[pd.DataFrame], pd.Series]:
        """
        Returns a function that returns a boolean series indicating whether each event in a dataframe is a progressive pass

        Returns:
            Callable[[pd.DataFrame], pd.Series]: A function that returns a boolean series indicating whether each event in a dataframe is a progressive pass
        """
        return is_progressive

    @classmethod
    def get_pass_classification(cls) -> PassType:
        """
        Returns the pass type that this classifier classifies

        Returns:
            PassType: The pass type that this classifier classifies
        """
        return PassType.PROGRESSIVE


class CutbackPassClassifier(PassClassifier):
    @classmethod
    def get_qualifier_function(cls) -> Callable[[pd.DataFrame], pd.Series]:
        """
        Returns a function that returns a boolean series indicating whether each event in a dataframe is a cutback pass

        Returns:
            Callable[[pd.DataFrame], pd.Series]: A function that returns a boolean series indicating whether each event in a dataframe is a cutback pass
        """
        return is_cutback

    @classmethod
    def get_pass_classification(cls) -> PassType:
        """
        Returns the pass type that this classifier classifies

        Returns:
            PassType: The pass type that this classifier classifies
        """
        return PassType.CUTBACK


def classify_passes(whoscored_df: pd.DataFrame) -> pd.Series:
    """
    Returns a dataframe with a column for each pass type indicating whether each event in a dataframe is a pass of a particular type

    Args:
        whoscored_df (pd.DataFrame): The dataframe

    Returns:
        pd.Series: A series that contains the sum of the values of all types of pass that this pass is a type of
    """
    series = pd.Series(0, index=whoscored_df.index)
    for subtype in PassClassifier.__subclasses__():
        series += subtype().classify(whoscored_df)  # type: ignore
    return series
