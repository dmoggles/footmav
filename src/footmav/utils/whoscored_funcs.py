from typing import Callable, List, Tuple
import requests  # type: ignore
import pandas as pd
import numpy as np
from footmav.data_definitions.whoscored.constants import (
    BOTTOM_GOAL_COORDS,
    MIDDLE_GOAL_COORDS,
    SHOT_EVENTS,
    TOP_GOAL_COORDS,
    EventType,
    PassType,
)
from typing import Dict, Any
from footmav.data_definitions.whoscored import whoscored_columns as wc
import abc
from functools import lru_cache


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


def is_shot(whoscored_df: pd.DataFrame) -> pd.Series:
    """
    Checks if each event in a dataframe is a shot, regardless of the outcome (and excluding own goals)

    Args:
        whoscored_df (pd.DataFrame): The dataframe

    Returns:
        pd.Series: True if the event is a shot, False otherwise
    """
    return (whoscored_df[wc.EVENT_TYPE.N].isin(SHOT_EVENTS)) & ~col_has_qualifier(
        whoscored_df, display_name="OwnGoal"
    )


def in_rectangle(
    x: float, y: float, x1: float, y1: float, x2: float, y2: float
) -> bool:
    """
    Returns True if the point (x,y) is in the rectangle defined by (x1,y1) and (x2,y2).
    """

    return min(x1, x2) <= x <= max(x1, x2) and min(y1, y2) <= y <= max(y1, y2)


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


def distance(x: np.array, y: np.array, end_x: np.array, end_y: np.array) -> np.array:
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
