from footmav.data_definitions import base
from footmav.data_definitions.data_sources import DataSource
from footmav.data_definitions.derived import FunctionDerivedDataAttribute
from footmav.data_definitions import attribute_functions as F

import pandas as pd

ID = base.IntDataAttribute(
    "id", normalizable=False, source=DataSource.UNDERSTAT, rename_to="us_id"
)
MINUTE = base.IntDataAttribute(
    "minute", normalizable=False, source=DataSource.UNDERSTAT, rename_to="us_minute"
)
RESULT = base.StrDataAttribute(
    "result", source=DataSource.UNDERSTAT, rename_to="us_result"
)
X = base.FloatDataAttribute(
    "X",
    agg_function="mean",
    normalizable=False,
    source=DataSource.UNDERSTAT,
    rename_to="us_X",
)
Y = base.FloatDataAttribute(
    "Y",
    agg_function="mean",
    normalizable=False,
    source=DataSource.UNDERSTAT,
    rename_to="us_Y",
)
XG = base.FloatDataAttribute(
    "xG",
    agg_function="sum",
    normalizable=False,
    source=DataSource.UNDERSTAT,
    rename_to="us_xG",
)
PLAYER = base.StrDataAttribute(
    "player", source=DataSource.UNDERSTAT, rename_to="us_player"
)
HOME_OR_AWAY = base.StrDataAttribute(
    "h_a", source=DataSource.UNDERSTAT, rename_to="us_home_or_away"
)
PLAYER_ID = base.IntDataAttribute(
    "player_id",
    normalizable=False,
    source=DataSource.UNDERSTAT,
    rename_to="us_player_id",
)
SITUATION = base.StrDataAttribute(
    "situation", source=DataSource.UNDERSTAT, rename_to="us_situation"
)
YEAR = base.IntDataAttribute(
    "season",
    normalizable=False,
    source=DataSource.UNDERSTAT,
    agg_function="first",
    rename_to="us_season",
)
SHOT_TYPE = base.StrDataAttribute(
    "shotType", source=DataSource.UNDERSTAT, rename_to="us_shot_type"
)
MATCH_ID = base.IntDataAttribute(
    "match_id", normalizable=False, source=DataSource.UNDERSTAT, rename_to="us_match_id"
)
HOME_TEAM = base.StrDataAttribute(
    "h_team", source=DataSource.UNDERSTAT, rename_to="us_home_team"
)
AWAY_TEAM = base.StrDataAttribute(
    "a_team", source=DataSource.UNDERSTAT, rename_to="us_away_team"
)
HOME_GOALS = base.IntDataAttribute(
    "h_goals",
    normalizable=False,
    source=DataSource.UNDERSTAT,
    rename_to="us_home_goals",
)
AWAY_GOALS = base.IntDataAttribute(
    "a_goals",
    normalizable=False,
    source=DataSource.UNDERSTAT,
    rename_to="us_away_goals",
)
DATE = base.DateDataAttribute(
    "date",
    transform_function=lambda x: pd.Timestamp(x).date(),
    source=DataSource.UNDERSTAT,
    rename_to="us_date",
)
PLAYER_ASSISTED = base.StrDataAttribute(
    "player_assisted", source=DataSource.UNDERSTAT, rename_to="us_player_assisted"
)
LAST_ACTION = base.StrDataAttribute(
    "lastAction", source=DataSource.UNDERSTAT, rename_to="us_last_action"
)


PLAYER_TEAM = FunctionDerivedDataAttribute(
    "us_player_team",
    F.If(F.Col(HOME_OR_AWAY) == F.Lit("h"), F.Col(HOME_TEAM), F.Col(AWAY_TEAM)),
    source=DataSource.UNDERSTAT,
    agg_function="first",
    data_type=str,
    recalculate_on_aggregation=False,
)
