from footmav.data_definitions.base import (
    DateDataAttribute,
    FloatDataAttribute,
    IntDataAttribute,
    StrDataAttribute,
    list_all_values,
)

from footmav.data_definitions.data_sources import DataSource
from footmav.data_definitions.derived import FunctionDerivedDataAttribute
from footmav.data_definitions import attribute_functions as F

YEAR = IntDataAttribute("season", agg_function="first", source=DataSource.FBREF)
PLAYER_ID = StrDataAttribute("player_id", source=DataSource.FBREF)
PLAYER = StrDataAttribute("player", source=DataSource.FBREF)
DATE = DateDataAttribute("date", agg_function=None, source=DataSource.FBREF)
DAY_OF_WEEK = StrDataAttribute("dayofweek", agg_function=None, source=DataSource.FBREF)
COMPETITION = StrDataAttribute(
    "comp", agg_function=list_all_values, source=DataSource.FBREF
)
ROUND = StrDataAttribute("round", agg_function=None, source=DataSource.FBREF)
VENUE = StrDataAttribute("venue", agg_function=None, source=DataSource.FBREF)
RESULT = StrDataAttribute("result", agg_function=None, source=DataSource.FBREF)
TEAM = StrDataAttribute("squad", agg_function=list_all_values, source=DataSource.FBREF)
OPPONENT = StrDataAttribute(
    "opponent", agg_function=list_all_values, source=DataSource.FBREF
)
STARTED = StrDataAttribute("game_started", agg_function=None, source=DataSource.FBREF)
POSITION = StrDataAttribute(
    "position", agg_function=list_all_values, source=DataSource.FBREF
)
MINUTES = FloatDataAttribute("minutes", source=DataSource.FBREF)
TACKLES = FloatDataAttribute("tackles", source=DataSource.FBREF)
TACKLES_WON = FloatDataAttribute("tackles_won", source=DataSource.FBREF)
TACKLES_DEF_3RD = FloatDataAttribute("tackles_def_3rd", source=DataSource.FBREF)
TACKLES_MID_3RD = FloatDataAttribute("tackles_mid_3rd", source=DataSource.FBREF)
TACKLES_ATT_3RD = FloatDataAttribute("tackles_att_3rd", source=DataSource.FBREF)
DRIBBLE_TACKLES = FloatDataAttribute("dribble_tackles", source=DataSource.FBREF)
DRIBBLES_VS = FloatDataAttribute("dribbles_vs", source=DataSource.FBREF)
DRIBBLED_PAST = FloatDataAttribute("dribbled_past", source=DataSource.FBREF)
PRESSURES = FloatDataAttribute("pressures", source=DataSource.FBREF)
PRESSURE_REGAINS = FloatDataAttribute("pressure_regains", source=DataSource.FBREF)
PRESSURES_DEF_3RD = FloatDataAttribute("pressures_def_3rd", source=DataSource.FBREF)
PRESSURES_MID_3RD = FloatDataAttribute("pressures_mid_3rd", source=DataSource.FBREF)
PRESSURES_ATT_3RD = FloatDataAttribute("pressures_att_3rd", source=DataSource.FBREF)
BLOCKS = FloatDataAttribute("blocks", source=DataSource.FBREF)
BLOCKED_SHOTS = FloatDataAttribute("blocked_shots", source=DataSource.FBREF)
BLOCKED_SHOTS_SAVES = FloatDataAttribute("blocked_shots_saves", source=DataSource.FBREF)
BLOCKED_PASSES = FloatDataAttribute("blocked_passes", source=DataSource.FBREF)
INTERCEPTIONS = FloatDataAttribute("interceptions", source=DataSource.FBREF)
TACKLES_INTERCEPTIONS = FloatDataAttribute(
    "tackles_interceptions", source=DataSource.FBREF
)
CLEARANCES = FloatDataAttribute("clearances", source=DataSource.FBREF)
ERRORS = FloatDataAttribute("errors", source=DataSource.FBREF)
BENCH_EXPLAIN = StrDataAttribute(
    "bench_explain", agg_function=None, source=DataSource.FBREF
)

SCA = FloatDataAttribute("sca", source=DataSource.FBREF)
SCA_PASSES_LIVE = FloatDataAttribute("sca_passes_live", source=DataSource.FBREF)
SCA_PASSES_DEAD = FloatDataAttribute("sca_passes_dead", source=DataSource.FBREF)
SCA_DRIBBLES = FloatDataAttribute("sca_dribbles", source=DataSource.FBREF)
SCA_SHOTS = FloatDataAttribute("sca_shots", source=DataSource.FBREF)
SCA_FOULED = FloatDataAttribute("sca_fouled", source=DataSource.FBREF)
SCA_DEFENSE = FloatDataAttribute("sca_defense", source=DataSource.FBREF)
GCA = FloatDataAttribute("gca", source=DataSource.FBREF)
GCA_PASSES_LIVE = FloatDataAttribute("gca_passes_live", source=DataSource.FBREF)
GCA_PASSES_DEAD = FloatDataAttribute("gca_passes_dead", source=DataSource.FBREF)
GCA_DRIBBLES = FloatDataAttribute("gca_dribbles", source=DataSource.FBREF)
GCA_SHOTS = FloatDataAttribute("gca_shots", source=DataSource.FBREF)
GCA_FOULED = FloatDataAttribute("gca_fouled", source=DataSource.FBREF)
GCA_DEFENSE = FloatDataAttribute("gca_defense", source=DataSource.FBREF)


PASSES_COMPLETED = FloatDataAttribute("passes_completed", source=DataSource.FBREF)
PASSES = FloatDataAttribute("passes", source=DataSource.FBREF)

PASSES_TOTAL_DISTANCE = FloatDataAttribute(
    "passes_total_distance", source=DataSource.FBREF
)
PASSES_PROGRESSIVE_DISTANCE = FloatDataAttribute(
    "passes_progressive_distance", source=DataSource.FBREF
)
PASSES_COMPLETED_SHORT = FloatDataAttribute(
    "passes_completed_short", source=DataSource.FBREF
)
PASSES_SHORT = FloatDataAttribute("passes_short", source=DataSource.FBREF)

PASSES_COMPLETED_MEDIUM = FloatDataAttribute(
    "passes_completed_medium", source=DataSource.FBREF
)
PASSES_MEDIUM = FloatDataAttribute("passes_medium", source=DataSource.FBREF)

PASSES_COMPLETED_LONG = FloatDataAttribute(
    "passes_completed_long", source=DataSource.FBREF
)
PASSES_LONG = FloatDataAttribute("passes_long", source=DataSource.FBREF)

ASSISTS = FloatDataAttribute("assists", source=DataSource.FBREF)
XA = FloatDataAttribute("xa", source=DataSource.FBREF)
ASSISTED_SHOTS = FloatDataAttribute("assisted_shots", source=DataSource.FBREF)
PASSES_INTO_FINAL_THIRD = FloatDataAttribute(
    "passes_into_final_third", source=DataSource.FBREF
)
PASSES_INTO_PENALTY_AREA = FloatDataAttribute(
    "passes_into_penalty_area", source=DataSource.FBREF
)
CROSSES_INTO_PENALTY_AREA = FloatDataAttribute(
    "crosses_into_penalty_area", source=DataSource.FBREF
)
PROGRESSIVE_PASSES = FloatDataAttribute("progressive_passes", source=DataSource.FBREF)

PASSES_LIVE = FloatDataAttribute("passes_live", source=DataSource.FBREF)
PASSES_DEAD = FloatDataAttribute("passes_dead", source=DataSource.FBREF)
PASSES_FREE_KICKS = FloatDataAttribute("passes_free_kicks", source=DataSource.FBREF)
THROUGH_BALLS = FloatDataAttribute("through_balls", source=DataSource.FBREF)
PASSES_PRESSURE = FloatDataAttribute("passes_pressure", source=DataSource.FBREF)
PASSES_SWITCHES = FloatDataAttribute("passes_switches", source=DataSource.FBREF)
CROSSES = FloatDataAttribute("crosses", source=DataSource.FBREF)
CORNER_KICKS = FloatDataAttribute("corner_kicks", source=DataSource.FBREF)
CORNER_KICKS_IN = FloatDataAttribute("corner_kicks_in", source=DataSource.FBREF)
CORNER_KICKS_OUT = FloatDataAttribute("corner_kicks_out", source=DataSource.FBREF)
CORNER_KICKS_STRAIGHT = FloatDataAttribute(
    "corner_kicks_straight", source=DataSource.FBREF
)
PASSES_GROUND = FloatDataAttribute("passes_ground", source=DataSource.FBREF)
PASSES_LOW = FloatDataAttribute("passes_low", source=DataSource.FBREF)
PASSES_HIGH = FloatDataAttribute("passes_high", source=DataSource.FBREF)
PASSES_LEFT_FOOT = FloatDataAttribute("passes_left_foot", source=DataSource.FBREF)
PASSES_RIGHT_FOOT = FloatDataAttribute("passes_right_foot", source=DataSource.FBREF)
PASSES_HEAD = FloatDataAttribute("passes_head", source=DataSource.FBREF)
THROW_INS = FloatDataAttribute("throw_ins", source=DataSource.FBREF)
PASSES_OTHER_BODY = FloatDataAttribute("passes_other_body", source=DataSource.FBREF)
PASSES_COMPLETED = FloatDataAttribute("passes_completed", source=DataSource.FBREF)
PASSES_OFFSIDES = FloatDataAttribute("passes_offsides", source=DataSource.FBREF)
PASSES_OOB = FloatDataAttribute("passes_oob", source=DataSource.FBREF)
PASSES_INTERCEPTED = FloatDataAttribute("passes_intercepted", source=DataSource.FBREF)
PASSES_BLOCKED = FloatDataAttribute("passes_blocked", source=DataSource.FBREF)

CARDS_YELLOW = FloatDataAttribute("cards_yellow", source=DataSource.FBREF)
CARDS_RED = FloatDataAttribute("cards_red", source=DataSource.FBREF)
CARDS_YELLOW_RED = FloatDataAttribute("cards_yellow_red", source=DataSource.FBREF)
FOULS = FloatDataAttribute("fouls", source=DataSource.FBREF)
FOULED = FloatDataAttribute("fouled", source=DataSource.FBREF)
OFFSIDES = FloatDataAttribute("offsides", source=DataSource.FBREF)
CROSSES = FloatDataAttribute("crosses", source=DataSource.FBREF)
INTERCEPTIONS = FloatDataAttribute("interceptions", source=DataSource.FBREF)
TACKLES_WON = FloatDataAttribute("tackles_won", source=DataSource.FBREF)
PENS_WON = FloatDataAttribute("pens_won", source=DataSource.FBREF)
PENS_CONCEDED = FloatDataAttribute("pens_conceded", source=DataSource.FBREF)
OWN_GOALS = FloatDataAttribute("own_goals", source=DataSource.FBREF)
BALL_RECOVERIES = FloatDataAttribute("ball_recoveries", source=DataSource.FBREF)
AERIALS_WON = FloatDataAttribute("aerials_won", source=DataSource.FBREF)
AERIALS_LOST = FloatDataAttribute("aerials_lost", source=DataSource.FBREF)


TOUCHES = FloatDataAttribute("touches", source=DataSource.FBREF)
TOUCHES_DEF_PEN_AREA = FloatDataAttribute(
    "touches_def_pen_area", source=DataSource.FBREF
)
TOUCHES_DEF_3RD = FloatDataAttribute("touches_def_3rd", source=DataSource.FBREF)
TOUCHES_MID_3RD = FloatDataAttribute("touches_mid_3rd", source=DataSource.FBREF)
TOUCHES_ATT_3RD = FloatDataAttribute("touches_att_3rd", source=DataSource.FBREF)
TOUCHES_ATT_PEN_AREA = FloatDataAttribute(
    "touches_att_pen_area", source=DataSource.FBREF
)
TOUCHES_LIVE_BALL = FloatDataAttribute("touches_live_ball", source=DataSource.FBREF)
DRIBBLES_COMPLETED = FloatDataAttribute("dribbles_completed", source=DataSource.FBREF)
DRIBBLES = FloatDataAttribute("dribbles", source=DataSource.FBREF)

PLAYERS_DRIBBLED_PAST = FloatDataAttribute(
    "players_dribbled_past", source=DataSource.FBREF
)
NUTMEGS = FloatDataAttribute("nutmegs", source=DataSource.FBREF)
CARRIES = FloatDataAttribute("carries", source=DataSource.FBREF)
CARRY_DISTANCE = FloatDataAttribute("carry_distance", source=DataSource.FBREF)
CARRY_PROGRESSIVE_DISTANCE = FloatDataAttribute(
    "carry_progressive_distance", source=DataSource.FBREF
)
PROGRESSIVE_CARRIES = FloatDataAttribute("progressive_carries", source=DataSource.FBREF)
CARRIES_INTO_FINAL_THIRD = FloatDataAttribute(
    "carries_into_final_third", source=DataSource.FBREF
)
CARRIES_INTO_PENALTY_AREA = FloatDataAttribute(
    "carries_into_penalty_area", source=DataSource.FBREF
)
MISCONTROLS = FloatDataAttribute("miscontrols", source=DataSource.FBREF)
DISPOSSESSED = FloatDataAttribute("dispossessed", source=DataSource.FBREF)
PASS_TARGETS = FloatDataAttribute("pass_targets", source=DataSource.FBREF)
PASSES_RECEIVED = FloatDataAttribute("passes_received", source=DataSource.FBREF)

PROGRESSIVE_PASSES_RECEIVED = FloatDataAttribute(
    "progressive_passes_received", source=DataSource.FBREF
)

GOALS = FloatDataAttribute("goals", source=DataSource.FBREF)
ASSISTS = FloatDataAttribute("assists", source=DataSource.FBREF)
PENS_MADE = FloatDataAttribute("pens_made", source=DataSource.FBREF)
PENS_ATT = FloatDataAttribute("pens_att", source=DataSource.FBREF)
SHOTS_TOTAL = FloatDataAttribute("shots_total", source=DataSource.FBREF)
SHOTS_ON_TARGET = FloatDataAttribute("shots_on_target", source=DataSource.FBREF)
CARDS_YELLOW = FloatDataAttribute("cards_yellow", source=DataSource.FBREF)
CARDS_RED = FloatDataAttribute("cards_red", source=DataSource.FBREF)
TOUCHES = FloatDataAttribute("touches", source=DataSource.FBREF)
PRESSURES = FloatDataAttribute("pressures", source=DataSource.FBREF)
TACKLES = FloatDataAttribute("tackles", source=DataSource.FBREF)
INTERCEPTIONS = FloatDataAttribute("interceptions", source=DataSource.FBREF)
BLOCKS = FloatDataAttribute("blocks", source=DataSource.FBREF)
XG = FloatDataAttribute("xg", source=DataSource.FBREF)
NPXG = FloatDataAttribute("npxg", source=DataSource.FBREF)
XA = FloatDataAttribute("xa", source=DataSource.FBREF)
SCA = FloatDataAttribute("sca", source=DataSource.FBREF)
GCA = FloatDataAttribute("gca", source=DataSource.FBREF)
PASSES_COMPLETED = FloatDataAttribute("passes_completed", source=DataSource.FBREF)
PASSES = FloatDataAttribute("passes", source=DataSource.FBREF)

PROGRESSIVE_PASSES = FloatDataAttribute("progressive_passes", source=DataSource.FBREF)
CARRIES = FloatDataAttribute("carries", source=DataSource.FBREF)
PROGRESSIVE_CARRIES = FloatDataAttribute("progressive_carries", source=DataSource.FBREF)
DRIBBLES_COMPLETED = FloatDataAttribute("dribbles_completed", source=DataSource.FBREF)
DRIBBLES = FloatDataAttribute("dribbles", source=DataSource.FBREF)

YEAR_FB_REF = IntDataAttribute(
    "year",
    rename_to="season",
    agg_function="first",
    source=DataSource.FBREF,
)

SHOTS_ON_TARGET_AGAINST = FloatDataAttribute(
    "shots_on_target_against", source=DataSource.FBREF
)
GOALS_AGAINST_GK = FloatDataAttribute("goals_against_gk", source=DataSource.FBREF)
SAVES = FloatDataAttribute("saves", source=DataSource.FBREF)
SAVE_PCT = FloatDataAttribute("save_pct", source=DataSource.FBREF)
CLEAN_SHEETS = FloatDataAttribute("clean_sheets", source=DataSource.FBREF)
PSXG_GK = FloatDataAttribute("psxg_gk", source=DataSource.FBREF)
PENS_ATT_GK = FloatDataAttribute("pens_att_gk", source=DataSource.FBREF)
PENS_ALLOWED = FloatDataAttribute("pens_allowed", source=DataSource.FBREF)
PENS_SAVED = FloatDataAttribute("pens_saved", source=DataSource.FBREF)
PENS_MISSED_GK = FloatDataAttribute("pens_missed_gk", source=DataSource.FBREF)
PASSES_COMPLETED_LAUNCHED_GK = FloatDataAttribute(
    "passes_completed_launched_gk", source=DataSource.FBREF
)
PASSES_LAUNCHED_GK = FloatDataAttribute("passes_launched_gk", source=DataSource.FBREF)
PASSES_PCT_LAUNCHED_GK = FloatDataAttribute(
    "passes_pct_launched_gk", source=DataSource.FBREF
)
PASSES_GK = FloatDataAttribute("passes_gk", source=DataSource.FBREF)
PASSES_THROWS_GK = FloatDataAttribute("passes_throws_gk", source=DataSource.FBREF)
PCT_PASSES_LAUNCHED_GK = FloatDataAttribute(
    "pct_passes_launched_gk", source=DataSource.FBREF
)
PASSES_LENGTH_AVG_GK = FloatDataAttribute(
    "passes_length_avg_gk", source=DataSource.FBREF
)
GOAL_KICKS = FloatDataAttribute("goal_kicks", source=DataSource.FBREF)
PCT_GOAL_KICKS_LAUNCHED = FloatDataAttribute(
    "pct_goal_kicks_launched", source=DataSource.FBREF
)
GOAL_KICK_LENGTH_AVG = FloatDataAttribute(
    "goal_kick_length_avg", source=DataSource.FBREF
)
CROSSES_GK = FloatDataAttribute("crosses_gk", source=DataSource.FBREF)
CROSSES_STOPPED_GK = FloatDataAttribute("crosses_stopped_gk", source=DataSource.FBREF)
CROSSES_STOPPED_PCT_GK = FloatDataAttribute(
    "crosses_stopped_pct_gk", source=DataSource.FBREF
)
DEF_ACTIONS_OUTSIDE_PEN_AREA_GK = FloatDataAttribute(
    "def_actions_outside_pen_area_gk", source=DataSource.FBREF
)
AVG_DISTANCE_DEF_ACTIONS_GK = FloatDataAttribute(
    "avg_distance_def_actions_gk", source=DataSource.FBREF
)


# SHOT_PCT = PctDerivedAttribute("shot_pct", SHOTS_ON_TARGET, SHOTS_TOTAL, source=DataSource.FBREF)
SHOT_PCT = FunctionDerivedDataAttribute(
    "shot_pct",
    F.Col(SHOTS_ON_TARGET) / F.Col(SHOTS_TOTAL) * F.Lit(100.0),
    data_type=float,
    source=DataSource.FBREF,
)
XG_PER_SHOT = FunctionDerivedDataAttribute(
    "xg_per_shot",
    F.Col(XG) / F.Col(SHOTS_TOTAL),
    data_type=float,
    source=DataSource.FBREF,
)

XG_OUTPERFORM = FunctionDerivedDataAttribute(
    "xg_outperform", F.Col(GOALS) - F.Col(XG), data_type=float, source=DataSource.FBREF
)
NON_PENALTY_GOALS = FunctionDerivedDataAttribute(
    "non_penalty_goals",
    F.Col(GOALS) - F.Col(PENS_MADE),
    data_type=float,
    source=DataSource.FBREF,
)
SCA_LIVE = FunctionDerivedDataAttribute(
    "sca_live",
    F.Col(SCA) - F.Col(SCA_PASSES_DEAD),
    data_type=float,
    source=DataSource.FBREF,
)
NPXG_PER_SHOT = FunctionDerivedDataAttribute(
    "npxg_per_shot",
    F.Col(NPXG) / F.Col(SHOTS_TOTAL),
    data_type=float,
    source=DataSource.FBREF,
)
NPXG_OUTPERFORM = FunctionDerivedDataAttribute(
    "npxg_outperform",
    F.Col(NON_PENALTY_GOALS) - F.Col(NPXG),
    data_type=float,
    source=DataSource.FBREF,
)
NPXG_OUTPERFORM_PER_SHOT = FunctionDerivedDataAttribute(
    "npxg_outperform_per_shot",
    F.Col(NPXG_OUTPERFORM) / F.Col(SHOTS_TOTAL),
    data_type=float,
    source=DataSource.FBREF,
)
NON_PENALTY_GOALS_PER_SHOT = FunctionDerivedDataAttribute(
    "non_penalty_goals_per_shot",
    F.Col(NON_PENALTY_GOALS) / F.Col(SHOTS_TOTAL),
    data_type=float,
    source=DataSource.FBREF,
)

TEST = FunctionDerivedDataAttribute(
    "test_test",
    F.Sum(F.Col(SHOTS_ON_TARGET)) / F.Sum(F.Col(SHOTS_TOTAL)),
    data_type=float,
    source=DataSource.FBREF,
)
