import pandas as pd
from footmav.data_definitions.fbref import fbref_columns as fc

NON_TOP_5_TEAMS = [
    "cska_moscow",
    "spartak_moscow",
    "shakhtar",
    "loko_moscow",
    "krasnodar",
    "dynamo_mosc",
    "zenit",
    "rostov",
    "dynamo_kyiv",
    "rb_salzburg",
    "lask",
    "rubin_kazan",
    "arsenal_tula",
    "samara",
    "sochi",
    "austria_wien",
    "sturm_graz",
    "rapid_wien",
    "cs_emelec",
    "independiente",
    "sk_dnipro_1",
]


def remove_non_top_5_teams(input_data: pd.DataFrame) -> pd.DataFrame:
    """
    Removes all rows with russian and austrian teams.  They are here because Russian premier league and English premier league have the same competition string in fbref,
    as well as German Bundesliga and Austrian Bundesliga.

    Args:
        input_data (pd.DataFrame): Data to be filtered

    Returns:
        pd.DataFrame: Filtered data
    """
    return input_data[~input_data[fc.TEAM.N].isin(NON_TOP_5_TEAMS)]
