import pandas as pd
from footmav.data_definitions.fbref import fbref_columns as fc

NON_TOP_5_TEAMS = [
    "CSKA Moscow",
    "Spartak Moscow",
    "Shakhtar",
    "Loko Moscow",
    "Krasnodar",
    "Dynamo Mosc",
    "Zenit",
    "Rostov",
    "Dynamo Kyiv",
    "RB Salzburg",
    "LASK",
    "Rubin Kazan",
    "Arsenal Tula",
    "Samara",
    "Sochi",
    "Austria Wien",
    "Sturm Graz",
    "Rapid Wien",
    "CS Emelec",
    "Independiente",
    "SK Dnipro-1",
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
