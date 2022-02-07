import pandas as pd


def test_remove_non_top_5_teams():
    from footmav.utils.cleanup import remove_non_top_5_teams

    bad_teams = [
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
    ]
    other_teams = ["Chelsea", "Arsenal", "Liverpool"]
    data = pd.DataFrame({"squad": bad_teams + other_teams})
    result = remove_non_top_5_teams(data)
    pd.testing.assert_frame_equal(result, data[data["squad"].isin(other_teams)])
