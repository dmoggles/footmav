import pandas as pd


def test_remove_non_top_5_teams():
    from footmav.utils.cleanup import remove_non_top_5_teams

    bad_teams = [
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
    other_teams = ["chelsea", "arsenal", "liverpool"]
    data = pd.DataFrame({"squad": bad_teams + other_teams})
    result = remove_non_top_5_teams(data)
    pd.testing.assert_frame_equal(result, data[data["squad"].isin(other_teams)])
