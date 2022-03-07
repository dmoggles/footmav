import pandas as pd


class TestFbrefUtils:
    def test_opponent_attribute(self):
        from footmav.data_definitions.fbref import fbref_columns as fb
        from footmav.data_definitions.fbref.utils import opponent_attribute

        opposition_goals = opponent_attribute(fb.GOALS)
        assert opposition_goals.N == "opposition_goals"
        assert opposition_goals.source == fb.DataSource.FBREF
        assert opposition_goals.data_type == "float"
        assert opposition_goals.recalculate_on_aggregation is False
        assert opposition_goals.agg_function == "sum"

        df = pd.DataFrame(
            {
                fb.TEAM.N: ["a", "a", "b", "b", "c", "c"],
                fb.DATE.N: ["date1", "date2", "date1", "date3", "date2", "date3"],
                fb.GOALS.N: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
                fb.OPPONENT.N: ["b", "c", "a", "c", "a", "b"],
            }
        )
        result = opposition_goals.apply(df)
        pd.testing.assert_series_equal(
            pd.Series(
                data=[3.0, 5.0, 1.0, 6.0, 2.0, 4.0],
                index=df.index,
                name="opposition_goals",
            ),
            result,
        )
