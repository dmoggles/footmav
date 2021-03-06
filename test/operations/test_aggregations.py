from unittest.mock import MagicMock, patch
import pandas as pd


def test_aggregate_by():
    with patch(
        "footmav.data_definitions.base.RegisteredAttributeStore._registered_attributes",
        new=dict(),
    ):
        from footmav.operations.aggregations import aggregate_by
        from footmav.data_definitions.base import StrDataAttribute, FloatDataAttribute
        from footmav.data_definitions.derived import FunctionDerivedDataAttribute
        from footmav.data_definitions.attribute_functions import Col, Lit

        df = pd.DataFrame(
            {
                "test_team": [
                    "chelsea",
                    "chelsea",
                    "chelsea",
                    "arsenal",
                    "arsenal",
                    "arsenal",
                ],
                "test_goals": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
                "test_assists": [7.0, 8.0, 9.0, 10.0, 11.0, 12.0],
            }
        )
        TEAM = StrDataAttribute("test_team", MagicMock())
        GOALS = FloatDataAttribute("test_goals", MagicMock())
        ASSISTS = FloatDataAttribute("test_assists", MagicMock(), agg_function="mean")
        GOALS_PLUS_ASSISTS = FunctionDerivedDataAttribute(
            "goals_plus_assists",
            Col(GOALS) + Col(ASSISTS),
            data_type=float,
            source=MagicMock(),
            recalculate_on_aggregation=False,
        )
        df[GOALS_PLUS_ASSISTS.N] = GOALS_PLUS_ASSISTS.apply(df)
        GOALS_MINUS_ASSISTS = FunctionDerivedDataAttribute(
            "goals_minus_assists",
            Col(GOALS) - Col(ASSISTS),
            data_type=float,
            source=MagicMock(),
            recalculate_on_aggregation=False,
            agg_function="sum",
        )
        df[GOALS_MINUS_ASSISTS.N] = GOALS_MINUS_ASSISTS.apply(df)

        GOALS_TIMES_2 = FunctionDerivedDataAttribute(
            "2_times_goals",
            Lit(2) * Col(GOALS),
            data_type=float,
            source=MagicMock(),
            recalculate_on_aggregation=True,
        )
        df[GOALS_TIMES_2.N] = GOALS_TIMES_2.apply(df)

        GOALS_TIMES_3 = FunctionDerivedDataAttribute(
            "3_times_goals",
            Lit(3) * Col(GOALS),
            data_type=float,
            source=MagicMock(),
            recalculate_on_aggregation=False,
        )
        df[GOALS_TIMES_3.N] = GOALS_TIMES_3.apply(df)

        FunctionDerivedDataAttribute(
            "4_times_goals",
            Lit(3) * Col(GOALS),
            data_type=float,
            source=MagicMock(),
            recalculate_on_aggregation=True,
        )

        result = aggregate_by.f(df, [TEAM])
        pd.testing.assert_frame_equal(
            result.sort_values("test_team").sort_index(axis=1).reset_index(drop=True),
            pd.DataFrame(
                {
                    "test_team": ["chelsea", "arsenal"],
                    "test_goals": [6.0, 15.0],
                    "test_assists": [8.0, 11.0],
                    "goals_minus_assists": [-18.0, -18.0],
                    "2_times_goals": [12.0, 30.0],
                }
            )
            .sort_values("test_team")
            .sort_index(axis=1)
            .reset_index(drop=True),
        )


def test_rank():
    with patch(
        "footmav.data_definitions.base.RegisteredAttributeStore._registered_attributes",
        new=dict(),
    ):
        from footmav.operations.aggregations import rank

        df = pd.DataFrame(
            {
                "test_team": ["chelsea", "chelsea", "arsenal", "arsenal"],
                "player_name": ["p1", "p2", "p3", "p4"],
                "test_goals": [2.0, 1.0, 5.0, 4.0],
            }
        )

        result = rank.f(df, pct=False)
        pd.testing.assert_frame_equal(
            result.sort_index(axis=1).reset_index(drop=True),
            pd.DataFrame(
                {
                    "test_team": ["chelsea", "chelsea", "arsenal", "arsenal"],
                    "player_name": ["p1", "p2", "p3", "p4"],
                    "test_goals": [2.0, 1.0, 4.0, 3.0],
                }
            )
            .sort_index(axis=1)
            .reset_index(drop=True),
        )


def test_rank_pct():
    with patch(
        "footmav.data_definitions.base.RegisteredAttributeStore._registered_attributes",
        new=dict(),
    ):
        from footmav.operations.aggregations import rank

        df = pd.DataFrame(
            {
                "test_team": ["chelsea", "chelsea", "arsenal", "arsenal"],
                "player_name": ["p1", "p2", "p3", "p4"],
                "test_goals": [2.0, 1.0, 5.0, 4.0],
            }
        )

        result = rank.f(df, pct=True)
        pd.testing.assert_frame_equal(
            result.sort_index(axis=1).reset_index(drop=True),
            pd.DataFrame(
                {
                    "test_team": ["chelsea", "chelsea", "arsenal", "arsenal"],
                    "player_name": ["p1", "p2", "p3", "p4"],
                    "test_goals": [0.5, 0.25, 1.0, 0.75],
                }
            )
            .sort_index(axis=1)
            .reset_index(drop=True),
        )
