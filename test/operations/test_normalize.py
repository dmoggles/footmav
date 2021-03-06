import pandas as pd
from unittest.mock import MagicMock, patch
import pytest


def test_per_90():
    with patch(
        "footmav.data_definitions.base.RegisteredAttributeStore._registered_attributes",
        new=dict(),
    ):
        from footmav.operations.normalize import per_90
        from footmav.data_definitions.derived import FunctionDerivedDataAttribute
        from footmav.data_definitions.base import StrDataAttribute, FloatDataAttribute
        from footmav.data_definitions.attribute_functions import Col

        PLAYER = StrDataAttribute("player", MagicMock())
        MINUTES = FloatDataAttribute("minutes", MagicMock(), normalizable=False)
        GOALS = FloatDataAttribute("goals", MagicMock())
        SHOTS_TOTAL = FloatDataAttribute("shots_total", MagicMock())
        df = pd.DataFrame(
            {
                PLAYER.N: ["aa", "ba", "ac", "ad", "ebc"],
                MINUTES.N: [10, 20, 30, 40, 50],
                GOALS.N: [1, 2, 2, 5, 2],
                SHOTS_TOTAL.N: [2, 2, 4, 5, 6],
            }
        )

        PCT_SHOT = FunctionDerivedDataAttribute(
            "shot%",
            Col(GOALS) / Col(SHOTS_TOTAL),
            data_type=float,
            source=MagicMock(),
            recalculate_on_aggregation=True,
        )
        df[PCT_SHOT.N] = PCT_SHOT.apply(df)

        result = per_90.f(df)
        pd.testing.assert_frame_equal(
            result.sort_index(axis=1).reset_index(drop=True),
            pd.DataFrame(
                {
                    PLAYER.N: ["aa", "ba", "ac", "ad", "ebc"],
                    MINUTES.N: [10, 20, 30, 40, 50],
                    GOALS.N: [
                        1.0 * 9.0,
                        2 * 4.5,
                        2 * 3.0,
                        5 * 9.0 / 4.0,
                        2 * 9.0 / 5.0,
                    ],
                    SHOTS_TOTAL.N: [
                        2 * 9.0,
                        2 * 4.5,
                        4 * 3,
                        5 * 9.0 / 4.0,
                        6 * 9.0 / 5.0,
                    ],
                    PCT_SHOT.N: [0.5, 1.0, 0.5, 1.0, 1.0 / 3.0],
                }
            )
            .sort_index(axis=1)
            .reset_index(drop=True),
        )


def test_per_90_no_mins():
    with patch(
        "footmav.data_definitions.base.RegisteredAttributeStore._registered_attributes",
        new=dict(),
    ):
        from footmav.operations.normalize import per_90

        from footmav.data_definitions.derived import FunctionDerivedDataAttribute
        from footmav.data_definitions.attribute_functions import Col
        from footmav.data_definitions.base import StrDataAttribute, FloatDataAttribute

        PLAYER = StrDataAttribute("player", MagicMock())
        GOALS = FloatDataAttribute("goals", MagicMock())
        SHOTS_TOTAL = FloatDataAttribute("shots_total", MagicMock())

        df = pd.DataFrame(
            {
                PLAYER.N: ["aa", "ba", "ac", "ad", "ebc"],
                GOALS.N: [1, 2, 2, 5, 2],
                SHOTS_TOTAL.N: [2, 2, 4, 5, 6],
            }
        )

        PCT_SHOT = FunctionDerivedDataAttribute(
            "shot%",
            Col(GOALS) / Col(SHOTS_TOTAL),
            data_type=float,
            source=MagicMock(),
            recalculate_on_aggregation=True,
        )
        df[PCT_SHOT.N] = PCT_SHOT.apply(df)
        with pytest.raises(ValueError):
            per_90.f(df)


def test_z_score():
    with patch(
        "footmav.data_definitions.base.RegisteredAttributeStore._registered_attributes",
        new=dict(),
    ):
        from footmav.operations.normalize import z_score
        from footmav.data_definitions.base import StrDataAttribute, FloatDataAttribute

        PLAYER = StrDataAttribute("player", MagicMock())
        GOALS = FloatDataAttribute("goals", MagicMock())
        df = pd.DataFrame(
            {
                PLAYER.N: ["aa", "ba", "ac", "ad", "ebc"],
                GOALS.N: [1, 2, 2, 5, 2],
            }
        )
        result = z_score.f(df)
        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(
                {
                    PLAYER.N: ["aa", "ba", "ac", "ad", "ebc"],
                    GOALS.N: [
                        -0.92313266,
                        -0.26375219,
                        -0.26375219,
                        1.71438923,
                        -0.26375219,
                    ],
                }
            ),
        )
