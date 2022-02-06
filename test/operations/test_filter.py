import pandas as pd
from unittest.mock import MagicMock


def test_filter():
    from footmav.operations.filter import filter

    df = pd.DataFrame(
        {
            "a": ["aa", "ba", "ac", "ad", "ebc"],
            "b": [10, 20, 30, 40, 50],
        }
    )
    return_df = pd.DataFrame(
        {
            "a": ["ba", "ebc"],
            "b": [20, 50],
        }
    )
    filter_obj = MagicMock(apply=MagicMock(return_value=return_df))
    result = filter.f(df, [filter_obj])
    pd.testing.assert_frame_equal(result, return_df)
    pd.testing.assert_frame_equal(filter_obj.apply.call_args_list[0][0][0], df)
