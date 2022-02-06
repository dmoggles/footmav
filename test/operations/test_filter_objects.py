import pandas as pd
from unittest.mock import MagicMock, sentinel
import pytest


class TestFilterOperations:
    def test_gt(self):
        from footmav.operations.filter_objects import GT

        df = pd.DataFrame({"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]})
        pd.testing.assert_frame_equal(
            GT.apply(df, df["a"], 2).reset_index(drop=True),
            pd.DataFrame({"a": [3, 4, 5], "b": [30, 40, 50]}),
        )

    def test_gte(self):
        from footmav.operations.filter_objects import GTE

        df = pd.DataFrame({"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]})
        pd.testing.assert_frame_equal(
            GTE.apply(df, df["a"], 2).reset_index(drop=True),
            pd.DataFrame({"a": [2, 3, 4, 5], "b": [20, 30, 40, 50]}),
        )

    def test_lte(self):
        from footmav.operations.filter_objects import LTE

        df = pd.DataFrame({"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]})
        pd.testing.assert_frame_equal(
            LTE.apply(df, df["a"], 2).reset_index(drop=True),
            pd.DataFrame({"a": [1, 2], "b": [10, 20]}),
        )

    def test_lt(self):
        from footmav.operations.filter_objects import LT

        df = pd.DataFrame({"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]})
        pd.testing.assert_frame_equal(
            LT.apply(df, df["a"], 2).reset_index(drop=True),
            pd.DataFrame({"a": [1], "b": [10]}),
        )

    def test_eq(self):
        from footmav.operations.filter_objects import EQ

        df = pd.DataFrame({"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]})
        pd.testing.assert_frame_equal(
            EQ.apply(df, df["a"], 2).reset_index(drop=True),
            pd.DataFrame({"a": [2], "b": [20]}),
        )

    def test_neq(self):
        from footmav.operations.filter_objects import NEQ

        df = pd.DataFrame({"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]})
        pd.testing.assert_frame_equal(
            NEQ.apply(df, df["a"], 2).reset_index(drop=True),
            pd.DataFrame({"a": [1, 3, 4, 5], "b": [10, 30, 40, 50]}),
        )

    def test_isin(self):
        from footmav.operations.filter_objects import IsIn

        df = pd.DataFrame({"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]})
        pd.testing.assert_frame_equal(
            IsIn.apply(df, df["a"], [2, 4]).reset_index(drop=True),
            pd.DataFrame(
                {
                    "a": [
                        2,
                        4,
                    ],
                    "b": [20, 40],
                }
            ),
        )

    def test_str_contains_one_of(self):
        from footmav.operations.filter_objects import StrContainsOneOf

        df = pd.DataFrame(
            {"a": ["aa", "ba", "ac", "ad", "ebc"], "b": [10, 20, 30, 40, 50]}
        )
        pd.testing.assert_frame_equal(
            StrContainsOneOf.apply(df, df["a"], ["b", "d"]).reset_index(drop=True),
            pd.DataFrame(
                {
                    "a": [
                        "ba",
                        "ad",
                        "ebc",
                    ],
                    "b": [
                        20,
                        40,
                        50,
                    ],
                }
            ),
        )

    def test_str_contains(self):
        from footmav.operations.filter_objects import Contains

        df = pd.DataFrame(
            {"a": ["aa", "ba", "ac", "ad", "ebc"], "b": [10, 20, 30, 40, 50]}
        )
        pd.testing.assert_frame_equal(
            Contains.apply(df, df["a"], "b").reset_index(drop=True),
            pd.DataFrame(
                {
                    "a": [
                        "ba",
                        "ebc",
                    ],
                    "b": [
                        20,
                        50,
                    ],
                }
            ),
        )

    def test_str_not_contains(self):
        from footmav.operations.filter_objects import NotContains

        df = pd.DataFrame(
            {"a": ["aa", "ba", "ac", "ad", "ebc"], "b": [10, 20, 30, 40, 50]}
        )
        pd.testing.assert_frame_equal(
            NotContains.apply(df, df["a"], "b").reset_index(drop=True),
            pd.DataFrame(
                {
                    "a": [
                        "aa",
                        "ac",
                        "ad",
                    ],
                    "b": [10, 30, 40],
                }
            ),
        )


class TestFilter:
    @pytest.fixture
    def get_filter(self):
        from footmav.operations.filter_objects import Filter

        data_attr = MagicMock()
        operation = MagicMock()

        filter = Filter(data_attr, sentinel.value, operation)
        return filter, data_attr, operation

    def test_init(self, get_filter):
        filter, data_attr, operation = get_filter
        assert filter._attribute == data_attr
        assert filter._operation == operation
        assert filter._value == sentinel.value

    def test_apply(self, get_filter):
        filter, data_attr, operation = get_filter
        operation.apply = MagicMock(return_value=sentinel.result)
        data_attr.N = MagicMock(return_value="a")

        data = pd.DataFrame({"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]})
        result = filter.apply(data)
        assert result == sentinel.result
        pd.testing.assert_frame_equal(operation.apply.call_args_list[0][0][0], data)
        pd.testing.assert_series_equal(
            operation.apply.call_args_list[0][0][1], data["a"]
        )
        assert operation.apply.call_args_list[0][0][2] == sentinel.value
