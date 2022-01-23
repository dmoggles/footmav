import pytest
from footmav.odm.data import Data
from footmav.operations.pipeable import pipeable
from unittest.mock import MagicMock, PropertyMock, sentinel

import pandas as pd


class TestPipeable:
    def test_pipeable_simple(self):
        f = MagicMock(
            return_value=pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}),
            __name__="test_function",
        )
        data = MagicMock(df=MagicMock())
        type(data).df = PropertyMock(
            return_value=pd.DataFrame({"c": [1, 2, 3], "d": [4, 5, 6]})
        )
        func = pipeable(f)
        result = func(data, 2, a=3)
        assert isinstance(result, Data)
        pd.testing.assert_frame_equal(
            result.df, pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        )
        assert result.unique_keys == data.unique_keys
        assert result._original_data == data.original_data
        f.assert_called_once_with(data.df, 2, a=3)
        assert func.f == f

    def test_pipeable_simple_kwarg_input(self):
        f = MagicMock(
            return_value=pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}),
            __name__="test_function",
        )
        data = MagicMock(df=MagicMock())
        type(data).df = PropertyMock(
            return_value=pd.DataFrame({"c": [1, 2, 3], "d": [4, 5, 6]})
        )
        func = pipeable(f)
        result = func(data=data, b=2, a=3)
        assert isinstance(result, Data)
        pd.testing.assert_frame_equal(
            result.df, pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        )
        assert result.unique_keys == data.unique_keys
        assert result._original_data == data.original_data
        f.assert_called_once_with(data.df, b=2, a=3)
        assert func.f == f

    def test_pipeable_matching_keys(self):
        f = MagicMock(
            return_value=pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}),
            __name__="test_function",
        )
        data = MagicMock(df=MagicMock())
        type(data).df = PropertyMock(
            return_value=pd.DataFrame({"c": [1, 2, 3], "d": [4, 5, 6]})
        )
        type(data).unique_keys = PropertyMock(return_value=[sentinel.k1, sentinel.k2])
        func = pipeable(f, required_unique_keys=[sentinel.k1, sentinel.k2])
        result = func(data, 2, a=3)
        assert isinstance(result, Data)
        pd.testing.assert_frame_equal(
            result.df, pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        )
        assert result.unique_keys == data.unique_keys
        assert result._original_data == data.original_data
        f.assert_called_once_with(data.df, 2, a=3)

    def test_pipeable_non_matching_keys(self):
        f = MagicMock(
            return_value=pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}),
            __name__="test_function",
        )
        data = MagicMock(df=MagicMock())
        type(data).df = PropertyMock(
            return_value=pd.DataFrame({"c": [1, 2, 3], "d": [4, 5, 6]})
        )
        type(data).unique_keys = PropertyMock(return_value=[sentinel.k1, sentinel.k2])
        func = pipeable(f, required_unique_keys=[sentinel.k1, sentinel.k3])
        with pytest.raises(ValueError):
            func(data, 2, a=3)

    def test_pipeable_aggregate_by_test(self):
        f = MagicMock(
            return_value=pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}),
            __name__="aggregate_by",
        )
        data = MagicMock(df=MagicMock())
        type(data).df = PropertyMock(
            return_value=pd.DataFrame({"c": [1, 2, 3], "d": [4, 5, 6]})
        )
        func = pipeable(f)
        result = func(data, [sentinel.k1, sentinel.k2])
        assert isinstance(result, Data)
        pd.testing.assert_frame_equal(
            result.df, pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        )
        assert result.unique_keys == [sentinel.k1, sentinel.k2]
        assert result._original_data == data.original_data
        f.assert_called_once_with(data.df, [sentinel.k1, sentinel.k2])
        assert func.f == f

    def test_pipeable_aggregate_by_kwd_test(self):
        f = MagicMock(
            return_value=pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}),
            __name__="aggregate_by",
        )
        data = MagicMock(df=MagicMock())
        type(data).df = PropertyMock(
            return_value=pd.DataFrame({"c": [1, 2, 3], "d": [4, 5, 6]})
        )
        func = pipeable(f)
        result = func(data, aggregate_cols=[sentinel.k1, sentinel.k2])
        assert isinstance(result, Data)
        pd.testing.assert_frame_equal(
            result.df, pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        )
        assert result.unique_keys == [sentinel.k1, sentinel.k2]
        assert result._original_data == data.original_data
        f.assert_called_once_with(data.df, aggregate_cols=[sentinel.k1, sentinel.k2])
        assert func.f == f
