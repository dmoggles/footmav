import pytest
from footmav.odm.data import Data
from footmav.operations.pipeable import pipeable
from unittest.mock import MagicMock, PropertyMock, sentinel, create_autospec

import pandas as pd


class TestPipeable:
    def test_pipeable_simple(self):
        f = MagicMock(
            return_value=pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}),
            __name__="test_function",
        )
        data = create_autospec(Data)

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
        data = create_autospec(Data)
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

    def test_pipeable_with_data_arg(self):
        f = MagicMock(
            return_value=pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}),
            __name__="test_function",
        )
        data = create_autospec(Data)
        data2 = create_autospec(Data)
        type(data).df = PropertyMock(
            return_value=pd.DataFrame({"c": [1, 2, 3], "d": [4, 5, 6]})
        )
        type(data2).df = PropertyMock(
            return_value=pd.DataFrame({"e": [1, 2, 3], "f": [4, 5, 6]})
        )
        func = pipeable(f)
        result = func(data, data2, a=3)
        assert isinstance(result, Data)
        pd.testing.assert_frame_equal(
            result.df, pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        )
        assert result.unique_keys == data.unique_keys
        assert result._original_data == data.original_data
        f.assert_called_once_with(data.df, data2.df, a=3)
        assert func.f == f

    def test_pipeable_with_data_kwarg(self):
        f = MagicMock(
            return_value=pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}),
            __name__="test_function",
        )
        data = create_autospec(Data)
        data2 = create_autospec(Data)
        type(data).df = PropertyMock(
            return_value=pd.DataFrame({"c": [1, 2, 3], "d": [4, 5, 6]})
        )
        type(data2).df = PropertyMock(
            return_value=pd.DataFrame({"e": [1, 2, 3], "f": [4, 5, 6]})
        )
        func = pipeable(f)
        result = func(data, data2=data2, a=3)
        assert isinstance(result, Data)
        pd.testing.assert_frame_equal(
            result.df, pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        )
        assert result.unique_keys == data.unique_keys
        assert result._original_data == data.original_data
        f.assert_called_once_with(data.df, data2=data2.df, a=3)
        assert func.f == f

    def test_pipeable_matching_keys(self):
        f = MagicMock(
            return_value=pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}),
            __name__="test_function",
        )
        data = create_autospec(Data)
        type(data).df = PropertyMock(
            return_value=pd.DataFrame({"c": [1, 2, 3], "d": [4, 5, 6]})
        )
        type(data).unique_keys = PropertyMock(return_value=[sentinel.k1, sentinel.k2])
        func = pipeable(required_unique_keys=[sentinel.k1, sentinel.k2])(f)
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
        data = create_autospec(Data)
        type(data).df = PropertyMock(
            return_value=pd.DataFrame({"c": [1, 2, 3], "d": [4, 5, 6]})
        )
        type(data).unique_keys = PropertyMock(return_value=[sentinel.k1, sentinel.k3])
        func = pipeable(required_unique_keys=[sentinel.k1, sentinel.k2])(f)
        with pytest.raises(ValueError):
            func(data, 2, a=3)

    def test_pipeable_aggregate_by_test(self):
        f = MagicMock(
            return_value=pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}),
            __name__="aggregate_by",
        )
        data = create_autospec(Data)
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
        data = create_autospec(Data)
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
