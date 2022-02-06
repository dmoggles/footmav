from footmav.odm.data import Data

import pandas as pd

from unittest.mock import MagicMock, sentinel, create_autospec
from unittest import mock
from pandas.testing import assert_frame_equal


class TestData:
    def test_init_all_params(self):
        data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        original_data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        unique_keys = [sentinel.k1, sentinel.k2]
        d = Data(data, original_data, unique_keys)
        assert_frame_equal(d._data, data)
        assert_frame_equal(d._original_data, original_data)
        assert d._unique_keys == unique_keys

    def test_init_no_original(self):
        data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        unique_keys = [sentinel.k1, sentinel.k2]
        d = Data(data, unique_keys=unique_keys)
        assert_frame_equal(d._data, data)
        assert_frame_equal(d._original_data, data)
        assert d._unique_keys == unique_keys

    def test_df_property(self):
        data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        d = Data(data)
        assert_frame_equal(d.df, data)

    def test_original_data_property(self):
        data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        original_data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        d = Data(data, original_data)
        assert_frame_equal(d.original_data, original_data)

    def test_unique_keys_property(self):
        data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [8, 7, 9]})
        d = Data(data, unique_keys=["a", "b"])
        assert d.unique_keys == ["a", "b"]

    def test_n_property(self):
        data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        d = Data(data)
        assert d.n == 3

    def test_pipe(self):
        test_function = MagicMock(return_value="test")
        data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        d = Data(data)

        actual = d.pipe(test_function, 1, 2, a=3)
        test_function.assert_called_once_with(d, 1, 2, a=3)
        assert actual == "test"

    def test_pipe_with_original_data(self):
        test_function = mock.create_autospec(lambda data, original_data, a, b: "test")
        test_function.return_value = "test"
        data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        original_data = pd.DataFrame({"e": [1, 2, 3], "f": [4, 5, 6]})
        d = Data(data, original_data)

        actual = d.pipe(test_function, 1, b=2)
        test_function.assert_called_once_with(d, original_data, 1, b=2)
        assert actual == "test"

    def test_with_attributes(self):
        data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        d = Data(data)
        attr1 = MagicMock(
            N="attr1", apply=MagicMock(return_value=pd.Series([11, 22, 33]))
        )
        attr2 = MagicMock(
            N="attr2", apply=MagicMock(return_value=pd.Series([44, 55, 66]))
        )
        d = d.with_attributes([attr1, attr2])
        pd.testing.assert_frame_equal(
            d.df,
            pd.DataFrame(
                {
                    "a": [1, 2, 3],
                    "b": [4, 5, 6],
                    "attr1": [11, 22, 33],
                    "attr2": [44, 55, 66],
                }
            ),
        )

    def test_with_attributes_single(self):
        from footmav.data_definitions.derived import FunctionDerivedDataAttribute

        data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        d = Data(data)
        attr1 = create_autospec(
            FunctionDerivedDataAttribute,
            N="attr1",
        )
        attr1.apply.return_value = pd.Series([11, 22, 33])

        d = d.with_attributes(attr1)
        pd.testing.assert_frame_equal(
            d.df,
            pd.DataFrame(
                {
                    "a": [1, 2, 3],
                    "b": [4, 5, 6],
                    "attr1": [11, 22, 33],
                }
            ),
        )
