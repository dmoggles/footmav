from unittest.mock import MagicMock, sentinel, patch
import pytest
import pandas as pd


class TestDerivedDataAttribute:
    from footmav.data_definitions.derived import DerivedDataAttribute

    class TestDerivedDataAttributeClass(DerivedDataAttribute):

        apply = MagicMock()

    @pytest.fixture
    def get_class(self):
        from footmav.data_definitions.data_sources import DataSource

        return TestDerivedDataAttribute.TestDerivedDataAttributeClass(
            "test_name", "test_type", "test_agg_function", DataSource.FBREF
        )

    def test_init(
        self, get_class: "TestDerivedDataAttribute.TestDerivedDataAttributeClass"
    ):
        with patch(
            "footmav.data_definitions.base.RegisteredAttributeStore._registered_attributes",
            new=dict(),
        ):
            from footmav.data_definitions.data_sources import DataSource

            derived = get_class
            assert derived._name == "test_name"
            assert derived._agg_function == "test_agg_function"
            assert derived._data_type == "test_type"
            assert derived._source == DataSource.FBREF
            assert derived._recalculate_on_aggregation

    def test_init_twice(self):
        with patch(
            "footmav.data_definitions.base.RegisteredAttributeStore._registered_attributes",
            new=dict(),
        ):
            from footmav.data_definitions.data_sources import DataSource

            TestDerivedDataAttribute.TestDerivedDataAttributeClass(
                "test_name", "test_type", "test_agg_function", DataSource.FBREF
            )
            TestDerivedDataAttribute.TestDerivedDataAttributeClass(
                "test_name", "test_type", "test_agg_function", DataSource.FBREF
            )
            assert True

    def test_init_twice_first_is_native(self):
        with patch(
            "footmav.data_definitions.base.RegisteredAttributeStore._registered_attributes",
            new=dict(),
        ):
            from footmav.data_definitions.data_sources import DataSource
            from footmav.data_definitions.base import NativeDataAttribute

            with pytest.raises(
                ValueError,
                match="Attribute test_name is already registered. Please use a different name.",
            ):
                NativeDataAttribute("test_name", "test_type", DataSource.FBREF)
                TestDerivedDataAttribute.TestDerivedDataAttributeClass(
                    "test_name", "test_type", "test_agg_function", DataSource.FBREF
                )


class TestFunctionDerivedDataAttribute:
    @pytest.fixture
    def get_class(self):
        with patch(
            "footmav.data_definitions.base.RegisteredAttributeStore._registered_attributes",
            new=dict(),
        ):
            from footmav.data_definitions.derived import FunctionDerivedDataAttribute
            from footmav.data_definitions.data_sources import DataSource

            function = MagicMock(apply=MagicMock(return_value=sentinel.function))
            obj = FunctionDerivedDataAttribute(
                "test_name",
                function,
                "test_type",
                DataSource.FBREF,
                "test_agg_function",
            )
            return obj, function

    def test_init(self, get_class):
        with patch(
            "footmav.data_definitions.base.RegisteredAttributeStore._registered_attributes",
            new=dict(),
        ):
            from footmav.data_definitions.data_sources import DataSource

            data_attribute, function = get_class
            assert data_attribute._name == "test_name"
            assert data_attribute._agg_function == "test_agg_function"
            assert data_attribute._data_type == "test_type"
            assert data_attribute._source == DataSource.FBREF
            assert data_attribute._recalculate_on_aggregation
            assert data_attribute.function == function

    def test_apply(self, get_class):
        with patch(
            "footmav.data_definitions.base.RegisteredAttributeStore._registered_attributes",
            new=dict(),
        ):
            data_attribute, function = get_class
            result = data_attribute.apply(sentinel.data)
            assert result == sentinel.function
            function.apply.assert_called_with(sentinel.data)


class TestLambdaAttribute:
    def test_lambda_attribute(self):
        from footmav.data_definitions.derived import lambda_attribute
        from footmav.data_definitions.data_sources import DataSource

        @lambda_attribute
        def test_function(data):
            return data["x"] + data["y"]

        df = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})
        df_result = test_function.apply(df)
        pd.testing.assert_series_equal(
            df_result,
            pd.Series(data=[5, 7, 9], index=df.index),
        )
        assert test_function.N == "test_function"
        assert test_function.data_type == "float"
        assert test_function.source == DataSource.FBREF

    def test_lambda_attribute_parameters(self):
        from footmav.data_definitions.derived import lambda_attribute
        from footmav.data_definitions.data_sources import DataSource

        @lambda_attribute(data_type="str", data_source=DataSource.UNDERSTAT)
        def test_function(data):
            return data["x"] + data["y"]

        df = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})
        df_result = test_function.apply(df)
        pd.testing.assert_series_equal(
            df_result,
            pd.Series(data=[5, 7, 9], index=df.index),
        )
        assert test_function.N == "test_function"
        assert test_function.data_type == "str"
        assert test_function.source == DataSource.UNDERSTAT
