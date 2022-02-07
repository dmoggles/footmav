from unittest.mock import MagicMock, sentinel, patch
import pytest


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
            new=set(),
        ):
            from footmav.data_definitions.data_sources import DataSource

            derived = get_class
            assert derived._name == "test_name"
            assert derived._agg_function == "test_agg_function"
            assert derived._data_type == "test_type"
            assert derived._source == DataSource.FBREF
            assert derived._recalculate_on_aggregation

        def test_apply(
            self, get_class: "TestDerivedDataAttribute.TestDerivedDataAttributeClass"
        ):
            derived = get_class
            assert derived.recalculate_on_aggregation


class TestFunctionDerivedDataAttribute:
    @pytest.fixture
    def get_class(self):
        with patch(
            "footmav.data_definitions.base.RegisteredAttributeStore._registered_attributes",
            new=set(),
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
            new=set(),
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
            new=set(),
        ):
            data_attribute, function = get_class
            result = data_attribute.apply(sentinel.data)
            assert result == sentinel.function
            function.apply.assert_called_with(sentinel.data)
