from unittest.mock import MagicMock, sentinel, patch
import pandas as pd


class TestDataAttribute:
    def test_init(self):
        from footmav.data_definitions.base import DataAttribute
        from footmav.data_definitions.data_sources import DataSource

        f = MagicMock()
        attr = DataAttribute(
            "test_name",
            "test_type",
            f,
            DataSource.FBREF,
        )
        assert attr.N == "test_name"
        assert attr.data_type == "test_type"
        assert attr.agg_function == f
        assert attr.source == DataSource.FBREF


class TestNativeDataAttribute:
    def test_init(self):
        from footmav.data_definitions.base import NativeDataAttribute
        from footmav.data_definitions.data_sources import DataSource

        agg_f = MagicMock()
        transform_f = MagicMock()
        attr = NativeDataAttribute(
            name="test_name",
            data_type="test_type",
            agg_function=agg_f,
            source=DataSource.FBREF,
            rename_to="test_rename",
            transform_function=transform_f,
        )
        assert attr.data_type == "test_type"
        assert attr.agg_function == agg_f
        assert attr.source == DataSource.FBREF

    def test_n_with_rename(self):
        from footmav.data_definitions.base import NativeDataAttribute
        from footmav.data_definitions.data_sources import DataSource

        agg_f = MagicMock()
        transform_f = MagicMock()
        attr = NativeDataAttribute(
            name="test_name",
            data_type="test_type",
            agg_function=agg_f,
            source=DataSource.FBREF,
            rename_to="test_rename",
            transform_function=transform_f,
        )
        assert attr.N == "test_rename"

    def test_n_without_rename(self):
        from footmav.data_definitions.base import NativeDataAttribute
        from footmav.data_definitions.data_sources import DataSource

        agg_f = MagicMock()
        transform_f = MagicMock()
        attr = NativeDataAttribute(
            name="test_name",
            data_type="test_type",
            agg_function=agg_f,
            source=DataSource.FBREF,
            transform_function=transform_f,
        )
        assert attr.N == "test_name"

    def test_user_transform_with_transform_function(self):
        from footmav.data_definitions.base import NativeDataAttribute
        from footmav.data_definitions.data_sources import DataSource

        agg_f = MagicMock()
        transform_f = MagicMock()
        attr = NativeDataAttribute(
            name="test_name",
            data_type="test_type",
            agg_function=agg_f,
            source=DataSource.FBREF,
            transform_function=transform_f,
        )

        column = MagicMock(apply=MagicMock(return_value=sentinel.column))

        result = attr.user_transform(column)
        assert result == sentinel.column
        column.apply.assert_called_once_with(transform_f)

    def test_user_transform_without_transform_function(self):
        from footmav.data_definitions.base import NativeDataAttribute
        from footmav.data_definitions.data_sources import DataSource

        agg_f = MagicMock()
        attr = NativeDataAttribute(
            name="test_name",
            data_type="test_type",
            agg_function=agg_f,
            source=DataSource.FBREF,
        )

        column = MagicMock(apply=MagicMock(return_value=sentinel.column))

        result = attr.user_transform(column)
        assert result == column
        column.apply.assert_not_called()

    def test_pre_type_conversion_transform(self):
        from footmav.data_definitions.base import NativeDataAttribute
        from footmav.data_definitions.data_sources import DataSource

        agg_f = MagicMock()
        attr = NativeDataAttribute(
            name="test_name",
            data_type="test_type",
            agg_function=agg_f,
            source=DataSource.FBREF,
        )

        column = MagicMock(apply=MagicMock(return_value=sentinel.column))

        result = attr.pre_type_conversion_transform(column)
        assert result == column

    def test_post_type_conversion_transform(self):
        from footmav.data_definitions.base import NativeDataAttribute
        from footmav.data_definitions.data_sources import DataSource

        agg_f = MagicMock()
        attr = NativeDataAttribute(
            name="test_name",
            data_type="test_type",
            agg_function=agg_f,
            source=DataSource.FBREF,
        )

        column = MagicMock(apply=MagicMock(return_value=sentinel.column))

        result = attr.post_type_conversion_transform(column)
        assert result == column

    @patch(
        "footmav.data_definitions.base.NativeDataAttribute.pre_type_conversion_transform",
        return_value=MagicMock(astype=MagicMock(return_value=MagicMock())),
    )
    @patch(
        "footmav.data_definitions.base.NativeDataAttribute.post_type_conversion_transform",
        return_value=MagicMock(),
    )
    @patch(
        "footmav.data_definitions.base.NativeDataAttribute.user_transform",
        return_value=MagicMock(),
    )
    def test_transform(
        self,
        user_transform,
        post_type_conversion_transform,
        pre_type_conversion_transform,
    ):
        from footmav.data_definitions.base import NativeDataAttribute
        from footmav.data_definitions.data_sources import DataSource

        agg_f = MagicMock()
        attr = NativeDataAttribute(
            name="test_name",
            data_type="test_type",
            agg_function=agg_f,
            source=DataSource.FBREF,
        )
        column = MagicMock()
        result = attr.apply(column)
        assert result == post_type_conversion_transform.return_value
        user_transform.assert_called_once_with(column)
        pre_type_conversion_transform.assert_called_once_with(
            user_transform.return_value
        )
        pre_type_conversion_transform.return_value.astype.assert_called_once_with(
            "test_type"
        )
        post_type_conversion_transform.assert_called_once_with(
            pre_type_conversion_transform.return_value.astype.return_value
        )


class TestNumericDataAttribute:
    def test_init(self):
        from footmav.data_definitions.base import NumericDataAttribute
        from footmav.data_definitions.data_sources import DataSource

        agg_f = MagicMock()
        attr = NumericDataAttribute(
            name="test_name",
            data_type="float",
            agg_function=agg_f,
            source=DataSource.FBREF,
        )
        assert attr.data_type == "float"
        assert attr.agg_function == agg_f
        assert attr.source == DataSource.FBREF
        assert attr.N == "test_name"

    def test_pre_type_conversion_transform(self):
        from footmav.data_definitions.base import NumericDataAttribute
        from footmav.data_definitions.data_sources import DataSource

        agg_f = MagicMock()
        attr = NumericDataAttribute(
            name="test_name",
            data_type="float",
            agg_function=agg_f,
            source=DataSource.FBREF,
        )
        column = MagicMock(replace=MagicMock(return_value=sentinel.column))
        result = attr.pre_type_conversion_transform(column)
        assert result == sentinel.column
        column.replace.assert_called_once_with("", 0)

    def test_post_type_conversion_transform(self):
        from footmav.data_definitions.base import NumericDataAttribute
        from footmav.data_definitions.data_sources import DataSource

        agg_f = MagicMock()
        attr = NumericDataAttribute(
            name="test_name",
            data_type="float",
            agg_function=agg_f,
            source=DataSource.FBREF,
        )
        column = MagicMock(fillna=MagicMock(return_value=sentinel.column))
        result = attr.post_type_conversion_transform(column)
        assert result == sentinel.column
        column.fillna.assert_called_once_with(0)


class TestFloatNumericDataAttribute:
    def test_init(self):
        from footmav.data_definitions.base import FloatDataAttribute
        from footmav.data_definitions.data_sources import DataSource

        agg_f = MagicMock()
        attr = FloatDataAttribute(
            name="test_name",
            agg_function=agg_f,
            source=DataSource.FBREF,
        )
        assert attr.data_type == "float"
        assert attr.agg_function == agg_f
        assert attr.source == DataSource.FBREF
        assert attr.N == "test_name"


class TestIntNumericDataAttribute:
    def test_init(self):
        from footmav.data_definitions.base import IntDataAttribute
        from footmav.data_definitions.data_sources import DataSource

        agg_f = MagicMock()
        attr = IntDataAttribute(
            name="test_name",
            agg_function=agg_f,
            source=DataSource.FBREF,
        )
        assert attr.data_type == "int"
        assert attr.agg_function == agg_f
        assert attr.source == DataSource.FBREF
        assert attr.N == "test_name"


class TestStrNumericDataAttribute:
    def test_init(self):
        from footmav.data_definitions.base import StrDataAttribute
        from footmav.data_definitions.data_sources import DataSource

        agg_f = MagicMock()
        attr = StrDataAttribute(
            name="test_name",
            agg_function=agg_f,
            source=DataSource.FBREF,
        )
        assert attr.data_type == "str"
        assert attr.agg_function == agg_f
        assert attr.source == DataSource.FBREF
        assert attr.N == "test_name"


class TestDateNumericDataAttribute:
    def test_init(self):
        from footmav.data_definitions.base import DateDataAttribute
        from footmav.data_definitions.data_sources import DataSource

        agg_f = MagicMock()
        attr = DateDataAttribute(
            name="test_name",
            agg_function=agg_f,
            source=DataSource.FBREF,
        )
        assert attr.data_type == pd.Timestamp
        assert attr.agg_function == agg_f
        assert attr.source == DataSource.FBREF
        assert attr.N == "test_name"
