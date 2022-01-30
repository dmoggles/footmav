from unittest.mock import MagicMock


class TestDataAttribute:
    def test_data_attribute_init(self):
        from footmav.data_definitions.base import DataAttribute

        f = MagicMock()
        attr = DataAttribute(
            "test_name",
            "test_type",
            f,
            "test_source",
        )
        assert attr.N == "test_name"
        assert attr.data_type == "test_type"
        assert attr.agg_function == f
        assert attr.source == "test_source"
