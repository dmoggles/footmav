from unittest.mock import MagicMock, patch, create_autospec, sentinel


class TestUnderstatData:
    @patch("footmav.odm.understat_data.Data.__init__")
    def test_init(self, super_init):
        with patch(
            "footmav.data_definitions.base.RegisteredAttributeStore._registered_attributes",
            new=dict(),
        ):
            from footmav.odm.understat_data import UnderstatData
            from footmav.data_definitions.derived import DerivedDataAttribute
            from footmav.data_definitions.base import (
                FloatDataAttribute,
                RegisteredAttributeStore,
            )
            from footmav.data_definitions.data_sources import DataSource

            attr1 = create_autospec(DerivedDataAttribute)
            attr1.N = "attr1"
            attr1.source = DataSource.UNDERSTAT
            attr1.apply = MagicMock(return_value=sentinel.attr1_apply)
            attr2 = create_autospec(FloatDataAttribute)
            attr2.N = "new_name"
            attr2.source = DataSource.UNDERSTAT
            attr2.original_name = "orig_name"
            attr2.rename_to = "new_name"

            for a in [attr1, attr2]:
                RegisteredAttributeStore.register_attribute(a)
            renamed_data = MagicMock()
            data = MagicMock(
                columns=["orig_name", "b"], rename=MagicMock(return_value=renamed_data)
            )

            UnderstatData(data)
            data.rename.assert_called_once_with(columns={"orig_name": "new_name"})
            renamed_data.__setitem__.assert_called_once_with(
                "attr1", sentinel.attr1_apply
            )
            super_init.assert_called_once_with(renamed_data)
