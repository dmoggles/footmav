from unittest.mock import MagicMock, patch, create_autospec, sentinel


class TestFbRefData:
    @patch("footmav.odm.fbref_data.Data.__init__")
    @patch("footmav.odm.fbref_data.remove_non_top_5_teams")
    def test_init(self, remove_non_top_5_teams, super_init):
        with patch(
            "footmav.data_definitions.base.RegisteredAttributeStore._registered_attributes",
            new=dict(),
        ):
            from footmav.odm.fbref_data import FbRefData
            from footmav.data_definitions.derived import DerivedDataAttribute
            from footmav.data_definitions.base import (
                FloatDataAttribute,
                RegisteredAttributeStore,
            )
            from footmav.data_definitions.data_sources import DataSource

            attr1 = create_autospec(DerivedDataAttribute)
            attr1.N = "attr1"
            attr1.source = DataSource.FBREF
            attr1.recalculate_on_aggregation = True
            attr1.apply = MagicMock(return_value=sentinel.attr1_apply)
            attr2 = create_autospec(DerivedDataAttribute)
            attr2.N = "attr2"
            attr2.source = DataSource.FBREF
            attr2.recalculate_on_aggregation = False
            attr3 = create_autospec(DerivedDataAttribute)
            attr3.N = "attr3"
            attr3.source = DataSource.UNDERSTAT
            attr3.recalculate_on_aggregation = True
            attr4 = create_autospec(FloatDataAttribute)
            attr4.N = "attr4"
            attr5 = create_autospec(DerivedDataAttribute)
            attr5.N = "attr5"
            attr5.apply = MagicMock(side_effect=Exception("apply"))
            attr5.source = DataSource.FBREF

            for a in [attr1, attr2, attr3, attr4, attr5]:
                RegisteredAttributeStore.register_attribute(a)
            data_with_duplicates_dropped = MagicMock()
            drop_duplicates_mock = MagicMock(return_value=data_with_duplicates_dropped)
            data_with_non_top_5_teams_removed = MagicMock(
                drop_duplicates=drop_duplicates_mock
            )
            remove_non_top_5_teams.return_value = data_with_non_top_5_teams_removed

            data = MagicMock(columns=["a", "b", "c", "d"])

            FbRefData(data)
            remove_non_top_5_teams.assert_called_once_with(data)
            drop_duplicates_mock.assert_called_once_with(["player_id", "date"])
            super_init.assert_called_once()
            data_with_duplicates_dropped.__setitem__.assert_called_once_with(
                "attr1", sentinel.attr1_apply
            )
            attr1.apply.assert_called_once_with(data_with_duplicates_dropped)
            attr2.apply.assert_not_called()
            attr3.apply.assert_not_called()
            attr4.apply.assert_not_called()
