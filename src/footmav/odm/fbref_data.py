from footmav.utils.cleanup import remove_non_top_5_teams
from footmav.odm.data import Data
from footmav.data_definitions.fbref import fbref_columns as fc
from footmav.data_definitions.base import RegisteredAttributeStore
from footmav.data_definitions.derived import DerivedDataAttribute
from footmav.data_definitions.data_sources import DataSource
import pandas as pd


class FbRefData(Data):
    """
    FbRef Data Object.  This object is used to access the data from the fbref data.

    Attributes:
        data (pd.DataFrame): The dataframe containing the data.
    """

    def __init__(self, data: pd.DataFrame):
        data = remove_non_top_5_teams(data).drop_duplicates([fc.PLAYER_ID.N, fc.DATE.N])
        derived_data_to_add = [
            c
            for c in RegisteredAttributeStore.get_registered_attributes()
            if isinstance(c, DerivedDataAttribute)
            and c.source == DataSource.FBREF
            and c.recalculate_on_aggregation
        ]
        for c in derived_data_to_add:
            try:
                data[c.N] = c.apply(data)
            except Exception as e:
                print(f"Error applying {c.N} to fbref data: {e}")
        super().__init__(data)
