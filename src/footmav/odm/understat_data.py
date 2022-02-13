from footmav.odm.data import Data
from footmav.data_definitions.base import NativeDataAttribute, RegisteredAttributeStore
from footmav.data_definitions.derived import DerivedDataAttribute
from footmav.data_definitions.data_sources import DataSource
import pandas as pd


class UnderstatData(Data):
    """
    Understat Data Object.  This object is used to access the data from the understat data.

    Attributes:
        data (pd.DataFrame): The dataframe containing the data.
    """

    def __init__(self, data: pd.DataFrame):
        rename_dict = {
            c.original_name: c.rename_to
            for c in RegisteredAttributeStore.get_registered_attributes()
            if c.source == DataSource.UNDERSTAT
            and isinstance(c, NativeDataAttribute)
            and c.rename_to
        }
        data = data.rename(columns=rename_dict)
        derived_data_to_add = [
            c
            for c in RegisteredAttributeStore.get_registered_attributes()
            if isinstance(c, DerivedDataAttribute) and c.source == DataSource.UNDERSTAT
        ]
        for c in derived_data_to_add:
            data[c.N] = c.apply(data)

        super().__init__(data)
