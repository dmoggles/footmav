from footmav.odm.data import Data
import pandas as pd
from typing import List, Dict, Any
from footmav.data_definitions.base import DataAttribute


class WhoscoredData(Data):
    """
    Data class for holding Whoscored event data, and associated metadata
    """

    def __init__(
        self,
        data: pd.DataFrame,
        metadata: Dict[int, Dict[str, Any]],
        original_data: pd.DataFrame = None,
        unique_keys: List[DataAttribute] = None,
    ):
        super().__init__(data, original_data, unique_keys)
        self._metadata_dict = metadata
