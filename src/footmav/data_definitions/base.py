from typing import Callable, Union
from footmav.data_definitions import data_sources


class DataAttribute:
    def __init__(
        self,
        name: str,
        data_type: Union[str, type],
        agg_function: Callable,
        source: data_sources.DataSource,
    ):
        self._name = name
        self._data_type = data_type
        self._agg_function = agg_function
        self._source = source

    @property
    def N(self):
        return self.name

    @property
    def source(self):
        return self._source
