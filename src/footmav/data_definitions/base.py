from typing import Callable, Union
from footmav.data_definitions import data_sources


class DataAttribute:
    """
    Base class for a DataAttribute.  A DataAttribute is a representation of a particular data feature,
    normally implemented as a column in a DataFrame. The base class is meant to be descriptive, to provide
    a concrete object that can be identified through pylance and other developer and reflection tools.

    It also provides some additional information that can be used by calculation, such as the data type,
    the aggregation function, and the source of the data.

    Attributes:
        name (str): The name of the DataAttribute
        data_type (Union[str, type]): The data type of the DataAttribute
        agg_function (Callable): The aggregation function that describes how the data should be aggregated
        source (DataSource): The source of the data (e.g. FBRef, Understat, etc)
    """

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
        """
        Return the name of the DataAttribute

        Returns:
            str: The name of the DataAttribute
        """
        return self._name

    @property
    def source(self):
        """
        Return the source of the DataAttribute

            Returns:
                DataSource: The source of the DataAttribute
        """

        return self._source

    @property
    def data_type(self):
        """
        Return the data type of the DataAttribute

        Returns:
            Union[str, type]: The data type of the DataAttribute
        """
        return self._data_type

    @property
    def agg_function(self):
        """
        Return the aggregation function of the DataAttribute

        Returns:
            Callable: The aggregation function of the DataAttribute
        """
        return self._agg_function
