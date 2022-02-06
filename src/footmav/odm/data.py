from typing import Callable, Iterable, List, Union
import pandas as pd
from footmav.data_definitions.base import DataAttribute
import inspect

from footmav.data_definitions.derived import DerivedDataAttribute


class Data:
    """
    Base Data class for holding and manipulating a dataframe holding football data.

    Since the purpose of this object is to do state transitions that create new Data objects,
    the class also stores the original user-supplied dataframe. This is used for when a data transition
    needs access to the original data for calculations.

    Because dataframe indexes are evil, the Data class also stores a list of unique keys that can be used
    to figure out how the dataframe is currently aggregated, for purposes of validation and merges.
    """

    def __init__(
        self,
        data: pd.DataFrame,
        original_data: pd.DataFrame = None,
        unique_keys: List[DataAttribute] = None,
    ):
        self._data = data
        if original_data is None:
            self._original_data = data
        else:
            self._original_data = original_data
        if unique_keys is None:
            self._unique_keys = []
        else:
            self._unique_keys = unique_keys

    @property
    def n(self) -> int:
        """
        Returns the number of records in the dataset

        Returns:
            int: The number of records in the dataset
        """
        return len(self.df)

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns the raw dataframe stored in the Data object

        Returns:
            pd.DataFrame: The raw dataframe stored in the Data object
        """
        return self._data

    @property
    def unique_keys(self) -> List[DataAttribute]:
        """
        Returns the list of unique keys stored in the Data object

        Returns:
            List[DataAttribute]: The list of unique keys stored in the Data object
        """
        return self._unique_keys

    @property
    def original_data(self) -> pd.DataFrame:
        """
        Returns the original dataframe stored in the Data object.  We store this in case an operation needs to have access to original data.
        This is triggered by having a keyword argument `original_data` in the function signature.

        Returns:
            pd.DataFrame: The original dataframe stored in the Data object
        """
        return self._original_data

    def pipe(self, func: Callable[..., "Data"], *args, **kwargs) -> "Data":
        """
        Pipe a function to the Data object

        Args:
            func (Callable[["Data"], "Data"]): The function to pipe to the Data object
            *args: Arguments to pass to the function
            **kwargs: Keyword arguments to pass to the function

        Returns:
            Data: The Data object after the function has been applied
        """
        func_spec = inspect.getfullargspec(func)
        if len(func_spec.args) > 1 and func_spec.args[1] == "original_data":
            return func(self, self._original_data, *args, **kwargs)
        else:
            return func(self, *args, **kwargs)

    def with_attributes(
        self, attributes: Union[Iterable[DerivedDataAttribute], DerivedDataAttribute]
    ) -> "Data":
        """
        Add provided attributes to the Data object

        Args:
            attributes (Union[Iterable[DerivedDataAttribute], DerivedDataAttribute]): The attributes to add to the Data object

        Returns:
            Data: The Data object with the provided attributes added

        """
        if isinstance(attributes, DerivedDataAttribute):
            attributes = [attributes]
        for attr in attributes:
            self.df[attr.N] = attr.apply(self.df)
        return self
