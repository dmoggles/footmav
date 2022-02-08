from typing import Callable, Optional, Union
from footmav.data_definitions.base import DataAttribute
from footmav.data_definitions.data_sources import DataSource
from footmav.data_definitions.function_builder import FunctionBuilder
import pandas as pd
import abc


class DerivedDataAttribute(DataAttribute, abc.ABC):
    """Base class for a derived data attribute, ie a user-defined data attribute that's calculated from attributes in the original data source.

    Attributes:
        name (str): Name of the derived data attribute.
        data_type (Union[str, type]): Type of the derived data attribute.
        agg_function (Union[Callable, str]): Aggregation function to apply to the data attribute.
        source (DataSource): Data source that the derived data attribute is derived from.
        recalculate_on_aggregation (bool): Whether to recalculate the derived data attribute when the source data is aggregated.
    """

    def __init__(
        self,
        name: str,
        data_type: Union[str, type],
        agg_function: Optional[Union[Callable, str]],
        source: DataSource,
        recalculate_on_aggregation: bool = True,
    ):
        self._recalculate_on_aggregation = recalculate_on_aggregation
        super().__init__(name, data_type, agg_function, source)

    @property
    def recalculate_on_aggregation(self) -> bool:
        """
        Whether to recalculate the derived data attribute when the source data is aggregated.

        Returns:
            bool: Whether to recalculate the derived data attribute when the source data is aggregated.
        """
        return self._recalculate_on_aggregation

    @abc.abstractmethod
    def apply(self, data: pd.DataFrame) -> pd.Series:
        """
        Calculate the data attribute from the baseline data.

        Args:
            data (pd.DataFrame): DataFrame containing the baseline data.

        Returns:
            pd.Series: Result of applying the operator to the operands.
        """


class FunctionDerivedDataAttribute(DerivedDataAttribute):
    """
    Derived data attribute that's computed using the `FunctionBuilder` functionality.  This is done by performing arithmetic on the data attributes.

    Attributes:
        name (str): Name of the derived data attribute.
        function (FunctionBuilder): FunctionBuilder that calculates the derived data attribute.
        data_type (Union[str, type]): Type of the derived data attribute.
        source (DataSource): Data source that the derived data attribute is derived from.
        agg_function (Union[Callable, str]): Aggregation function to apply to the data attribute.
        recalculate_on_aggregation (bool): Whether to recalculate the derived data attribute when the source data is aggregated.
    """

    def __init__(
        self,
        name: str,
        function: FunctionBuilder,
        data_type: Union[str, type],
        source: DataSource,
        agg_function: Optional[Union[Callable, str]] = None,
        recalculate_on_aggregation: bool = True,
    ):
        self.function = function
        super().__init__(
            name, data_type, agg_function, source, recalculate_on_aggregation
        )

    def apply(self, data: pd.DataFrame) -> pd.Series:
        """Calculate the data attribute from the baseline data.

        Args:
            data (pd.DataFrame): DataFrame containing the baseline data.

        Returns:
            pd.Series: Result of applying the operator to the operands.
        """
        return self.function.apply(data)
