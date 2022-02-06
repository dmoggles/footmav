import abc
import pandas as pd
import numpy as np
from footmav.data_definitions.base import DataAttribute


class FilterOperation(abc.ABC):
    """
    Baseline class for definiting filtering operations on DataFrames.
    """

    @staticmethod
    @abc.abstractmethod
    def apply(frame: pd.DataFrame, column: pd.Series, value):
        """
        Apply the filter operation to the dataframe.

        Args:
            frame (pd.DataFrame): DataFrame to filter.
            column (pd.Series): Column to apply the filter operation to.
            value: Value to filter the column by.

        Returns:
            pd.DataFrame: Filtered dataframe.
        """


class GT(FilterOperation):
    """
    Greater than filter operation.
    """

    @staticmethod
    def apply(frame: pd.DataFrame, column: pd.Series, value) -> pd.DataFrame:
        """
        Apply the greater than filter operation to the dataframe.

        Args:
            frame (pd.DataFrame): DataFrame to filter.
            column (pd.Series): Column to apply the filter operation to.
            value: Value to filter the column by.

        Returns:
            pd.DataFrame: Filtered dataframe.
        """
        return frame.loc[column > value]


class GTE(FilterOperation):
    """
    Greater than or equal to filter operation.
    """

    @staticmethod
    def apply(frame: pd.DataFrame, column: pd.Series, value):
        """
        Apply the greater than or equal to filter operation to the dataframe.

        Args:
            frame (pd.DataFrame): DataFrame to filter.
            column (pd.Series): Column to apply the filter operation to.
            value: Value to filter the column by.

        Returns:
            pd.DataFrame: Filtered dataframe.
        """
        return frame.loc[column >= value]


class LT(FilterOperation):
    """
    Less than filter operation.
    """

    @staticmethod
    def apply(frame: pd.DataFrame, column: pd.Series, value):
        """
        Apply the less than filter operation to the dataframe.

        Args:

            frame (pd.DataFrame): DataFrame to filter.
            column (pd.Series): Column to apply the filter operation to.
            value: Value to filter the column by.

        Returns:
            pd.DataFrame: Filtered dataframe.

        """
        return frame.loc[column < value]


class LTE(FilterOperation):
    """
    Less than or equal to filter operation.
    """

    @staticmethod
    def apply(frame: pd.DataFrame, column: pd.Series, value):
        """
        Apply the less than or equal to filter operation to the dataframe.

        Args:
            frame (pd.DataFrame): DataFrame to filter.
            column (pd.Series): Column to apply the filter operation to.
            value: Value to filter the column by.

        Returns:
            pd.DataFrame: Filtered dataframe.
        """
        return frame.loc[column <= value]


class EQ(FilterOperation):
    """
    Equal to filter operation.
    """

    @staticmethod
    def apply(frame, column: pd.Series, value):
        """
        Apply the equal to filter operation to the dataframe.

        Args:
            frame (pd.DataFrame): DataFrame to filter.
            column (pd.Series): Column to apply the filter operation to.
            value: Value to filter the column by.

        Returns:
            pd.DataFrame: Filtered dataframe.
        """
        return frame.loc[column == value]


class NEQ(FilterOperation):
    """
    Not equal to filter operation.
    """

    @staticmethod
    def apply(frame, column: pd.Series, value):
        """
        Apply the not equal to filter operation to the dataframe.

        Args:
            frame (pd.DataFrame): DataFrame to filter.
            column (pd.Series): Column to apply the filter operation to.
            value: Value to filter the column by.

        Returns:
            pd.DataFrame: Filtered dataframe.
        """
        return frame.loc[column != value]


class IsIn(FilterOperation):
    """
    Is in filter operation.  Returns rows where value is in the provided list.
    """

    @staticmethod
    def apply(frame: pd.DataFrame, column: pd.Series, values: list):
        """
        Apply the is in filter operation to the dataframe.

        Args:
            frame (pd.DataFrame): DataFrame to filter.
            column (pd.Series): Column to apply the filter operation to.
            value: Value to filter the column by.

        Returns:
            pd.DataFrame: Filtered dataframe.
        """
        return frame.loc[column.isin(values)]


class StrContainsOneOf(FilterOperation):
    """
    String Contains One Of filter operation.  Returns rows where the value is a string containing one of the provided values.
    """

    @staticmethod
    def apply(frame: pd.DataFrame, column: pd.Series, values: list):
        """
        Apply the string contains one of filter operation to the dataframe.

        Args:
            frame (pd.DataFrame): DataFrame to filter.
            column (pd.Series): Column to apply the filter operation to.
            value: Value to filter the column by.

        Returns:
            pd.DataFrame: Filtered dataframe.
        """
        idx = np.array([False] * len(column))
        for v in values:
            idx = idx | (column.str.contains(v))
        return frame.loc[idx]


class Contains(FilterOperation):
    """
    Contains filter operation.  Returns rows where the value is a string containing the provided value.
    """

    @staticmethod
    def apply(frame: pd.DataFrame, column: pd.Series, value: str):
        """
        Apply the contains filter operation to the dataframe.

        Args:
            frame (pd.DataFrame): DataFrame to filter.
            column (pd.Series): Column to apply the filter operation to.
            value: Value to filter the column by.

        Returns:
            pd.DataFrame: Filtered dataframe.
        """
        return frame.loc[column.str.contains(value)]


class NotContains(FilterOperation):
    """
    Not Contains filter operation.  Returns rows where the value is a string not containing the provided value.
    """

    @staticmethod
    def apply(frame: pd.DataFrame, column: pd.Series, value: str):
        """
        Apply the not contains filter operation to the dataframe.

        Args:
            frame (pd.DataFrame): DataFrame to filter.
            column (pd.Series): Column to apply the filter operation to.
            value: Value to filter the column by.

        Returns:
            pd.DataFrame: Filtered dataframe.
        """
        return frame.loc[~column.str.contains(value)]


class Filter:
    """
    Defines a basic filtering operation.

    Attributes:
        attribute (DataAttribute): The attribute to filter on.
        value: The value to filter on.
        operation (FilterOperation): The filtering operation to perform. Filtering operations inherit from `FilterOperation`


    """

    def __init__(self, attribute: DataAttribute, value, operation: FilterOperation):
        self._attribute = attribute
        self._value = value
        self._operation = operation

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        return self._operation.apply(df, df[self._attribute.N], self._value)
