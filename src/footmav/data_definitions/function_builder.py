import abc
import pandas as pd
from typing import Type, List


class DataAttributeOperator(abc.ABC):
    """
    Abstract class for data attribute operators.
    """

    @classmethod
    @abc.abstractmethod
    def _apply(cls, operand_list: List[pd.Series]) -> pd.Series:
        """Abstract function for applying the opeartor to a list of operands.


        Args:
            operand_list (List[pd.Series]): List of operands.

        Returns:
            pd.Series: Result of applying the operator to the operands.
        """


class Add(DataAttributeOperator):
    """
    Addition operator.
    """

    @classmethod
    def _apply(cls, operand_list: List[pd.Series]) -> pd.Series:
        return operand_list[0] + operand_list[1]


class Subtract(DataAttributeOperator):
    """
    Subtraction operator.
    """

    @classmethod
    def _apply(cls, operand_list: List[pd.Series]) -> pd.Series:
        return operand_list[0] - operand_list[1]


class Multiply(DataAttributeOperator):
    """
    Multiplication operator.
    """

    @classmethod
    def _apply(cls, operand_list: List[pd.Series]) -> pd.Series:
        return operand_list[0] * operand_list[1]


class Divide(DataAttributeOperator):
    """
    Division operator.
    """

    @classmethod
    def _apply(cls, operand_list: List[pd.Series]) -> pd.Series:
        return operand_list[0] / operand_list[1]


class FunctionBuilder:
    """
    FunctionBuilder class.  The class that's used behind the scenes to build
    DataAttribute arithmetic that's used in user-defined columns.

    Realistically, there's no reason for the user to ever interact with this class directly.
    """

    def __init__(
        self,
        operator: Type,
        *operands,
    ):
        self._operator = operator
        self._operands = operands

    def apply(self, data: pd.DataFrame) -> pd.Series:
        """apply the function to a dataframe.
        Args:
            df (pandas.DataFrame): dataframe to apply the function to.

        Returns:
            pandas.Series: Series of the result of applying the function to the dataframe.

        """
        return self._apply(data)

    def _apply(self, df: pd.DataFrame) -> pd.Series:
        """
        Inner implementation of apply, used by recursion and not publicly visible.

        Args:
            df (pandas.DataFrame): dataframe to apply the function to.

        Returns:
            pandas.Series: Series of the result of applying the function to the dataframe.
        """
        if self._operator.__name__ == "Col":
            return self._operator._apply(df, self._operands[0])
        else:
            operand_series = [
                o._apply(df) if hasattr(o, "_apply") else o for o in self._operands
            ]
            return self._operator._apply(operand_series)

    def __add__(self, other) -> "FunctionBuilder":
        """Add operator to add FunctionBuilders to each other.

        Args:
            other (FunctionBuilder): rhs of the addition.

        Returns:
            FunctionBuilder: Result of adding the two FunctionBuilders.
        """
        return FunctionBuilder(Add, self, other)

    def __sub__(self, other) -> "FunctionBuilder":
        """Subtraction operator to subtract FunctionBuilders from each other.

        Args:
            other (FunctionBuilder): rhs of the subtraction.

        Returns:
            FunctionBuilder: Result of subtracting the two FunctionBuilders.

        """
        return FunctionBuilder(Subtract, self, other)

    def __mul__(self, other) -> "FunctionBuilder":
        """Multiplication operator to multiply FunctionBuilders by each other.

        Args:
            other (FunctionBuilder): rhs of the multiplication.

        Returns:
            FunctionBuilder: Result of multiplying the two FunctionBuilders.

        """
        return FunctionBuilder(Multiply, self, other)

    def __truediv__(self, other) -> "FunctionBuilder":
        """Division operator to divide FunctionBuilders by each other.

        Args:
            other (FunctionBuilder): rhs of the division.

        Returns:
            FunctionBuilder: Result of dividing the two FunctionBuilders.

        """

        return FunctionBuilder(Divide, self, other)
