from typing import Any, List

from footmav.data_definitions.function_builder import (
    FunctionBuilder,
    DataAttributeOperator,
)
import pandas as pd


class UnaryMixin:
    """
    Mixin class for unary operators.  Defines the __new__ function that allows you to chain operators
    by using F.Sum(DATA) calls, for example
    """

    def __new__(cls, operand: FunctionBuilder):
        return FunctionBuilder(cls, operand)


class Sum(DataAttributeOperator, UnaryMixin):
    """
    Sum operator.
    """

    @classmethod
    def _apply(cls, operand_list: List[pd.Series]) -> str:
        return operand_list[0].sum()


class Lit(DataAttributeOperator, UnaryMixin):
    """
    Literal operator.
    """

    @classmethod
    def _apply(cls, operand_list: List[pd.Series]) -> Any:
        return operand_list[0]


class Col(UnaryMixin):
    """Column operator"""

    @classmethod
    def _apply(cls, data: pd.DataFrame, operand):
        return data[operand.N]
