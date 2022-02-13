from typing import Any
from footmav.data_definitions.base import DataAttribute

from footmav.data_definitions.function_builder import FunctionBuilder
import pandas as pd
from functools import wraps


def attribute_function_operator(f):
    def _wrapper(*args) -> FunctionBuilder:
        class _F:
            @wraps(f)
            @classmethod
            def _apply(cls, *args):
                return f(*args)

        return FunctionBuilder(_F, *args)

    return _wrapper


@attribute_function_operator
def Lit(operand: pd.Series) -> Any:
    """
    Literal operator.
    """
    return operand


@attribute_function_operator
def Sum(operand: pd.Series) -> pd.Series:
    """
    Sum operator.
    """
    return operand.sum()


def Col(attr: DataAttribute) -> FunctionBuilder:
    """
    Column operator.
    """

    class Col:
        @classmethod
        def _apply(cls, data: pd.DataFrame, operand):
            return data[operand.N]

    return FunctionBuilder(Col, attr)


@attribute_function_operator
def If(
    conditional: pd.Series, true_values: pd.Series, false_values: pd.Series
) -> pd.Series:
    """
    If operator.
    """
    return true_values.where(conditional, false_values)
