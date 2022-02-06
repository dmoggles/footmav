from typing import Any, List
from footmav.data_definitions.base import DataAttribute

from footmav.data_definitions.function_builder import FunctionBuilder
import pandas as pd


def attribute_function_operator(f):
    def _wrapper(*args) -> FunctionBuilder:
        class _F:
            @classmethod
            def _apply(cls, operand_list: List[pd.Series]):
                return f(operand_list)

        return FunctionBuilder(_F, *args)

    return _wrapper


@attribute_function_operator
def Lit(operand_list: List[pd.Series]) -> Any:
    """
    Literal operator.
    """
    return operand_list[0]


@attribute_function_operator
def Sum(operand_list: List[pd.Series]) -> pd.Series:
    """
    Sum operator.
    """
    return operand_list[0].sum()


def Col(attr: DataAttribute) -> FunctionBuilder:
    """
    Column operator.
    """

    class Col:
        @classmethod
        def _apply(cls, data: pd.DataFrame, operand):
            return data[operand.N]

    return FunctionBuilder(Col, attr)
