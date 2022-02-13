from unittest.mock import sentinel, MagicMock


class TestWrapper:
    def test_repr(self):
        from footmav.data_definitions.attribute_functions import (
            attribute_function_operator,
        )

        @attribute_function_operator
        def g(x):
            return x

        v = g(sentinel.x)
        assert repr(v._operator) == "g"

    def test_name(self):
        from footmav.data_definitions.attribute_functions import (
            attribute_function_operator,
        )

        @attribute_function_operator
        def g(x):
            return x

        v = g(sentinel.x)
        assert v._operator.__name__() == "_F"


class TestSum:
    def test_apply(self):
        from footmav.data_definitions.attribute_functions import Sum

        operand = MagicMock(sum=MagicMock(return_value=sentinel.sum))
        sum = Sum(operand)
        assert sum._operator._apply(operand) == sentinel.sum
        operand.sum.assert_called_once_with()


class TestLit:
    def test_apply(self):
        from footmav.data_definitions.attribute_functions import Lit

        lit = Lit(sentinel.lit)
        assert lit._operator._apply(sentinel.lit) == sentinel.lit


class TestCol:
    def test_apply(self):
        from footmav.data_definitions.attribute_functions import Col

        data = MagicMock(__getitem__=MagicMock(return_value=sentinel.col))
        operand = MagicMock(N=sentinel.N)
        col = Col(operand)
        assert col._operator._apply(data, operand) == sentinel.col
        data.__getitem__.assert_called_once_with(sentinel.N)


class TestIf:
    def test_apply(self):
        from footmav.data_definitions.attribute_functions import If

        operand1 = MagicMock(N=sentinel.N)
        operand2 = MagicMock(N=sentinel.N, where=MagicMock(return_value=sentinel.where))
        operand3 = MagicMock(N=sentinel.N)
        if_ = If(operand1, operand2, operand3)
        assert if_._operator._apply(operand1, operand2, operand3) == sentinel.where
        operand2.where.assert_called_once_with(operand1, operand3)
