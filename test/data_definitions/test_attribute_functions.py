from unittest.mock import sentinel, MagicMock


class TestSum:
    def test_apply(self):
        from footmav.data_definitions.attribute_functions import Sum

        operand = MagicMock(sum=MagicMock(return_value=sentinel.sum))
        sum = Sum(operand)
        assert sum._operator._apply([operand]) == sentinel.sum
        operand.sum.assert_called_once_with()


class TestLit:
    def test_apply(self):
        from footmav.data_definitions.attribute_functions import Lit

        lit = Lit(sentinel.lit)
        assert lit._operator._apply([sentinel.lit]) == sentinel.lit


class TestCol:
    def test_apply(self):
        from footmav.data_definitions.attribute_functions import Col

        data = MagicMock(__getitem__=MagicMock(return_value=sentinel.col))
        operand = MagicMock(N=sentinel.N)
        col = Col(operand)
        assert col._operator._apply(data, operand) == sentinel.col
        data.__getitem__.assert_called_once_with(sentinel.N)
