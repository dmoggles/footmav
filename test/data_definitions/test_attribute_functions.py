from unittest.mock import patch, sentinel, MagicMock


class TestUnaryMixin:
    @patch(
        "footmav.data_definitions.attribute_functions.FunctionBuilder",
        return_value=sentinel.f,
    )
    def test_unary_mixin_new(self, function_builder):
        from footmav.data_definitions.attribute_functions import UnaryMixin

        operand = MagicMock()
        f = UnaryMixin(operand)
        assert f == sentinel.f
        function_builder.assert_called_once_with(UnaryMixin, operand)


class TestSum:
    def test_apply(self):
        from footmav.data_definitions.attribute_functions import Sum

        operand = MagicMock(sum=MagicMock(return_value=sentinel.sum))

        assert Sum._apply([operand]) == sentinel.sum
        operand.sum.assert_called_once_with()


class TestLit:
    def test_apply(self):
        from footmav.data_definitions.attribute_functions import Lit

        assert Lit._apply([sentinel.lit]) == sentinel.lit


class TestCol:
    def test_apply(self):
        from footmav.data_definitions.attribute_functions import Col

        data = MagicMock(__getitem__=MagicMock(return_value=sentinel.col))
        operand = MagicMock(N=sentinel.N)
        assert Col._apply(data, operand) == sentinel.col
        data.__getitem__.assert_called_once_with(sentinel.N)
