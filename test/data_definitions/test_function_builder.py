from unittest.mock import MagicMock, patch, sentinel
import pytest


class TestAdd:
    def test_apply(self):
        from footmav.data_definitions.function_builder import Add

        operand_1 = 1
        operand_2 = 2
        assert Add._apply([operand_1, operand_2]) == 3


class TestSubtract:
    def test_apply(self):
        from footmav.data_definitions.function_builder import Subtract

        operand_1 = 1
        operand_2 = 2
        assert Subtract._apply([operand_1, operand_2]) == -1


class TestMultiply:
    def test_apply(self):
        from footmav.data_definitions.function_builder import Multiply

        operand_1 = 1
        operand_2 = 2
        assert Multiply._apply([operand_1, operand_2]) == 2


class TestDivide:
    def test_apply(self):
        from footmav.data_definitions.function_builder import Divide

        operand_1 = 1
        operand_2 = 2
        assert Divide._apply([operand_1, operand_2]) == 0.5


class TestFunctionBuilder:
    @pytest.fixture
    def get_function_builder(self):
        from footmav.data_definitions.function_builder import FunctionBuilder

        operator = MagicMock(_apply=MagicMock(return_value=sentinel.f))
        operand1 = MagicMock(_apply=MagicMock(return_value=sentinel.f1))
        operand2 = MagicMock(_appy=MagicMock(return_value=sentinel.f2))
        return FunctionBuilder(operator, operand1, operand2)

    @patch(
        "footmav.data_definitions.function_builder.FunctionBuilder._apply",
        return_value=sentinel.f,
    )
    def test_apply(self, function_builder_apply, get_function_builder):

        function_builder = get_function_builder
        df = MagicMock()
        result = function_builder.apply(df)
        assert result == sentinel.f
        function_builder_apply.assert_called_once_with(df)

    def test_inner_apply_col(self):
        from footmav.data_definitions.function_builder import FunctionBuilder

        operator = MagicMock(__name__="Col")
        operator._apply = MagicMock(return_value=sentinel.f)
        operand1 = MagicMock()
        data = MagicMock()
        fb = FunctionBuilder(operator, operand1)
        result = fb._apply(data)
        assert result == sentinel.f
        operator._apply.assert_called_once_with(data, operand1)

    def test_inner_apply(self):
        from footmav.data_definitions.function_builder import FunctionBuilder

        operator = MagicMock(
            _apply=MagicMock(return_value=sentinel.f), __name__="operator"
        )
        operand1 = MagicMock(_apply=MagicMock(return_value=sentinel.f1))
        operand2 = MagicMock(_apply=MagicMock(return_value=sentinel.f2))
        fb = FunctionBuilder(operator, operand1, operand2)
        result = fb._apply(sentinel.data)
        assert result == sentinel.f
        operand1._apply.assert_called_once_with(sentinel.data)
        operand2._apply.assert_called_once_with(sentinel.data)
        operator._apply.assert_called_once_with([sentinel.f1, sentinel.f2])

    def test_inner_apply_not_function_operands(self):
        from footmav.data_definitions.function_builder import FunctionBuilder

        operator = MagicMock(
            _apply=MagicMock(return_value=sentinel.f), __name__="operator"
        )
        operand1 = sentinel.f1
        operand2 = sentinel.f2

        fb = FunctionBuilder(operator, operand1, operand2)
        result = fb._apply(sentinel.data)
        assert result == sentinel.f
        operator._apply.assert_called_once_with([sentinel.f1, sentinel.f2])

    def test_add(self):
        from footmav.data_definitions.function_builder import FunctionBuilder
        from footmav.data_definitions.function_builder import Add

        operator1 = MagicMock()
        operator2 = MagicMock()
        fb1 = FunctionBuilder(operator1, MagicMock())
        fb2 = FunctionBuilder(operator2, MagicMock())

        result = fb1 + fb2
        assert result._operator == Add
        assert result._operands == (fb1, fb2)

    def test_subtract(self):
        from footmav.data_definitions.function_builder import FunctionBuilder
        from footmav.data_definitions.function_builder import Subtract

        operator1 = MagicMock()
        operator2 = MagicMock()
        fb1 = FunctionBuilder(operator1, MagicMock())
        fb2 = FunctionBuilder(operator2, MagicMock())

        result = fb1 - fb2
        assert result._operator == Subtract
        assert result._operands == (fb1, fb2)

    def test_multiply(self):
        from footmav.data_definitions.function_builder import FunctionBuilder
        from footmav.data_definitions.function_builder import Multiply

        operator1 = MagicMock()
        operator2 = MagicMock()
        fb1 = FunctionBuilder(operator1, MagicMock())
        fb2 = FunctionBuilder(operator2, MagicMock())

        result = fb1 * fb2
        assert result._operator == Multiply
        assert result._operands == (fb1, fb2)

    def test_divide(self):
        from footmav.data_definitions.function_builder import FunctionBuilder
        from footmav.data_definitions.function_builder import Divide

        operator1 = MagicMock()
        operator2 = MagicMock()
        fb1 = FunctionBuilder(operator1, MagicMock())
        fb2 = FunctionBuilder(operator2, MagicMock())

        result = fb1 / fb2
        assert result._operator == Divide
        assert result._operands == (fb1, fb2)
