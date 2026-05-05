"""_Float specific tests - float method proxying"""
import pytest
from serif import Vector, _Float


class TestFloatCreation:
    """Test _Float automatic creation"""
    
    def test_auto_creates_Float(self):
        v = Vector([1.5, 2.5, 3.5])
        assert isinstance(v, _Float)
        assert v.schema().kind == float
    
    def test_mixed_int_float_creates_Float(self):
        v = Vector([1, 2.5, 3])
        assert isinstance(v, _Float)
        assert v.schema().kind == float
        assert list(v) == [1.0, 2.5, 3.0]


class TestFloatMethods:
    """Test float method proxying"""
    
    def test_as_integer_ratio(self):
        v = Vector([0.5, 0.25, 0.75])
        result = v.as_integer_ratio()
        assert isinstance(result, Vector)
        assert len(result) == 3
        # 0.5 = 1/2, 0.25 = 1/4, 0.75 = 3/4
        assert result[0] == (1, 2)
    
    def test_is_integer(self):
        v = Vector([1.0, 2.5, 3.0])
        result = v.is_integer()
        assert isinstance(result, Vector)
        assert list(result) == [True, False, True]
    
    def test_hex(self):
        v = Vector([1.5, 2.0])
        result = v.hex()
        assert isinstance(result, Vector)
        assert len(result) == 2
        # Just check they're strings
        for x in result:
            assert isinstance(x, str)


class TestFloatOperations:
    """Test operations on float vectors"""
    
    def test_division_preserves_float(self):
        v = Vector([1.0, 2.0, 3.0])
        result = v / 2
        assert isinstance(result, Vector)
        assert result.schema().kind == float
        assert list(result) == [0.5, 1.0, 1.5]
    
    def test_comparison_with_floats(self):
        v = Vector([1.5, 2.5, 3.5])
        result = v > 2.0
        assert list(result) == [False, True, True]




