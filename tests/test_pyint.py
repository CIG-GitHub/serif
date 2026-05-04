"""_Int specific tests - int method proxying"""
import pytest
from serif import Vector, _Int


class TestIntCreation:
    """Test _Int automatic creation"""
    
    def test_auto_creates_Int(self):
        v = Vector([1, 2, 3])
        assert isinstance(v, _Int)
        assert v.schema().kind == int
    
    def test_does_not_include_bool(self):
        # Bools should not create _Int
        v = Vector([True, False, True])
        assert not isinstance(v, _Int)


class TestIntMethods:
    """Test int method proxying"""
    
    def test_bit_length(self):
        v = Vector([1, 2, 4, 8, 16])
        result = v.bit_length()
        assert isinstance(result, Vector)
        assert list(result) == [1, 2, 3, 4, 5]
    
    def test_bit_count(self):
        v = Vector([0, 1, 3, 7, 15])  # 0, 1, 11, 111, 1111 in binary
        result = v.bit_count()
        assert isinstance(result, Vector)
        assert list(result) == [0, 1, 2, 3, 4]
    
    def test_to_bytes(self):
        v = Vector([1, 255, 256])
        result = v.to_bytes(2, 'big')
        assert isinstance(result, Vector)
        assert len(result) == 3
        # Just check they're bytes
        for x in result:
            assert isinstance(x, bytes)


class TestIntOperations:
    """Test operations on int vectors"""
    
    def test_floor_division(self):
        v = Vector([10, 21, 30])
        result = v // 3
        assert isinstance(result, Vector)
        assert v.schema().kind == int
        assert list(result) == [3, 7, 10]
    
    def test_modulo(self):
        v = Vector([10, 21, 30])
        result = v % 7
        assert list(result) == [3, 0, 2]
    
    def test_power_with_ints(self):
        v = Vector([2, 3, 4])
        result = v ** 2
        assert list(result) == [4, 9, 16]




