"""Vector math operations - addition, multiplication, division, etc."""
import pytest
from serif import Vector


class TestAddition:
    """Test addition operations"""
    
    @pytest.mark.parametrize("v1_data,v2_data,expected", [
        ([1, 2, 3], [10, 20, 30], [11, 22, 33]),
        ([1.5, 2.5], [0.5, 0.5], [2.0, 3.0]),
        ([0, 0, 0], [1, 2, 3], [1, 2, 3]),
    ])
    def test_add_vectors(self, v1_data, v2_data, expected):
        v1 = Vector(v1_data)
        v2 = Vector(v2_data)
        result = v1 + v2
        assert list(result) == expected
    
    def test_add_scalar(self):
        v = Vector([1, 2, 3])
        result = v + 10
        assert list(result) == [11, 12, 13]
    
    def test_radd_scalar(self):
        v = Vector([1, 2, 3])
        result = 10 + v
        assert list(result) == [11, 12, 13]
    
    def test_add_string_vectors(self):
        v1 = Vector(['hello', 'world'])
        v2 = Vector([' there', '!'])
        result = v1 + v2
        assert list(result) == ['hello there', 'world!']
    
    def test_radd_string(self):
        v = Vector(['world', 'python'])
        result = 'hello ' + v
        assert list(result) == ['hello world', 'hello python']
    
    def test_radd_zero_with_none(self):
        """Test that 0 + v works with None values (important for sum())"""
        v = Vector([10, None, 20])
        result = 0 + v
        assert list(result) == [10, None, 20]


class TestSubtraction:
    """Test subtraction operations"""
    
    def test_subtract_vectors(self):
        v1 = Vector([10, 20, 30])
        v2 = Vector([1, 2, 3])
        result = v1 - v2
        assert list(result) == [9, 18, 27]
    
    def test_subtract_scalar(self):
        v = Vector([10, 20, 30])
        result = v - 5
        assert list(result) == [5, 15, 25]
    
    def test_rsubtract_scalar(self):
        v = Vector([1, 2, 3])
        result = 10 - v
        assert list(result) == [9, 8, 7]


class TestMultiplication:
    """Test multiplication operations"""
    
    def test_multiply_vectors(self):
        v1 = Vector([1, 2, 3])
        v2 = Vector([10, 20, 30])
        result = v1 * v2
        assert list(result) == [10, 40, 90]
    
    def test_multiply_scalar(self):
        v = Vector([1, 2, 3])
        result = v * 10
        assert list(result) == [10, 20, 30]
    
    def test_rmultiply_scalar(self):
        v = Vector([1, 2, 3])
        result = 10 * v
        assert list(result) == [10, 20, 30]
    
    def test_multiply_string_vector(self):
        v = Vector(['a', 'b', 'c'])
        result = v * 3
        assert list(result) == ['aaa', 'bbb', 'ccc']


class TestDivision:
    """Test division operations"""
    
    def test_truediv_vectors(self):
        v1 = Vector([10, 20, 30])
        v2 = Vector([2, 4, 5])
        result = v1 / v2
        assert list(result) == [5.0, 5.0, 6.0]
    
    def test_truediv_scalar(self):
        v = Vector([10, 20, 30])
        result = v / 2
        assert list(result) == [5.0, 10.0, 15.0]
    
    def test_floordiv_vectors(self):
        v1 = Vector([10, 21, 30])
        v2 = Vector([3, 4, 7])
        result = v1 // v2
        assert list(result) == [3, 5, 4]
    
    def test_floordiv_scalar(self):
        v = Vector([10, 21, 30])
        result = v // 3
        assert list(result) == [3, 7, 10]


class TestPower:
    """Test power operations"""
    
    def test_pow_vectors(self):
        v1 = Vector([2, 3, 4])
        v2 = Vector([2, 2, 2])
        result = v1 ** v2
        assert list(result) == [4, 9, 16]
    
    def test_pow_scalar(self):
        v = Vector([2, 3, 4])
        result = v ** 2
        assert list(result) == [4, 9, 16]
    
    def test_rpow_scalar(self):
        v = Vector([1, 2, 3])
        result = 2 ** v
        assert list(result) == [2, 4, 8]


class TestModulo:
    """Test modulo operations"""
    
    def test_mod_vectors(self):
        v1 = Vector([10, 21, 30])
        v2 = Vector([3, 4, 7])
        result = v1 % v2
        assert list(result) == [1, 1, 2]
    
    def test_mod_scalar(self):
        v = Vector([10, 21, 30])
        result = v % 3
        assert list(result) == [1, 0, 0]


class TestComparison:
    """Test comparison operations"""
    
    def test_equality(self):
        v = Vector([1, 2, 3, 4, 5])
        result = v == 3
        assert list(result) == [False, False, True, False, False]
    
    def test_greater_than(self):
        v = Vector([1, 2, 3, 4, 5])
        result = v > 3
        assert list(result) == [False, False, False, True, True]
    
    def test_less_than(self):
        v = Vector([1, 2, 3, 4, 5])
        result = v < 3
        assert list(result) == [True, True, False, False, False]
    
    def test_greater_equal(self):
        v = Vector([1, 2, 3, 4, 5])
        result = v >= 3
        assert list(result) == [False, False, True, True, True]
    
    def test_less_equal(self):
        v = Vector([1, 2, 3, 4, 5])
        result = v <= 3
        assert list(result) == [True, True, True, False, False]
    
    def test_not_equal(self):
        v = Vector([1, 2, 3, 4, 5])
        result = v != 3
        assert list(result) == [True, True, False, True, True]


class TestAggregation:
    """Test aggregation methods"""
    
    def test_sum(self):
        v = Vector([1, 2, 3, 4, 5])
        assert v.sum() == 15
    
    def test_mean(self):
        v = Vector([1, 2, 3, 4, 5])
        assert v.mean() == 3.0
    
    def test_min(self):
        v = Vector([5, 2, 8, 1, 9])
        assert v.min() == 1
    
    def test_max(self):
        v = Vector([5, 2, 8, 1, 9])
        assert v.max() == 9
    
    def test_stdev(self):
        v = Vector([2, 4, 6, 8])
        stdev = v.stdev()
        assert stdev > 0  # Basic sanity check




