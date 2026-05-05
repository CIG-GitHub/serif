"""Test column name preservation in table arithmetic operations"""
import pytest
import warnings
from serif import Vector, Table


class TestTableScalarArithmetic:
    """Test Table + scalar preserves column names"""
    
    def test_table_add_scalar_preserves_names(self):
        t = Table({'a': [1, 2, 3], 'b': [4, 5, 6]})
        result = t + 10
        assert result.column_names() == ['a', 'b']
        assert list(result.a) == [11, 12, 13]
        assert list(result.b) == [14, 15, 16]
    
    def test_table_sub_scalar_preserves_names(self):
        t = Table({'price': [100, 200], 'cost': [50, 75]})
        result = t - 10
        assert result.column_names() == ['price', 'cost']
        assert list(result.price) == [90, 190]
        assert list(result.cost) == [40, 65]
    
    def test_table_mul_scalar_preserves_names(self):
        t = Table({'x': [1, 2], 'y': [3, 4]})
        result = t * 2
        assert result.column_names() == ['x', 'y']
        assert list(result.x) == [2, 4]
        assert list(result.y) == [6, 8]
    
    def test_table_div_scalar_preserves_names(self):
        t = Table({'a': [10, 20], 'b': [30, 40]})
        result = t / 2
        assert result.column_names() == ['a', 'b']
        assert list(result.a) == [5.0, 10.0]
        assert list(result.b) == [15.0, 20.0]
    
    def test_table_floordiv_scalar_preserves_names(self):
        t = Table({'x': [10, 21], 'y': [30, 45]})
        result = t // 3
        assert result.column_names() == ['x', 'y']
        assert list(result.x) == [3, 7]
        assert list(result.y) == [10, 15]
    
    def test_table_mod_scalar_preserves_names(self):
        t = Table({'a': [10, 21], 'b': [30, 45]})
        result = t % 7
        assert result.column_names() == ['a', 'b']
        assert list(result.a) == [3, 0]
        assert list(result.b) == [2, 3]
    
    def test_table_pow_scalar_preserves_names(self):
        t = Table({'x': [2, 3], 'y': [4, 5]})
        result = t ** 2
        assert result.column_names() == ['x', 'y']
        assert list(result.x) == [4, 9]
        assert list(result.y) == [16, 25]
    
    def test_unnamed_columns_stay_unnamed(self):
        """Unnamed columns should remain unnamed after scalar operations"""
        v1 = Vector([1, 2, 3])
        v2 = Vector([4, 5, 6])
        t = Table([v1, v2])
        result = t + 5
        assert result.column_names() == [None, None]


class TestTableTableArithmetic:
    """Test Table + Table with left-biased naming"""
    
    def test_table_add_table_matching_names(self):
        """Matching names on both sides: keep left name, no warning"""
        t1 = Table({'a': [1, 2], 'b': [3, 4]})
        t2 = Table({'a': [10, 20], 'b': [30, 40]})
        
        with warnings.catch_warnings():
            warnings.simplefilter("error")  # Turn warnings into errors
            result = t1 + t2
        
        assert result.column_names() == ['a', 'b']
        assert list(result.a) == [11, 22]
        assert list(result.b) == [33, 44]
    
    def test_table_add_table_mismatched_names_warns(self):
        """Mismatched names: drop to None, emit warning"""
        t1 = Table({'price': [100, 200]})
        t2 = Table({'cost': [10, 20]})
        
        with pytest.warns(UserWarning, match="unusual column naming"):
            result = t1 + t2
        
        # Column name should be dropped (None)
        assert result.column_names() == [None]
        assert list(result.cols()[0]) == [110, 220]
    
    def test_table_add_table_left_unnamed_right_named(self):
        """Left unnamed, right named: keep None (left-biased), warn"""
        v1 = Vector([1, 2])
        t1 = Table([v1])  # Unnamed column
        t2 = Table({'price': [10, 20]})
        
        with pytest.warns(UserWarning, match="left=None right='price'"):
            result = t1 + t2
        
        # Left-biased: keep left's None
        assert result.column_names() == [None]
    
    def test_table_add_table_left_named_right_unnamed(self):
        """Left named, right unnamed: keep left name, no warning"""
        t1 = Table({'price': [100, 200]})
        v2 = Vector([10, 20])
        t2 = Table([v2])  # Unnamed column
        
        with warnings.catch_warnings():
            warnings.simplefilter("error")  # Should not warn
            result = t1 + t2
        
        # Left-biased: keep left's name
        assert result.column_names() == ['price']
        assert list(result.price) == [110, 220]
    
    def test_table_add_table_multiple_columns_mixed(self):
        """Multiple columns with mixed naming scenarios"""
        t1 = Table({'a': [1, 2], 'b': [3, 4], 'c': [5, 6]})
        t2 = Table({'a': [10, 20], 'x': [30, 40], 'c': [50, 60]})
        
        with pytest.warns(UserWarning, match="unusual column naming"):
            result = t1 + t2
        
        # 'a' matches -> keep 'a'
        # 'b' vs 'x' mismatch -> drop to None
        # 'c' matches -> keep 'c'
        assert result.column_names() == ['a', None, 'c']
        assert list(result.a) == [11, 22]
        assert list(result.cols()[1]) == [33, 44]
        assert list(result.c) == [55, 66]
    
    def test_table_sub_table_naming(self):
        """Test subtraction preserves naming rules"""
        t1 = Table({'x': [10, 20]})
        t2 = Table({'y': [1, 2]})
        
        with pytest.warns(UserWarning, match="unusual column naming"):
            result = t1 - t2
        
        assert result.column_names() == [None]
        assert list(result.cols()[0]) == [9, 18]
    
    def test_table_mul_table_naming(self):
        """Test multiplication preserves naming rules"""
        t1 = Table({'a': [2, 3]})
        t2 = Table({'a': [4, 5]})
        
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            result = t1 * t2
        
        assert result.column_names() == ['a']
        assert list(result.a) == [8, 15]
    
    def test_table_div_table_naming(self):
        """Test division preserves naming rules"""
        t1 = Table({'x': [10, 20]})
        t2 = Table({'x': [2, 4]})
        
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            result = t1 / t2
        
        assert result.column_names() == ['x']
        assert list(result.x) == [5.0, 5.0]
    
    def test_warning_message_format(self):
        """Verify warning message format is helpful"""
        t1 = Table({'price': [100], 'qty': [5]})
        t2 = Table({'cost': [10], 'amount': [3]})
        
        with pytest.warns(UserWarning) as record:
            result = t1 + t2
        
        # Check warning contains expected information
        warning_msg = str(record[0].message)
        assert "Table operation (+)" in warning_msg
        assert "unusual column naming in 2 column(s)" in warning_msg
        assert "idx 0" in warning_msg
        assert "idx 1" in warning_msg
        assert "left='price' right='cost'" in warning_msg
        assert "left='qty' right='amount'" in warning_msg
    
    def test_table_width_mismatch_raises(self):
        """Table + Table with different widths should raise error"""
        t1 = Table({'a': [1, 2]})
        t2 = Table({'x': [10, 20], 'y': [30, 40]})
        
        with pytest.raises(ValueError, match="Table width mismatch"):
            result = t1 + t2


class TestTableArithmeticEdgeCases:
    """Edge cases and special scenarios"""
    
    def test_empty_table_operations(self):
        """Operations on empty tables should work"""
        t = Table({})
        result = t + 10
        assert len(result.cols()) == 0
    
    def test_single_column_operations(self):
        """Single column tables should work correctly"""
        t1 = Table({'x': [1, 2, 3]})
        result = t1 * 10
        assert result.column_names() == ['x']
        assert list(result.x) == [10, 20, 30]
    
    def test_chained_operations_preserve_names(self):
        """Chaining operations should preserve names correctly"""
        t = Table({'a': [1, 2], 'b': [3, 4]})
        result = (t + 5) * 2
        assert result.column_names() == ['a', 'b']
        assert list(result.a) == [12, 14]
        assert list(result.b) == [16, 18]
    
    def test_both_tables_unnamed_no_warning(self):
        """Both tables with unnamed columns: keep None, no warning"""
        v1 = Vector([1, 2])
        v2 = Vector([10, 20])
        t1 = Table([v1])
        t2 = Table([v2])
        
        with warnings.catch_warnings():
            warnings.simplefilter("error")  # Should not warn
            result = t1 + t2
        
        assert result.column_names() == [None]
        assert list(result.cols()[0]) == [11, 22]

