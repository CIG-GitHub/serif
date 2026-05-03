"""Basic Vector operations - creation, length, iteration, copy"""
import pytest
from serif import Vector


class TestCreation:
    """Test basic vector creation"""
    
    @pytest.mark.parametrize("initial,expected_len,expected_dtype", [
        ([1, 2, 3], 3, 'int64'),
        ([1.5, 2.5, 3.5], 3, 'float64'),
        (['a', 'b', 'c'], 3, str),
        ([], 0, None),
        ([1], 1, 'int64'),
    ])
    def test_creation_from_list(self, initial, expected_len, expected_dtype):
        v = Vector(initial)
        assert len(v) == expected_len
        if expected_dtype is None:
            assert v.schema() is None
        else:
            assert v.schema().kind == expected_dtype
        assert list(v) == initial
    
    def test_creation_empty(self):
        v = Vector()
        assert len(v) == 0
    
    def test_creation_with_name(self):
        v = Vector([1, 2, 3], name='test_vector')
        assert v._name == 'test_vector'
    
    def test_creation_typesafe(self):
        v = Vector([1, 2, 3], dtype=int)
        assert not v.schema().nullable
        assert v.schema().kind == 'int64'


class TestIteration:
    """Test iteration and access"""
    
    def test_iteration(self):
        v = Vector([1, 2, 3, 4, 5])
        result = []
        for x in v:
            result.append(x)
        assert result == [1, 2, 3, 4, 5]
    
    def test_list_conversion(self):
        v = Vector([1, 2, 3])
        assert list(v) == [1, 2, 3]
    
    def test_len(self):
        assert len(Vector([1, 2, 3])) == 3
        assert len(Vector([])) == 0


class TestCopy:
    """Test copy behavior"""
    
    def test_copy_basic(self):
        v1 = Vector([1, 2, 3])
        v2 = v1.copy()
        assert list(v1) == list(v2)
        assert v1 is not v2
        assert v1._underlying is not v2._underlying
    
    def test_copy_mutation_independence(self):
        v1 = Vector([1, 2, 3])
        v2 = v1.copy()
        v2[0] = 999
        assert v1[0] == 1
        assert v2[0] == 999
    
    def test_copy_with_new_values(self):
        v1 = Vector([1, 2, 3])
        v2 = v1.copy(new_values=[10, 20, 30])
        assert list(v2) == [10, 20, 30]
        assert v1.schema().kind == v2.schema().kind
    
    def test_copy_with_name(self):
        v1 = Vector([1, 2, 3], name='original')
        v2 = v1.copy(name='copy')
        assert v2._name == 'copy'


class TestTypePromotion:
    """Test automatic type promotion"""
    
    def test_int_to_float_promotion(self):
        v = Vector([1, 2, 3.5])
        assert v.schema().kind == 'float64'
        assert list(v) == [1.0, 2.0, 3.5]
    
    def test_no_promotion_with_dtype_int(self):
        # If dtype=int specified, should not promote to float
        v = Vector([1, 2, 3], dtype=int)
        assert v.schema().kind == 'int64'


class TestBooleanBehavior:
    """Test that vectors cannot be used in boolean context"""
    
    def test_empty_vector_len_check(self):
        v = Vector([])
        assert len(v) == 0
    
    def test_nonempty_vector_len_check(self):
        v = Vector([1])
        assert len(v) > 0
        
    def test_nonempty_typed_vector_len_check(self):
        v = Vector([0], dtype=int, typesafe=True)
        assert len(v) > 0  # Even with 0, the vector itself has length


class TestFingerprint:
    """Test fingerprint computation"""
    
    def test_fingerprint_exists(self):
        v = Vector([1, 2, 3])
        fp = v.fingerprint()
        assert isinstance(fp, int)
    
    def test_fingerprint_changes_on_mutation(self):
        v = Vector([1, 2, 3])
        fp1 = v.fingerprint()
        v[0] = 999
        fp2 = v.fingerprint()
        assert fp1 != fp2
    
    def test_fingerprint_same_for_equal_vectors(self):
        v1 = Vector([1, 2, 3])
        v2 = Vector([1, 2, 3])
        # Note: fingerprints should be same for same data
        assert v1.fingerprint() == v2.fingerprint()


class TestFillNA:
    """Test fillna behavior"""
    
    def test_fillna_basic(self):
        v = Vector([1, None, 3])
        result = v.fillna(0)
        assert list(result) == [1, 0, 3]
        assert not result.schema().nullable
    
    def test_fillna_promotes_int_to_float(self):
        """Test that filling int? with float promotes to float"""
        v = Vector([10, None, 20])
        result = v.fillna(5.4)
        assert list(result) == [10.0, 5.4, 20.0]
        assert result.schema().kind == 'float64'
        assert not result.schema().nullable
    
    def test_fillna_no_promotion_when_compatible(self):
        """Test that filling with compatible type doesn't promote"""
        v = Vector([1.0, None, 3.0])
        result = v.fillna(2.5)
        assert list(result) == [1.0, 2.5, 3.0]
        assert result.schema().kind == 'float64'
        assert not result.schema().nullable
    
    def test_fillna_preserves_name(self):
        v = Vector([1, None, 3], name='test')
        result = v.fillna(0)
        assert result._name == 'test'


class TestCast:
    """Test cast behavior"""
    
    def test_cast_preserves_none(self):
        """Test that None values are preserved, not converted to 'None' string"""
        v = Vector([10, None, 20])
        result = v.cast(str)
        assert list(result) == ['10', None, '20']
        assert result.schema().kind == str
        assert result.schema().nullable
    
    def test_cast_nullable_to_nullable(self):
        """Test that casting nullable input produces nullable output"""
        v = Vector([1.5, None, 2.5])
        result = v.cast(int)
        assert list(result) == [1, None, 2]
        assert result.schema().kind == 'int64'
        assert result.schema().nullable
    
    def test_cast_non_nullable_to_non_nullable(self):
        """Test that casting non-nullable input produces non-nullable output"""
        v = Vector([1.5, 2.5, 3.5])
        result = v.cast(int)
        assert list(result) == [1, 2, 3]
        assert result.schema().kind == 'int64'
        assert not result.schema().nullable




