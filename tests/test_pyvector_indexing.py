"""Vector indexing - single index, slicing, boolean masks, integer vectors"""
import pytest
from serif import Vector


class TestSingleIndexing:
    """Test single integer index access"""
    
    @pytest.mark.parametrize("index,expected", [
        (0, 10),
        (1, 20),
        (2, 30),
        (-1, 50),
        (-2, 40),
    ])
    def test_getitem_single(self, index, expected):
        v = Vector([10, 20, 30, 40, 50])
        assert v[index] == expected
    
    def test_setitem_single(self):
        v = Vector([1, 2, 3, 4, 5])
        v[2] = 999
        assert list(v) == [1, 2, 999, 4, 5]
    
    def test_setitem_negative_index(self):
        v = Vector([1, 2, 3, 4, 5])
        v[-1] = 999
        assert list(v) == [1, 2, 3, 4, 999]


class TestSlicing:
    """Test slice operations"""
    
    def test_getitem_slice(self):
        v = Vector([10, 20, 30, 40, 50])
        result = v[1:4]
        assert list(result) == [20, 30, 40]
        assert isinstance(result, Vector)
    
    def test_getitem_slice_with_step(self):
        v = Vector([10, 20, 30, 40, 50])
        result = v[::2]
        assert list(result) == [10, 30, 50]
    
    def test_setitem_slice(self):
        v = Vector([1, 2, 3, 4, 5])
        v[1:4] = [20, 30, 40]
        assert list(v) == [1, 20, 30, 40, 5]
    
    def test_setitem_slice_single_value(self):
        v = Vector([1, 2, 3, 4, 5])
        v[1:4] = 0
        assert list(v) == [1, 0, 0, 0, 5]
    
    def test_slice_preserves_name(self):
        v = Vector([1, 2, 3, 4, 5], name='test')
        result = v[1:4]
        assert result._name == 'test'


class TestBooleanMasking:
    """Test boolean mask indexing"""
    
    def test_getitem_boolean_mask(self):
        v = Vector([10, 20, 30, 40, 50])
        mask = Vector([True, False, True, False, True], dtype=bool, typesafe=True)
        result = v[mask]
        assert list(result) == [10, 30, 50]
    
    def test_getitem_boolean_list(self):
        v = Vector([10, 20, 30, 40, 50])
        mask = [True, False, True, False, True]
        result = v[mask]
        assert list(result) == [10, 30, 50]
    
    def test_setitem_boolean_mask_single_value(self):
        v = Vector([1, 2, 3, 4, 5])
        mask = [True, False, True, False, True]
        v[mask] = 999
        assert list(v) == [999, 2, 999, 4, 999]
    
    def test_setitem_boolean_mask_multiple_values(self):
        v = Vector([1, 2, 3, 4, 5])
        mask = [True, False, True, False, True]
        v[mask] = [10, 30, 50]
        assert list(v) == [10, 2, 30, 4, 50]
    
    def test_comparison_creates_boolean_mask(self):
        v = Vector([1, 2, 3, 4, 5])
        mask = v > 3
        assert mask.schema().kind == 'uint8'
        assert not mask.schema().nullable
        assert list(mask) == [False, False, False, True, True]
    
    def test_filter_with_comparison(self):
        v = Vector([1, 2, 3, 4, 5])
        result = v[v > 3]
        assert list(result) == [4, 5]


class TestIntegerVectorIndexing:
    """Test indexing with integer vectors"""
    
    def test_getitem_integer_vector(self):
        v = Vector([10, 20, 30, 40, 50])
        indices = Vector([0, 2, 4], dtype=int, typesafe=True)
        result = v[indices]
        assert list(result) == [10, 30, 50]
    
    def test_getitem_integer_list(self):
        v = Vector([10, 20, 30, 40, 50])
        indices = [0, 2, 4]
        result = v[indices]
        assert list(result) == [10, 30, 50]
    
    def test_setitem_integer_vector_single_value(self):
        v = Vector([1, 2, 3, 4, 5])
        indices = Vector([0, 2, 4], dtype=int, typesafe=True)
        v[indices] = 999
        assert list(v) == [999, 2, 999, 4, 999]
    
    def test_setitem_integer_vector_multiple_values(self):
        v = Vector([1, 2, 3, 4, 5])
        indices = Vector([0, 2, 4], dtype=int, typesafe=True)
        v[indices] = [10, 30, 50]
        assert list(v) == [10, 2, 30, 4, 50]


class TestMutationUpdatesFingerprint:
    """Test that all mutation types update fingerprint"""
    
    def test_single_index_updates_fingerprint(self):
        v = Vector([1, 2, 3])
        fp1 = v.fingerprint()
        v[0] = 999
        fp2 = v.fingerprint()
        assert fp1 != fp2
    
    def test_slice_updates_fingerprint(self):
        v = Vector([1, 2, 3, 4, 5])
        fp1 = v.fingerprint()
        v[1:4] = [20, 30, 40]
        fp2 = v.fingerprint()
        assert fp1 != fp2
    
    def test_boolean_mask_updates_fingerprint(self):
        v = Vector([1, 2, 3, 4, 5])
        fp1 = v.fingerprint()
        v[v > 3] = 999
        fp2 = v.fingerprint()
        assert fp1 != fp2



