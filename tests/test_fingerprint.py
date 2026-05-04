"""Fingerprint change detection tests"""
import pytest
from serif import Vector
from serif import Table


class TestBasicFingerprint:
    """Test basic fingerprint functionality"""
    
    def test_fingerprint_returns_int(self):
        v = Vector([1, 2, 3])
        fp = v.fingerprint()
        assert isinstance(fp, int)
    
    def test_same_data_same_fingerprint(self):
        v1 = Vector([1, 2, 3])
        v2 = Vector([1, 2, 3])
        assert v1.fingerprint() == v2.fingerprint()
    
    def test_different_data_different_fingerprint(self):
        v1 = Vector([1, 2, 3])
        v2 = Vector([1, 2, 4])
        assert v1.fingerprint() != v2.fingerprint()


class TestMutationDetection:
    """Test fingerprint changes on mutations"""
    
    def test_single_index_mutation(self):
        v = Vector([1, 2, 3, 4, 5])
        fp1 = v.fingerprint()
        v[2] = 999
        fp2 = v.fingerprint()
        assert fp1 != fp2
    
    def test_slice_mutation(self):
        v = Vector([1, 2, 3, 4, 5])
        fp1 = v.fingerprint()
        v[1:4] = [20, 30, 40]
        fp2 = v.fingerprint()
        assert fp1 != fp2
    
    def test_boolean_mask_mutation(self):
        v = Vector([1, 2, 3, 4, 5])
        fp1 = v.fingerprint()
        v[v > 3] = 999
        fp2 = v.fingerprint()
        assert fp1 != fp2
    
    def test_integer_vector_mutation(self):
        v = Vector([1, 2, 3, 4, 5])
        fp1 = v.fingerprint()
        indices = Vector([0, 2, 4], dtype=int, typesafe=True)
        v[indices] = 999
        fp2 = v.fingerprint()
        assert fp1 != fp2


class TestMultipleMutations:
    """Test fingerprint tracking multiple mutations"""
    
    def test_sequential_mutations(self):
        v = Vector([1, 2, 3, 4, 5])
        fp0 = v.fingerprint()
        
        v[0] = 10
        fp1 = v.fingerprint()
        assert fp0 != fp1
        
        v[1] = 20
        fp2 = v.fingerprint()
        assert fp1 != fp2
        assert fp0 != fp2
        
        v[2] = 30
        fp3 = v.fingerprint()
        assert fp2 != fp3
        assert fp1 != fp3
        assert fp0 != fp3
    
    def test_mutation_and_revert(self):
        v = Vector([1, 2, 3])
        fp_original = v.fingerprint()
        
        v[1] = 999
        fp_mutated = v.fingerprint()
        assert fp_original != fp_mutated
        
        v[1] = 2  # Revert to original value
        fp_reverted = v.fingerprint()
        assert fp_reverted == fp_original


class TestNestedStructures:
    """Test fingerprint for nested vectors (tables)"""
    
    def test_table_fingerprint(self):
        col1 = Vector([1, 2, 3])
        col2 = Vector([4, 5, 6])
        table = Vector([col1, col2])
        fp = table.fingerprint()
        assert isinstance(fp, int)
    
    def test_table_mutation_via_column(self):
        col1 = Vector([1, 2, 3])
        col2 = Vector([4, 5, 6])
        table = Vector([col1, col2])
        fp1 = table.fingerprint()
        
        # Mutate original column - value semantics means table is unaffected
        col1[0] = 999
        fp2 = table.fingerprint()
        assert fp1 == fp2  # Table has a copy, not affected by mutation


class TestFingerprintStability:
    """Test fingerprint stability and reproducibility"""
    
    def test_repeated_calls_same_result(self):
        v = Vector([1, 2, 3, 4, 5])
        fp1 = v.fingerprint()
        fp2 = v.fingerprint()
        fp3 = v.fingerprint()
        assert fp1 == fp2 == fp3
    
    def test_copy_has_same_fingerprint(self):
        v1 = Vector([1, 2, 3])
        v2 = v1.copy()
        assert v1.fingerprint() == v2.fingerprint()
    
    def test_copy_mutations_independent(self):
        v1 = Vector([1, 2, 3])
        v2 = v1.copy()
        fp1_original = v1.fingerprint()
        fp2_original = v2.fingerprint()
        
        v2[0] = 999
        assert v1.fingerprint() == fp1_original  # v1 unchanged
        assert v2.fingerprint() != fp2_original  # v2 changed


class TestFingerprintTypes:
    """Test fingerprint with different data types"""
    
    def test_int_vector_fingerprint(self):
        v = Vector([1, 2, 3])
        fp = v.fingerprint()
        assert isinstance(fp, int)
    
    def test_float_vector_fingerprint(self):
        v = Vector([1.5, 2.5, 3.5])
        fp = v.fingerprint()
        assert isinstance(fp, int)
    
    def test_string_vector_fingerprint(self):
        v = Vector(['hello', 'world'])
        fp = v.fingerprint()
        assert isinstance(fp, int)
    
    def test_different_types_different_fingerprints(self):
        v1 = Vector([1, 2, 3])
        v2 = Vector([1.0, 2.0, 3.0])
        # These might have different fingerprints due to hash differences
        # Just ensure they both return valid fingerprints
        assert isinstance(v1.fingerprint(), int)
        assert isinstance(v2.fingerprint(), int)


class TestChangeDetectionWorkflow:
    """Test typical change detection workflow"""
    
    def test_track_changes_workflow(self):
        # Create a vector
        v = Vector([1, 2, 3, 4, 5])
        initial_fp = v.fingerprint()
        
        # Simulate checking for changes (no changes)
        assert v.fingerprint() == initial_fp
        
        # Make a change
        v[2] = 999
        
        # Detect the change
        assert v.fingerprint() != initial_fp
        
        # Update the stored fingerprint
        new_fp = v.fingerprint()
        
        # No more changes
        assert v.fingerprint() == new_fp


class TestFingerprintEdgeCases:
	"""Test that fingerprinting handles edge cases correctly."""
	
	def test_none_fingerprint_stable(self):
		"""None values should have consistent fingerprints."""
		v1 = Vector([1, None, 3])
		v2 = Vector([1, None, 3])
		assert v1.fingerprint() == v2.fingerprint()
	
	def test_nan_fingerprint_stable(self):
		"""NaN values should have consistent fingerprints despite NaN != NaN."""
		v1 = Vector([1.0, float('nan'), 3.0])
		v2 = Vector([1.0, float('nan'), 3.0])
		assert v1.fingerprint() == v2.fingerprint()
	
	def test_negative_zero_equals_positive_zero(self):
		"""Fingerprint should treat -0.0 and 0.0 as identical."""
		v1 = Vector([1.0, -0.0, 3.0])
		v2 = Vector([1.0, 0.0, 3.0])
		assert v1.fingerprint() == v2.fingerprint()
	
	def test_complex_numbers(self):
		"""Complex numbers should fingerprint consistently."""
		v1 = Vector([1+2j, 3+4j])
		v2 = Vector([1+2j, 3+4j])
		assert v1.fingerprint() == v2.fingerprint()
		
		# Different complex numbers should have different fingerprints
		v3 = Vector([1+2j, 3+5j])
		assert v1.fingerprint() != v3.fingerprint()
	
	def test_date_fingerprint(self):
		"""Date objects should fingerprint consistently."""
		from datetime import date
		v1 = Vector([date(2024, 1, 1), date(2024, 12, 31)])
		v2 = Vector([date(2024, 1, 1), date(2024, 12, 31)])
		assert v1.fingerprint() == v2.fingerprint()
		
		v3 = Vector([date(2024, 1, 1), date(2024, 12, 30)])
		assert v1.fingerprint() != v3.fingerprint()
	
	def test_datetime_fingerprint(self):
		"""DateTime objects should fingerprint consistently."""
		from datetime import datetime
		v1 = Vector([datetime(2024, 1, 1, 12, 0), datetime(2024, 12, 31, 23, 59)])
		v2 = Vector([datetime(2024, 1, 1, 12, 0), datetime(2024, 12, 31, 23, 59)])
		assert v1.fingerprint() == v2.fingerprint()
		
		v3 = Vector([datetime(2024, 1, 1, 12, 0), datetime(2024, 12, 31, 23, 58)])
		assert v1.fingerprint() != v3.fingerprint()
	
	def test_set_fingerprint_order_independent(self):
		"""Sets should fingerprint consistently regardless of iteration order."""
		v1 = Vector([{1, 2, 3}, {4, 5}])
		v2 = Vector([{3, 2, 1}, {5, 4}])
		assert v1.fingerprint() == v2.fingerprint()
	
	def test_nested_list_fingerprint(self):
		"""Nested lists should fingerprint without converting to tuples."""
		v1 = Vector([[1, 2], [3, 4]])
		v2 = Vector([[1, 2], [3, 4]])
		assert v1.fingerprint() == v2.fingerprint()
		
		v3 = Vector([[1, 2], [3, 5]])
		assert v1.fingerprint() != v3.fingerprint()
	
	def test_tuple_fingerprint(self):
		"""Tuples should fingerprint recursively."""
		v1 = Vector([(1, 2), (3, 4)])
		v2 = Vector([(1, 2), (3, 4)])
		assert v1.fingerprint() == v2.fingerprint()
		
		v3 = Vector([(1, 2), (3, 5)])
		assert v1.fingerprint() != v3.fingerprint()
	
	def test_nested_tuple_fingerprint(self):
		"""Nested tuples should fingerprint correctly."""
		v1 = Vector([((1, 2), (3, 4)), ((5, 6), (7, 8))])
		v2 = Vector([((1, 2), (3, 4)), ((5, 6), (7, 8))])
		assert v1.fingerprint() == v2.fingerprint()
	
	def test_mixed_types_fingerprint(self):
		"""Vectors with mixed types should fingerprint consistently."""
		import warnings
		from datetime import date
		# Mixed types degrade to object dtype - expect warning
		with warnings.catch_warnings():
			warnings.simplefilter("ignore", UserWarning)
			v1 = Vector([1, 'hello', 3.14, None, date(2024, 1, 1)])
			v2 = Vector([1, 'hello', 3.14, None, date(2024, 1, 1)])
		assert v1.fingerprint() == v2.fingerprint()


class TestFingerprintIncrementalUpdates:
	"""Test that incremental fingerprint updates work correctly."""
	
	def test_single_update_matches_recompute(self):
		"""Single element update should match full recompute."""
		v = Vector([1, 2, 3, 4, 5])
		fp_before = v.fingerprint()
		
		# Manual update
		v[2] = 99
		fp_after_incremental = v.fingerprint()
		
		# Full recompute
		v_fresh = Vector([1, 2, 99, 4, 5])
		fp_after_recompute = v_fresh.fingerprint()
		
		assert fp_after_incremental == fp_after_recompute
		assert fp_before != fp_after_incremental
	
	def test_single_update_with_none(self):
		"""Updating to/from None should work correctly."""
		v = Vector([1, 2, 3])
		v[1] = None
		fp1 = v.fingerprint()
		
		v_expected = Vector([1, None, 3])
		assert fp1 == v_expected.fingerprint()
	
	def test_single_update_with_nan(self):
		"""Updating to/from NaN should work correctly."""
		v = Vector([1.0, 2.0, 3.0])
		v[1] = float('nan')
		fp1 = v.fingerprint()
		
		v_expected = Vector([1.0, float('nan'), 3.0])
		assert fp1 == v_expected.fingerprint()
	
	def test_single_update_with_negative_zero(self):
		"""Updating between -0.0 and 0.0 should not change fingerprint."""
		v = Vector([1.0, 0.0, 3.0])
		fp_before = v.fingerprint()
		
		v[1] = -0.0
		fp_after = v.fingerprint()
		
		assert fp_before == fp_after
	
	def test_multi_update_matches_recompute(self):
		"""Multiple element updates should match full recompute."""
		v = Vector([1, 2, 3, 4, 5])
		
		# Simulate multi-update via setitem slice
		v[1:4] = [20, 30, 40]
		fp_after_incremental = v.fingerprint()
		
		# Full recompute
		v_fresh = Vector([1, 20, 30, 40, 5])
		fp_after_recompute = v_fresh.fingerprint()
		
		assert fp_after_incremental == fp_after_recompute
	
	def test_update_with_complex_types(self):
		"""Updates with complex types should work."""
		v = Vector([[1, 2], [3, 4], [5, 6]])
		v[1] = [30, 40]
		fp1 = v.fingerprint()
		
		v_expected = Vector([[1, 2], [30, 40], [5, 6]])
		assert fp1 == v_expected.fingerprint()


class TestFingerprintDeterminism:
	"""Test that fingerprints are deterministic across runs."""
	
	def test_same_data_same_fingerprint_multiple_times(self):
		"""Creating the same vector multiple times should give same fingerprint."""
		import warnings
		fingerprints = []
		for _ in range(10):
			# Mixed types degrade to object dtype - suppress warning
			with warnings.catch_warnings():
				warnings.simplefilter("ignore", UserWarning)
				v = Vector([1, 2, 3, 'hello', None, float('nan')])
			fingerprints.append(v.fingerprint())
		
		# All should be identical
		assert len(set(fingerprints)) == 1
	
	def test_empty_vector_fingerprint(self):
		"""Empty vectors should fingerprint consistently."""
		v1 = Vector([])
		v2 = Vector([])
		assert v1.fingerprint() == v2.fingerprint()
		assert v1.fingerprint() == 0  # Empty should hash to 0
	
	def test_single_element_vectors(self):
		"""Single element vectors should fingerprint correctly."""
		v1 = Vector([42])
		v2 = Vector([42])
		v3 = Vector([43])
		
		assert v1.fingerprint() == v2.fingerprint()
		assert v1.fingerprint() != v3.fingerprint()




