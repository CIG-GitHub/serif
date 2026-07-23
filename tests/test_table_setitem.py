"""
Test Table.__setitem__ with comprehensive slice coverage.

All of these writes are OWNER-ADDRESSED (through the table), which is the
legal spelling under the mutation doctrine — read through the column,
write through the table. The vector-addressed spellings raise; see
test_mutation_doctrine.py.
"""

import pytest
from serif import Vector, Table
from serif.errors import (
	SerifIndexError,
	SerifKeyError,
	SerifValueError,
	SerifTypeError,
)


class TestScalarAssignment:
	"""Single cell assignment with various indexing patterns."""
	
	def test_scalar_by_int_int(self):
		t = Table({'a': [1, 2, 3], 'b': [4, 5, 6]})
		t[0, 0] = 99
		assert t[0, 0] == 99
		assert t[0, 'a'] == 99
	
	def test_scalar_by_int_name(self):
		t = Table({'a': [1, 2, 3], 'b': [4, 5, 6]})
		t[1, 'b'] = 88
		assert t[1, 1] == 88
		assert t[1, 'b'] == 88
	
	def test_scalar_by_neg_index(self):
		t = Table({'a': [1, 2, 3], 'b': [4, 5, 6]})
		t[-1, -1] = 77
		assert t[-1, -1] == 77
		assert t[2, 1] == 77


class TestBroadcastScalarToSlice:
	"""Broadcast a scalar to multiple cells."""
	
	def test_broadcast_to_column_slice(self):
		t = Table({'a': [1, 2, 3], 'b': [4, 5, 6]})
		t[0:2, 'a'] = 100
		assert list(t.cols()[0]) == [100, 100, 3]
	
	def test_broadcast_to_all_rows_single_col(self):
		t = Table({'a': [1, 2, 3], 'b': [4, 5, 6]})
		t[:, 'b'] = 42
		assert list(t.cols()[1]) == [42, 42, 42]
	
	def test_broadcast_to_rectangular_region(self):
		t = Table({
			'a': [1, 2, 3, 4],
			'b': [5, 6, 7, 8],
			'c': [9, 10, 11, 12]})
		t[1:3, 0:2] = 999
		assert t[1, 0] == 999
		assert t[1, 1] == 999
		assert t[2, 0] == 999
		assert t[2, 1] == 999
		assert t[2, 2] == 11  # Unchanged
	
	def test_broadcast_to_step_slice(self):
		t = Table({'x': [1, 2, 3, 4, 5]})
		t[::2, 'x'] = 0
		assert list(t.cols()[0]) == [0, 2, 0, 4, 0]
	
	def test_broadcast_to_reverse_slice(self):
		t = Table({'x': [1, 2, 3, 4]})
		t[::-1, 'x'] = 9
		assert list(t.cols()[0]) == [9, 9, 9, 9]
	
	def test_broadcast_to_all_columns(self):
		t = Table({'a': [1, 2], 'b': [3, 4], 'c': [5, 6]})
		t[0, :] = 0
		assert t[0, 0] == 0
		assert t[0, 1] == 0
		assert t[0, 2] == 0
		assert t[1, 0] == 2  # Unchanged


class TestRowAssignment:
	"""Assign a sequence to an entire row."""
	
	def test_assign_list_to_row(self):
		t = Table({'a': [1, 2, 3], 'b': [4, 5, 6]})
		t[0, :] = [10, 20]
		assert t[0, 'a'] == 10
		assert t[0, 'b'] == 20
	
	def test_assign_tuple_to_row(self):
		t = Table({'a': [1, 2, 3], 'b': [4, 5, 6]})
		t[1, :] = (30, 40)
		assert t[1, 'a'] == 30
		assert t[1, 'b'] == 40
	
	def test_assign_to_row_subset_columns(self):
		t = Table({'a': [1, 2], 'b': [3, 4], 'c': [5, 6]})
		t[0, 1:3] = [99, 88]
		assert t[0, 0] == 1   # Unchanged
		assert t[0, 1] == 99
		assert t[0, 2] == 88
	
	def test_row_assignment_length_mismatch(self):
		t = Table({'a': [1, 2], 'b': [3, 4]})
		with pytest.raises(SerifValueError, match="length mismatch"):
			t[0, :] = [1, 2, 3]  # Too many values


class TestColumnAssignment:
	"""Assign a vector/sequence to an entire column."""
	
	def test_assign_list_to_column_by_index(self):
		t = Table({'a': [1, 2, 3], 'b': [4, 5, 6]})
		t[:, 1] = [40, 50, 60]
		assert list(t.cols()[1]) == [40, 50, 60]
	
	def test_assign_to_column_slice(self):
		t = Table({'x': [1, 2, 3, 4]})
		t[1:3, 'x'] = [99, 88]
		assert list(t.cols()[0]) == [1, 99, 88, 4]
	
	def test_assign_with_step_to_column(self):
		t = Table({'x': [1, 2, 3, 4, 5]})
		t[::2, 'x'] = [10, 30, 50]
		assert list(t.cols()[0]) == [10, 2, 30, 4, 50]


class TestVectorColumnAssignment:
	"""Assign a Vector to one table-owned column region."""

	def test_assign_filtered_vector_to_boolean_mask(self):
		t = Table({'x': [1, 2, 3, 4]})
		values = Vector([10, 20, 30, 40])
		mask = t.x > 2

		t[mask, 'x'] = values[mask]

		assert list(t.x) == [1, 2, 30, 40]

	def test_assign_vector_to_entire_column(self):
		t = Table({'x': [1, 2, 3], 'y': [4, 5, 6]})

		t[:, 'x'] = Vector([10, 20, 30])

		assert list(t.x) == [10, 20, 30]
		assert list(t.y) == [4, 5, 6]

	def test_nullable_mask_excludes_null_rows(self):
		t = Table({'x': [1, 2, 3, 4]})
		mask = Vector([True, None, False, True])

		t[mask, 'x'] = Vector([10, 40])

		assert list(t.x) == [10, 2, 3, 40]

	def test_vector_length_mismatch_is_atomic(self):
		t = Table({'x': [1, 2, 3, 4]})
		mask = Vector([True, False, False, True])

		with pytest.raises(
			SerifValueError,
			match="number of True mask elements",
		):
			t[mask, 'x'] = Vector([10])

		assert list(t.x) == [1, 2, 3, 4]

	def test_vector_rhs_works_inside_batch(self):
		t = Table({'x': [1, 2, 3, 4]})
		mask = Vector([False, True, True, False])

		with t.batch() as mutable:
			mutable[mask, 'x'] = Vector([20, 30])

		assert list(t.x) == [1, 20, 30, 4]

	def test_vector_rhs_for_multiple_columns_remains_unsupported(self):
		t = Table({'x': [1, 2], 'y': [3, 4]})

		with pytest.raises(
			SerifTypeError,
			match="Unsupported assignment value type",
		):
			t[:, ['x', 'y']] = Vector([10, 20])

		assert t.to_dict() == {'x': [1, 2], 'y': [3, 4]}


class TestRectangularAssignment:
	"""Assign a Table to a rectangular region."""
	
	def test_assign_table_to_region(self):
		t = Table({
			'a': [1, 2, 3, 4],
			'b': [5, 6, 7, 8],
			'c': [9, 10, 11, 12]
		})
		
		source = Table({
			'x': [99, 88],
			'y': [77, 66]
		})
		
		t[1:3, 0:2] = source
		assert t[1, 0] == 99
		assert t[1, 1] == 77
		assert t[2, 0] == 88
		assert t[2, 1] == 66
		assert t[1, 2] == 10  # Unchanged

	def test_assign_table_full_rows(self):
		t = Table({'a': [1, 2, 3], 'b': [4, 5, 6]})
		source = Table({'x': [10, 20], 'y': [30, 40]})
		t[0:2, :] = source
		assert list(t.cols()[0]) == [10, 20, 3]
		assert list(t.cols()[1]) == [30, 40, 6]

	def test_assign_table_column_mismatch(self):
		t = Table({'a': [1, 2], 'b': [3, 4]})
		source = Table({'x': [10]})
		with pytest.raises(SerifValueError, match="Column count mismatch"):
			t[:, :] = source


class TestListOfColumnsAssignment:
	"""Assign a list of lists/vectors as columns."""
	
	def test_assign_list_of_lists_to_all_columns(self):
		t = Table({'a': [1, 2, 3], 'b': [4, 5, 6]})
		t[:, :] = [[10, 20, 30], [40, 50, 60]]
		assert list(t.cols()[0]) == [10, 20, 30]
		assert list(t.cols()[1]) == [40, 50, 60]
	
	def test_assign_list_of_vectors_to_subset(self):
		t = Table({
			'a': [1, 2, 3],
			'b': [4, 5, 6],
			'c': [7, 8, 9]
		})
		t[:, 1:3] = [Vector([99, 88, 77]), Vector([66, 55, 44])]
		assert list(t.cols()[0]) == [1, 2, 3]  # Unchanged
		assert list(t.cols()[1]) == [99, 88, 77]
		assert list(t.cols()[2]) == [66, 55, 44]
	
	def test_assign_list_shape_mismatch(self):
		t = Table({'a': [1, 2], 'b': [3, 4]})
		with pytest.raises(SerifValueError, match="Shape mismatch"):
			t[:, :] = [[1, 2], [3, 4], [5, 6]]  # Too many columns


class TestNamedColumnSequenceAssignment:
	"""Named-column sequences mean the same thing in reads and writes."""

	@pytest.mark.parametrize(
		"columns",
		[
			['col 1', 'col 2'],
			('col 1', 'col 2'),
		],
	)
	def test_assign_table_to_named_columns_for_every_row(self, columns):
		t = Table({
			'col 1': [1, 2, 3, 4],
			'keep': [5, 6, 7, 8],
			'col 2': [9, 10, 11, 12],
		})
		source = Table({
			'left': [20, 30, 40, 50],
			'right': [90, 100, 110, 120],
		})

		t[columns] = source

		assert t.column_names() == ['col 1', 'keep', 'col 2']
		assert t.to_dict() == {
			'col 1': [20, 30, 40, 50],
			'keep': [5, 6, 7, 8],
			'col 2': [90, 100, 110, 120],
		}

	@pytest.mark.parametrize(
		"columns",
		[
			['col 1', 'col 2'],
			('col 1', 'col 2'),
		],
	)
	def test_assign_table_to_named_columns_under_mask(self, columns):
		t = Table({
			'col 1': [1, 2, 3, 4],
			'keep': [5, 6, 7, 8],
			'col 2': [9, 10, 11, 12],
		})
		mask = Vector([True, False, True, False])
		source = Table({
			'left': [20, 40],
			'right': [90, 110],
		})

		t[mask, columns] = source

		assert t.to_dict() == {
			'col 1': [20, 2, 40, 4],
			'keep': [5, 6, 7, 8],
			'col 2': [90, 10, 110, 12],
		}

	def test_assign_table_to_named_columns_under_empty_reverse_slice(self):
		t = Table({
			'col 1': [1, 2, 3, 4],
			'col 2': [5, 6, 7, 8],
		})
		source = Table({'left': [], 'right': []})

		t[1:3:-7, ['col 1', 'col 2']] = source

		assert t.to_dict() == {
			'col 1': [1, 2, 3, 4],
			'col 2': [5, 6, 7, 8],
		}

	def test_masked_table_assignment_row_mismatch_is_atomic(self):
		t = Table({
			'col 1': [1, 2, 3, 4],
			'col 2': [5, 6, 7, 8],
		})
		mask = Vector([True, False, True, False])
		source = Table({'left': [20], 'right': [50]})

		with pytest.raises(
			SerifValueError,
			match="number of True mask elements",
		):
			t[mask, ['col 1', 'col 2']] = source

		assert t.to_dict() == {
			'col 1': [1, 2, 3, 4],
			'col 2': [5, 6, 7, 8],
		}


class TestSliceEdgeCases:
	"""Edge cases and boundary conditions."""
	
	def test_empty_slice_rows(self):
		t = Table({'x': [1, 2, 3]})
		t[5:10, 'x'] = 999  # Out of range, should be no-op
		assert list(t.cols()[0]) == [1, 2, 3]
	
	def test_negative_step_slice(self):
		t = Table({'x': [1, 2, 3, 4, 5]})
		t[4:1:-1, 'x'] = [50, 40, 30]
		assert list(t.cols()[0]) == [1, 2, 30, 40, 50]
	
	def test_assign_to_single_column_by_list(self):
		t = Table({'a': [1, 2], 'b': [3, 4], 'c': [5, 6]})
		t[:, ['b']] = [[99, 88]]
		assert list(t.cols()[1]) == [99, 88]
	
	def test_assign_to_multiple_columns_by_name_list(self):
		t = Table({'a': [1, 2], 'b': [3, 4], 'c': [5, 6]})
		t[:, ['a', 'c']] = [[10, 20], [30, 40]]
		assert list(t.cols()[0]) == [10, 20]
		assert list(t.cols()[1]) == [3, 4]  # Unchanged
		assert list(t.cols()[2]) == [30, 40]
	
	def test_assign_string_scalar_fails(self):
		# Can't assign string to int vector without explicit conversion
		t = Table({'x': [1, 2, 3]})
		with pytest.raises(SerifTypeError, match="Cannot set str in int vector"):
			t[0, 'x'] = "hello"
	
	def test_assign_to_entire_table(self):
		t = Table({'a': [1, 2], 'b': [3, 4]})
		t[:, :] = 0
		assert list(t.cols()[0]) == [0, 0]
		assert list(t.cols()[1]) == [0, 0]


class TestErrorConditions:
	"""Invalid operations that should raise errors."""
	
	def test_invalid_column_name(self):
		t = Table({'x': [1, 2]})
		with pytest.raises(SerifKeyError, match="not found"):
			t[0, 'nonexistent'] = 5
	
	def test_invalid_key_dimension(self):
		t = Table({'x': [1, 2]})
		with pytest.raises(SerifKeyError, match="requires 1D .* or 2D"):
			t[0, 0, 0] = 5
	
	def test_unsupported_value_type(self):
		t = Table({'x': [1, 2]})
		with pytest.raises(SerifTypeError, match="Unsupported assignment value type"):
			t[:, 'x'] = {1: 'a', 2: 'b'}  # Dict not supported

	def test_invalid_mixed_column_selector_is_atomic(self):
		t = Table({'a': [1, 2], 'b': [3, 4]})
		with pytest.raises(SerifTypeError, match="only names or integer"):
			t[0, [0, object()]] = 9
		assert t.to_dict() == {'a': [1, 2], 'b': [3, 4]}

	def test_out_of_range_column_selector_is_atomic(self):
		t = Table({'a': [1, 2], 'b': [3, 4]})
		with pytest.raises(SerifIndexError, match="Column index 9"):
			t[0, [0, 9]] = 9
		assert t.to_dict() == {'a': [1, 2], 'b': [3, 4]}

	def test_out_of_range_row_selector_is_atomic(self):
		t = Table({'a': [1, 2], 'b': [3, 4]})
		with pytest.raises(SerifIndexError, match="Row index 9"):
			t[[0, 9]] = 9
		assert t.to_dict() == {'a': [1, 2], 'b': [3, 4]}

	def test_boolean_scalar_is_not_a_position(self):
		t = Table({'a': [1, 2], 'b': [3, 4]})
		with pytest.raises(SerifTypeError, match="Boolean scalar"):
			t[True, :] = 9
		with pytest.raises(SerifTypeError, match="Boolean values"):
			t[0, True] = 9
		assert t.to_dict() == {'a': [1, 2], 'b': [3, 4]}


class TestMixedTypeAssignment:
	"""Assignment across different column types."""
	
	def test_assign_int_to_float_column(self):
		t = Table({'x': [1.5, 2.5, 3.5]})
		t[0, 'x'] = 10
		assert t[0, 'x'] == 10.0
	
	# Object vector assignment currently has issues
	# def test_assign_to_object_column(self):
	# 	t = Table({'mixed': [1, "two", 3.0]})
	# 	t[1, 'mixed'] = 999
	# 	assert t[1, 'mixed'] == 999
	
	def test_assign_heterogeneous_row(self):
		t = Table({
			'int': [1, 2],
			'float': [1.5, 2.5],
			'str': ["a", "b"]
		})
		t[0, :] = [99, 99.9, "xyz"]
		assert t[0, 'int'] == 99
		assert t[0, 'float'] == 99.9
		assert t[0, 'str'] == "xyz"


class TestComplexSlicing:
	"""Complex multi-dimensional slicing patterns."""
	
	def test_slice_with_step_both_dimensions(self):
		t = Table({
			'a': [1, 2, 3, 4, 5],
			'b': [6, 7, 8, 9, 10],
			'c': [11, 12, 13, 14, 15],
			'd': [16, 17, 18, 19, 20],
		})
		t[::2, ::2] = 0
		assert t[0, 0] == 0
		assert t[0, 2] == 0
		assert t[2, 0] == 0
		assert t[2, 2] == 0
		assert t[1, 1] == 7  # Unchanged
	
	def test_reverse_slice_assignment(self):
		t = Table({'a': [1, 2, 3], 'b': [4, 5, 6]})
		t[::-1, :] = [[30, 20, 10], [60, 50, 40]]
		assert list(t.cols()[0]) == [10, 20, 30]
		assert list(t.cols()[1]) == [40, 50, 60]
	
	def test_partial_reverse_slice(self):
		t = Table({'x': [1, 2, 3, 4, 5]})
		t[3:0:-1, 'x'] = [40, 30, 20]
		assert list(t.cols()[0]) == [1, 20, 30, 40, 5]
	
	def test_overlapping_slice_assignment(self):
		# Assign to a slice, then assign to overlapping slice
		t = Table({'x': [1, 2, 3, 4, 5]})
		t[0:3, 'x'] = [10, 20, 30]
		t[2:5, 'x'] = [33, 44, 55]
		assert list(t.cols()[0]) == [10, 20, 33, 44, 55]


class TestImplicitRowSlice:
	"""Test single index (row only) implies all columns."""
	
	def test_single_int_scalar_assignment(self):
		t = Table({'a': [1, 2, 3], 'b': [4, 5, 6]})
		t[0] = [99, 88]
		assert t[0, 'a'] == 99
		assert t[0, 'b'] == 88
	
	def test_single_slice_assignment(self):
		t = Table({'a': [1, 2, 3], 'b': [4, 5, 6]})
		t[0:2] = [[10, 20], [30, 40]]
		assert list(t.cols()[0]) == [10, 20, 3]
		assert list(t.cols()[1]) == [30, 40, 6]




