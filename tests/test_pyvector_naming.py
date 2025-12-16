import pytest
from serif import Vector
from serif import Table


def test_name_initialization():
	"""Test that names can be set during initialization"""
	v = Vector([1, 2, 3], name="my_vector")
	assert v._name == "my_vector"
	
	v_no_name = Vector([1, 2, 3])
	assert v_no_name._name is None


def test_copy_preserves_name_by_default():
	"""Test that copy() preserves name by default"""
	v = Vector([1, 2, 3], name="original")
	v_copy = v.copy()
	assert v_copy._name == "original"


def test_copy_can_override_name():
	"""Test that copy() can explicitly set a new name"""
	v = Vector([1, 2, 3], name="original")
	v_renamed = v.copy(name="renamed")
	assert v_renamed._name == "renamed"


def test_copy_can_clear_name():
	"""Test that copy() can explicitly clear the name"""
	v = Vector([1, 2, 3], name="original")
	v_unnamed = v.copy(name=None)
	assert v_unnamed._name is None


def test_transpose_preserves_name():
	"""Test that .T preserves the name"""
	v = Vector([1, 2, 3], name="my_vector")
	v_t = v.T
	assert v_t._name == "my_vector"


def test_slice_preserves_name():
	"""Test that slicing preserves the name"""
	v = Vector([1, 2, 3, 4, 5], name="my_vector")
	v_slice = v[1:4]
	assert v_slice._name == "my_vector"


def test_index_selection_preserves_name():
	"""Test that single index selection preserves the name"""
	v = Vector([1, 2, 3, 4, 5], name="my_vector")
	v_single = v[2]
	# Single index returns a scalar, not a Vector with a name
	assert isinstance(v_single, int)


def test_promote_preserves_name():
	"""Test that type promotion preserves the name"""
	v = Vector([1, 2, 3], name="my_vector")
	v._promote(float)
	assert v._name == "my_vector"
	assert v.schema().kind == float


def test_math_operations_do_not_preserve_name():
	"""Test that math operations do NOT preserve names"""
	v1 = Vector([1, 2, 3], name="vector1")
	v2 = Vector([4, 5, 6], name="vector2")
	
	# Binary operations with two named vectors
	assert (v1 + v2)._name is None
	assert (v1 - v2)._name is None
	assert (v1 * v2)._name is None
	assert (v1 / v2)._name is None
	assert (v1 // v2)._name is None
	assert (v1 % v2)._name is None
	assert (v1 ** v2)._name is None
	
	# Operations with scalars
	assert (v1 + 10)._name is None
	assert (v1 - 10)._name is None
	assert (v1 * 10)._name is None
	assert (v1 / 10)._name is None
	
	# Reverse operations
	assert (10 + v1)._name is None
	assert (10 - v1)._name is None
	assert (10 * v1)._name is None


def test_math_operations_with_unnamed_vectors():
	"""Test that math operations with unnamed vectors also return unnamed results"""
	v1 = Vector([1, 2, 3])
	v2 = Vector([4, 5, 6])
	
	assert (v1 + v2)._name is None
	assert (v1 * 2)._name is None


def test_aggregations_do_not_preserve_name():
	"""Test that aggregation methods do NOT preserve names"""
	v = Vector([1, 2, 3, 4, 5], name="my_vector")
	
	# 1D aggregations return scalars (no name attribute)
	assert isinstance(v.sum(), (int, float))
	assert isinstance(v.mean(), (int, float))
	assert isinstance(v.max(), (int, float))
	assert isinstance(v.min(), (int, float))
	assert isinstance(v.stdev(), (int, float))
	
	# unique() returns a Vector (no name)
	assert isinstance(v.unique(), Vector)


def test_2d_aggregations_do_not_preserve_name():
	"""Test that 2D aggregations do NOT preserve names"""
	# Create a proper 2D vector (Table-like structure)
	col1 = Vector([1, 3, 5])
	col2 = Vector([2, 4, 6])
	v = Vector([col1, col2], name="my_matrix")
	
	# 2D aggregations return Vectors without names
	assert v.sum()._name is None
	assert v.mean()._name is None
	assert v.max()._name is None
	assert v.min()._name is None
	assert v.stdev()._name is None


def test_string_methods_do_not_preserve_name():
	"""Test that string methods do NOT preserve names"""
	v = Vector(["Hello", "World"], name="my_strings", dtype=str)
	
	assert v.upper()._name is None
	assert v.lower()._name is None
	assert v.strip()._name is None
	assert v.replace("l", "L")._name is None
	assert v.split()._name is None


def test_Table_column_selection_preserves_name():
	"""Test that selecting a column from Table preserves the column name"""
	t = Table({
		'a': [1, 2, 3],
		'b': [4, 5, 6],
		'c': [7, 8, 9]
	})
	
	col_a = t['a']
	assert col_a._name == 'a'
	
	col_b = t['b']
	assert col_b._name == 'b'


def test_Table_multi_column_selection():
	"""Test that selecting multiple columns works and preserves names"""
	t = Table({
		'a': [1, 2, 3],
		'b': [4, 5, 6],
		'c': [7, 8, 9]
	})
	
	# Multi-column selection
	t2 = t['a', 'c']
	assert isinstance(t2, Table)
	assert len(t2._underlying) == 2
	assert t2['a']._name == 'a'
	assert t2['c']._name == 'c'
	assert list(t2['a']) == [1, 2, 3]
	assert list(t2['c']) == [7, 8, 9]
	
	# Duplicate column selection (should create copies) - duplicate 'a' warning expected
	with pytest.warns(UserWarning, match="Duplicate column name 'a' detected"):
		t3 = t['a', 'a', 'a']
	assert isinstance(t3, Table)
	assert len(t3._underlying) == 3
	# All three should have the same name and values
	for col in t3._underlying:
		assert col._name == 'a'
		assert list(col) == [1, 2, 3]


def test_Table_operations_on_named_columns():
	"""Test that operations on named columns from Table don't preserve names"""
	t = Table({
		'a': [1, 2, 3],
		'b': [4, 5, 6]
	})
	
	# Column selection preserves name
	assert t['a']._name == 'a'
	
	# But math operations do not
	assert (t['a'] + t['b'])._name is None
	assert (t['a'] * 2)._name is None
	assert (t['a'] + 10)._name is None


def test_name_after_mutation():
	"""Test that name is preserved after mutation via __setitem__"""
	v = Vector([1, 2, 3], name="my_vector")
	v[1] = 99
	assert v._name == "my_vector"
	assert v[1] == 99


def test_name_with_multiple_mutations():
	"""Test that name persists through multiple mutations"""
	v = Vector([1, 2, 3, 4, 5], name="persistent")
	v[0] = 10
	v[2] = 20
	v[4] = 30
	assert v._name == "persistent"
	assert list(v) == [10, 2, 20, 4, 30]


def test_name_independence_between_copies():
	"""Test that renaming a copy doesn't affect the original"""
	v1 = Vector([1, 2, 3], name="original")
	v2 = v1.copy(name="copy")
	
	assert v1._name == "original"
	assert v2._name == "copy"


def test_operations_create_independent_unnamed_vectors():
	"""Test that operations create new unnamed vectors independent of inputs"""
	v1 = Vector([1, 2, 3], name="vec1")
	v2 = Vector([4, 5, 6], name="vec2")
	
	v3 = v1 + v2
	assert v3._name is None
	
	# Original vectors unchanged
	assert v1._name == "vec1"
	assert v2._name == "vec2"


def test_alias_names_unnamed_vector():
	"""Test that alias() can name an unnamed vector"""
	v = Vector([1, 2, 3])
	assert v._name is None
	
	result = v.alias("my_name")
	assert v._name == "my_name"
	assert result is v  # Returns self for chaining
	assert v._wild == True  # Marks as wild


def test_alias_fails_on_named_vector():
	"""Test that alias() fails if vector already has a name"""
	from serif.errors import SerifValueError
	v = Vector([1, 2, 3], name="existing")
	
	with pytest.raises(SerifValueError, match="alias\\(\\) is reserved for unnamed vectors only"):
		v.alias("new_name")
	
	# Name should be unchanged
	assert v._name == "existing"


def test_name_property_getter():
	"""Test that .name property returns the name"""
	v = Vector([1, 2, 3], name="test")
	assert v.name == "test"
	
	v_unnamed = Vector([4, 5, 6])
	assert v_unnamed.name is None


def test_name_property_setter():
	"""Test that .name property can set the name"""
	v = Vector([1, 2, 3], name="old")
	assert v.name == "old"
	
	v.name = "new"
	assert v.name == "new"
	assert v._wild == True
	
	# Can also set to None
	v.name = None
	assert v.name is None
