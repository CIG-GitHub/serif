import pytest
from serif import Vector
from serif import Table


def test_Vector_rename():
	"""Test that Vector.name property changes the name"""
	v = Vector([1, 2, 3], name="old_name")
	assert v.name == "old_name"
	
	v.name = "new_name"
	assert v.name == "new_name"
	assert v._wild == True  # Name setter marks as wild


def test_Vector_rename_chaining():
	"""Test that name change persists across copy"""
	v = Vector([1, 2, 3], name="original")
	v.name = "renamed"
	v2 = v.copy()
	
	assert v.name == "renamed"
	assert v2.name == "renamed"


def test_Vector_rename_to_none():
	"""Test that we can clear a name with .name = None"""
	v = Vector([1, 2, 3], name="has_name")
	v.name = None
	assert v.name is None


def test_Table_rename_column():
	"""Test renaming a single column in Table"""
	t = Table({
		'a': [1, 2, 3],
		'b': [4, 5, 6],
		'c': [7, 8, 9]
	})
	
	assert t['a']._name == 'a'
	
	result = t.rename_column('a', 'alpha')
	
	assert t['alpha']._name == 'alpha'
	assert result is t  # Returns self for chaining
	
	# Old name should not work
	with pytest.raises(KeyError):
		t['a']


def test_Table_rename_column_not_found():
	"""Test that renaming a non-existent column raises KeyError"""
	t = Table({
		'a': [1, 2, 3],
		'b': [4, 5, 6]
	})
	
	with pytest.raises(KeyError, match="Column 'z' not found"):
		t.rename_column('z', 'zeta')


def test_Table_rename_columns_dict():
	"""Test renaming multiple columns at once"""
	t = Table({
		'a': [1, 2, 3],
		'b': [4, 5, 6],
		'c': [7, 8, 9]
	})
	
	result = t.rename_columns(['a', 'b', 'c'], ['alpha', 'beta', 'gamma'])
	
	assert t['alpha']._name == 'alpha'
	assert t['beta']._name == 'beta'
	assert t['gamma']._name == 'gamma'
	assert result is t  # Returns self for chaining


def test_Table_rename_columns_partial():
	"""Test renaming only some columns"""
	t = Table({
		'a': [1, 2, 3],
		'b': [4, 5, 6],
		'c': [7, 8, 9]
	})
	
	t.rename_columns(['a', 'c'], ['alpha', 'gamma'])
	
	assert t['alpha']._name == 'alpha'
	assert t['b']._name == 'b'  # Unchanged
	assert t['gamma']._name == 'gamma'


def test_Table_rename_columns_not_found():
	"""Test that renaming a non-existent column raises KeyError"""
	t = Table({
		'a': [1, 2, 3],
		'b': [4, 5, 6]
	})
	
	with pytest.raises(KeyError, match="Column 'z' not found"):
		t.rename_columns(['a', 'z'], ['alpha', 'zeta'])


def test_Table_rename_columns_atomic():
	"""Test that rename_columns is atomic - either all succeed or none"""
	t = Table({
		'a': [1, 2, 3],
		'b': [4, 5, 6],
		'c': [7, 8, 9]
	})
	
	# Try to rename with one invalid column
	with pytest.raises(KeyError, match="Column 'invalid' not found"):
		t.rename_columns(['a', 'invalid', 'b'], ['alpha', 'oops', 'beta'])
	
	# No changes should have been made - 'a' should NOT be renamed to 'alpha'
	assert t._underlying[0]._name == 'a'
	assert t._underlying[1]._name == 'b'
	assert t._underlying[2]._name == 'c'
	
	# Should still be accessible by original names
	assert list(t['a']) == [1, 2, 3]
	assert list(t['b']) == [4, 5, 6]


def test_Table_rename_columns_chaining():
	"""Test that rename_columns returns self for chaining"""
	t = Table({
		'a': [1, 2, 3],
		'b': [4, 5, 6]
	})
	
	# Chain multiple operations
	t.rename_columns(['a'], ['alpha']).rename_column('b', 'beta')
	
	assert t['alpha']._name == 'alpha'
	assert t['beta']._name == 'beta'


def test_Table_duplicate_column_names():
	"""Test the horrible real-world condition: duplicate column names"""
	# Create table with duplicate column names - warning expected
	col1 = Vector([1, 2, 3], name='a')
	col2 = Vector([4, 5, 6], name='a')
	col3 = Vector([7, 8, 9], name='b')
	with pytest.warns(UserWarning, match="Duplicate column name 'a' detected"):
		t = Table([col1, col2, col3])
	
	# Should have two columns named 'a' and one named 'b'
	assert t._underlying[0]._name == 'a'
	assert t._underlying[1]._name == 'a'
	assert t._underlying[2]._name == 'b'
	
	# Accessing t['a'] should return the first match
	assert list(t['a']) == [1, 2, 3]
	
	# Renaming one 'a' should only rename the first match
	t.rename_column('a', 'alpha')
	assert t._underlying[0]._name == 'alpha'
	assert t._underlying[1]._name == 'a'  # Second 'a' unchanged
	assert t._underlying[2]._name == 'b'
	
	# Now we have alpha, a, b
	assert list(t['alpha']) == [1, 2, 3]
	assert list(t['a']) == [4, 5, 6]  # Gets the remaining 'a'


def test_Table_rename_all_duplicates():
	"""Test renaming ALL columns with duplicate names"""
	# Create table with duplicate column names - warning expected
	col1 = Vector([1, 2, 3], name='a')
	col2 = Vector([4, 5, 6], name='a')
	col3 = Vector([7, 8, 9], name='a')
	with pytest.warns(UserWarning, match="Duplicate column name 'a' detected"):
		t = Table([col1, col2, col3])
	
	# All three columns named 'a'
	assert all(c._name == 'a' for c in t._underlying)
	
	# rename_columns with parallel lists renames each match in order
	t.rename_columns(['a', 'a', 'a'], ['x', 'y', 'z'])
	
	# Each 'a' renamed in order
	assert t._underlying[0]._name == 'x'
	assert t._underlying[1]._name == 'y'
	assert t._underlying[2]._name == 'z'


def test_Table_rename_duplicate_columns_separately():
	"""Test renaming duplicate columns to different names using parallel sequences"""
	# Create table with duplicate column names - warning expected
	col1 = Vector([1, 2, 3], name='data')
	col2 = Vector([4, 5, 6], name='data')
	col3 = Vector([7, 8, 9], name='label')
	with pytest.warns(UserWarning, match="Duplicate column name 'data' detected"):
		t = Table([col1, col2, col3])
	
	# We want to rename both 'data' columns to different names
	# This is where parallel lists shine
	t.rename_columns(['data', 'data'], ['measurement', 'control'])
	
	assert t._underlying[0]._name == 'measurement'
	assert t._underlying[1]._name == 'control'
	assert t._underlying[2]._name == 'label'
	
	# Verify data preserved
	assert list(t['measurement']) == [1, 2, 3]
	assert list(t['control']) == [4, 5, 6]
	assert list(t['label']) == [7, 8, 9]


def test_Table_getattr_after_rename():
	"""Test that __getattr__ works after renaming"""
	t = Table({
		'a': [1, 2, 3],
		'b': [4, 5, 6]
	})
	
	# Should work before rename
	assert list(t.a) == [1, 2, 3]
	
	t.rename_column('a', 'alpha')
	
	# Old attribute should raise AttributeError (column was renamed away)
	with pytest.raises(AttributeError):
		_ = t.a
	
	# New attribute should work
	assert list(t.alpha) == [1, 2, 3]


def test_rename_preserves_data():
	"""Test that renaming doesn't affect the data"""
	v = Vector([1, 2, 3], name="old")
	original_data = list(v)
	
	v.name = "new"
	
	assert list(v) == original_data
	assert v.name == "new"


def test_Table_rename_preserves_data():
	"""Test that renaming columns doesn't affect the data"""
	t = Table({
		'a': [1, 2, 3],
		'b': [4, 5, 6]
	})
	
	original_a = list(t['a'])
	original_b = list(t['b'])
	
	t.rename_columns(['a', 'b'], ['x', 'y'])
	
	assert list(t['x']) == original_a
	assert list(t['y']) == original_b



