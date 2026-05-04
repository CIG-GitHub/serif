import pytest
from serif import Vector, Table


def test_stale_column_name_after_vector_rename():
	"""Test that accessing old column name via dot-access fails after rename"""
	t = Table({
		'old_name': [1, 2, 3],
		'other': [4, 5, 6]
	})
	
	# Access and rename via vector's name property
	t.old_name.name = 'new_name'
	
	# Old name should raise AttributeError
	with pytest.raises(AttributeError, match="old_name"):
		_ = t.old_name
	
	# New name should work
	assert list(t.new_name) == [1, 2, 3]


def test_stale_column_name_after_multiple_renames():
	"""Test that multiple renames invalidate previous names"""
	t = Table({
		'first': [1, 2, 3]
	})
	
	# Rename multiple times
	t.first.name = 'second'
	t.second.name = 'third'
	
	# Both old names should fail
	with pytest.raises(AttributeError):
		_ = t.first
	
	with pytest.raises(AttributeError):
		_ = t.second
	
	# Only current name should work
	assert list(t.third) == [1, 2, 3]


def test_stale_column_name_subscript_access():
	"""Test that subscript access with old name also fails"""
	t = Table({
		'old': [1, 2, 3]
	})
	
	t.old.name = 'new'
	
	# Subscript access with old name should raise KeyError
	with pytest.raises(KeyError):
		_ = t['old']
	
	# New name should work
	assert list(t['new']) == [1, 2, 3]


def test_multiple_columns_rename_independently():
	"""Test that renaming one column doesn't affect access to others"""
	t = Table({
		'a': [1, 2, 3],
		'b': [4, 5, 6],
		'c': [7, 8, 9]
	})
	
	# Rename just 'b'
	t.b.name = 'beta'
	
	# 'a' and 'c' should still work
	assert list(t.a) == [1, 2, 3]
	assert list(t.c) == [7, 8, 9]
	
	# 'b' should not work
	with pytest.raises(AttributeError):
		_ = t.b
	
	# 'beta' should work
	assert list(t.beta) == [4, 5, 6]


def test_rename_then_rename_column_method():
	"""Test interaction between vector.name property and table.rename_column()"""
	t = Table({
		'col1': [1, 2, 3],
		'col2': [4, 5, 6]
	})
	
	# Use vector's name property
	t.col1.name = 'temp'
	
	# Now use table's rename_column on the new name
	t.rename_column('temp', 'final')
	
	# All old names should fail
	with pytest.raises(AttributeError):
		_ = t.col1
	
	with pytest.raises(AttributeError):
		_ = t.temp
	
	# Only 'final' should work
	assert list(t.final) == [1, 2, 3]


def test_rename_case_sensitivity():
	"""Test that renamed columns respect case changes"""
	t = Table({
		'lowercase': [1, 2, 3]
	})
	
	t.lowercase.name = 'UPPERCASE'
	
	# Old lowercase should fail
	with pytest.raises(AttributeError):
		_ = t.lowercase
	
	# New uppercase should work (exact match)
	assert list(t.UPPERCASE) == [1, 2, 3]
	
	# Lowercase version of new name should also work (case-insensitive fallback)
	assert list(t.uppercase) == [1, 2, 3]




