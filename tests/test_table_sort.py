"""Tests for Table.sort_by() method."""
import pytest
from serif import Table, Vector


def test_sort_by_single_column_ascending():
	"""Test sorting by a single column in ascending order."""
	t = Table({
		'name': ['Charlie', 'Alice', 'Bob'],
		'age': [30, 25, 35]
	})
	result = t.sort_by('name')
	assert list(result['name']) == ['Alice', 'Bob', 'Charlie']
	assert list(result['age']) == [25, 35, 30]


def test_sort_by_single_column_descending():
	"""Test sorting by a single column in descending order."""
	t = Table({
		'name': ['Charlie', 'Alice', 'Bob'],
		'age': [30, 25, 35]
	})
	result = t.sort_by('name', reverse=True)
	assert list(result['name']) == ['Charlie', 'Bob', 'Alice']
	assert list(result['age']) == [30, 35, 25]


def test_sort_by_vector_reference():
	"""Test sorting using a Vector reference instead of column name."""
	t = Table({
		'name': ['Charlie', 'Alice', 'Bob'],
		'age': [30, 25, 35]
	})
	result = t.sort_by(t.age)
	assert list(result['name']) == ['Alice', 'Charlie', 'Bob']
	assert list(result['age']) == [25, 30, 35]


def test_sort_by_multiple_columns():
	"""Test sorting by multiple columns."""
	t = Table({
		'dept': ['Sales', 'IT', 'Sales', 'IT'],
		'name': ['Bob', 'Alice', 'Alice', 'Charlie'],
		'salary': [60000, 70000, 65000, 75000]
	})
	result = t.sort_by(['dept', 'name'])
	assert list(result['dept']) == ['IT', 'IT', 'Sales', 'Sales']
	assert list(result['name']) == ['Alice', 'Charlie', 'Alice', 'Bob']
	assert list(result['salary']) == [70000, 75000, 65000, 60000]


def test_sort_by_tuple_of_columns():
	"""Test sorting with tuple instead of list."""
	t = Table({
		'a': [3, 1, 2],
		'b': [6, 4, 5]
	})
	result = t.sort_by((t.a, t.b), reverse=True)
	assert list(result['a']) == [3, 2, 1]
	assert list(result['b']) == [6, 5, 4]


def test_sort_by_mixed_reverse():
	"""Test sorting with different reverse flags for different columns."""
	t = Table({
		'dept': ['Sales', 'IT', 'Sales', 'IT'],
		'salary': [60000, 70000, 65000, 75000]
	})
	result = t.sort_by(['dept', 'salary'], reverse=[False, True])
	assert list(result['dept']) == ['IT', 'IT', 'Sales', 'Sales']
	assert list(result['salary']) == [75000, 70000, 65000, 60000]


def test_sort_by_with_none_default():
	"""Test sorting with None values using default na_last=True."""
	t = Table({
		'name': ['Alice', None, 'Charlie', 'Bob'],
		'age': [25, 30, 35, 40]
	})
	result = t.sort_by('name')
	assert list(result['name']) == ['Alice', 'Bob', 'Charlie', None]
	assert list(result['age']) == [25, 40, 35, 30]


def test_sort_by_with_none_na_last_false():
	"""Test sorting with None values first (na_last=False)."""
	t = Table({
		'name': ['Alice', None, 'Charlie', 'Bob'],
		'age': [25, 30, 35, 40]
	})
	result = t.sort_by('name', na_last=False)
	assert list(result['name']) == [None, 'Alice', 'Bob', 'Charlie']
	assert list(result['age']) == [30, 25, 40, 35]


def test_sort_by_with_none_reverse_true():
	"""Test sorting with None values in descending order."""
	t = Table({
		'score': [100, None, 85, 95, None],
		'id': [1, 2, 3, 4, 5]
	})
	result = t.sort_by('score', reverse=True)
	# None values should still be last even with reverse=True
	assert list(result['score']) == [100, 95, 85, None, None]
	assert list(result['id']) == [1, 4, 3, 2, 5]


def test_sort_by_with_none_reverse_true_na_last_false():
	"""Test sorting with None values first and reverse=True."""
	t = Table({
		'score': [100, None, 85, 95, None],
		'id': [1, 2, 3, 4, 5]
	})
	result = t.sort_by('score', reverse=True, na_last=False)
	# None values should be first when na_last=False
	assert list(result['score']) == [None, None, 100, 95, 85]
	assert list(result['id']) == [2, 5, 1, 4, 3]


def test_sort_by_empty_table():
	"""Test sorting an empty table."""
	t = Table({'name': [], 'age': []})
	result = t.sort_by('name')
	assert len(result) == 0
	assert result.column_names() == ['name', 'age']


def test_sort_by_stable_sort():
	"""Test that sorting is stable (preserves order of equal elements)."""
	t = Table({
		'group': ['A', 'B', 'A', 'B', 'A'],
		'order': [1, 2, 3, 4, 5]
	})
	result = t.sort_by('group')
	# All 'A' rows should maintain their original order
	a_orders = [result['order'][i] for i in range(len(result)) if result['group'][i] == 'A']
	assert a_orders == [1, 3, 5]
	# All 'B' rows should maintain their original order
	b_orders = [result['order'][i] for i in range(len(result)) if result['group'][i] == 'B']
	assert b_orders == [2, 4]


def test_sort_by_numeric_columns():
	"""Test sorting numeric columns."""
	t = Table({
		'int_col': [3, 1, 2],
		'float_col': [3.3, 1.1, 2.2]
	})
	result = t.sort_by('int_col')
	assert list(result['int_col']) == [1, 2, 3]
	assert list(result['float_col']) == [1.1, 2.2, 3.3]


def test_sort_by_error_empty_keys():
	"""Test that sorting with no keys raises an error."""
	t = Table({'a': [1, 2, 3]})
	with pytest.raises(Exception):  # SerifValueError
		t.sort_by([])


def test_sort_by_error_mismatched_reverse_length():
	"""Test that mismatched reverse list length raises an error."""
	t = Table({'a': [1, 2, 3], 'b': [4, 5, 6]})
	with pytest.raises(Exception):  # SerifValueError
		t.sort_by(['a', 'b'], reverse=[True])


def test_sort_by_error_wrong_column_length():
	"""Test that using a Vector with wrong length raises an error."""
	t = Table({'a': [1, 2, 3]})
	wrong_vec = Vector([1, 2])  # Wrong length
	with pytest.raises(Exception):  # SerifValueError
		t.sort_by(wrong_vec)


def test_sort_by_error_invalid_column_name():
	"""Test that invalid column name raises an error."""
	t = Table({'a': [1, 2, 3]})
	with pytest.raises(Exception):  # SerifKeyError
		t.sort_by('nonexistent')


def test_sort_by_preserves_column_names():
	"""Test that sorting preserves column names."""
	t = Table({
		'employee': ['Alice', 'Bob', 'Charlie'],
		'salary': [50000, 60000, 55000]
	})
	result = t.sort_by('salary')
	assert result.column_names() == ['employee', 'salary']


def test_sort_by_does_not_mutate_original():
	"""Test that sorting returns a new table and doesn't mutate the original."""
	t = Table({
		'name': ['Charlie', 'Alice', 'Bob'],
		'age': [30, 25, 35]
	})
	original_names = list(t['name'])
	result = t.sort_by('name')
	# Original should be unchanged
	assert list(t['name']) == original_names
	# Result should be sorted
	assert list(result['name']) == ['Alice', 'Bob', 'Charlie']

