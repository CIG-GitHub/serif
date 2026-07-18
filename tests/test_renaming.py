import pytest
from serif import Vector
from serif import Table
from serif.errors import SerifKeyError


def test_Vector_rename():
	"""Test that Vector.vector_name property changes the name"""
	v = Vector([1, 2, 3], name="old_name")
	assert v.vector_name == "old_name"

	v.vector_name = "new_name"
	assert v.vector_name == "new_name"
	assert v._wild == True  # Name setter marks as wild


def test_Vector_rename_chaining():
	"""Test that name change persists across copy"""
	v = Vector([1, 2, 3], name="original")
	v.vector_name = "renamed"
	v2 = v.copy()

	assert v.vector_name == "renamed"
	assert v2.vector_name == "renamed"


def test_Vector_rename_to_none():
	"""Test that we can clear a name with .vector_name = None"""
	v = Vector([1, 2, 3], name="has_name")
	v.vector_name = None
	assert v.vector_name is None


# ---------------------------------------------------------------------------
# Table.rename({old: new}) — returns a NEW table (non-mutating)
# ---------------------------------------------------------------------------

def test_Table_rename_single():
	"""rename({old: new}) returns a NEW table with the column renamed."""
	t = Table({'a': [1, 2, 3], 'b': [4, 5, 6], 'c': [7, 8, 9]})

	t2 = t.rename({'a': 'alpha'})

	assert t2.column_names() == ['alpha', 'b', 'c']
	assert t2 is not t                          # non-mutating: a new table
	assert t.column_names() == ['a', 'b', 'c']  # original unchanged
	with pytest.raises(KeyError):
		t2['a']


def test_Table_rename_not_found():
	"""A missing old name raises SerifKeyError."""
	t = Table({'a': [1, 2, 3], 'b': [4, 5, 6]})
	with pytest.raises(SerifKeyError, match="Column 'z' not found"):
		t.rename({'z': 'zeta'})


def test_Table_rename_multiple():
	"""A dict renames several columns at once."""
	t = Table({'a': [1, 2, 3], 'b': [4, 5, 6], 'c': [7, 8, 9]})
	t2 = t.rename({'a': 'alpha', 'b': 'beta', 'c': 'gamma'})
	assert t2.column_names() == ['alpha', 'beta', 'gamma']


def test_Table_rename_partial():
	"""Unlisted columns are left alone."""
	t = Table({'a': [1, 2, 3], 'b': [4, 5, 6], 'c': [7, 8, 9]})
	t2 = t.rename({'a': 'alpha', 'c': 'gamma'})
	assert t2.column_names() == ['alpha', 'b', 'gamma']


def test_Table_rename_does_not_cascade():
	"""Renames resolve against the original layout — simultaneous, not chained."""
	t = Table({'a': [1, 2], 'b': [3, 4]})
	t2 = t.rename({'a': 'b', 'b': 'c'})
	assert t2.column_names() == ['b', 'c']
	assert list(t2['c']) == [3, 4]  # original 'b' → 'c'; 'a' did not cascade through


def test_Table_rename_bad_key_changes_nothing():
	"""A bad key raises before anything lands; original is untouched (non-mutating)."""
	t = Table({'a': [1, 2, 3], 'b': [4, 5, 6], 'c': [7, 8, 9]})
	with pytest.raises(SerifKeyError, match="Column 'invalid' not found"):
		t.rename({'a': 'alpha', 'invalid': 'oops', 'b': 'beta'})
	assert t.column_names() == ['a', 'b', 'c']


def test_Table_rename_chaining():
	"""Functional chaining: each rename returns a new table."""
	t = Table({'a': [1, 2, 3], 'b': [4, 5, 6]})
	t2 = t.rename({'a': 'alpha'}).rename({'b': 'beta'})
	assert t2.column_names() == ['alpha', 'beta']


def test_Table_rename_ambiguous_name_raises():
	"""A duplicated name can't be renamed by name — it's ambiguous."""
	with pytest.warns(UserWarning, match="Duplicate column name 'a'"):
		t = Table([Vector([1, 2, 3], name='a'),
		           Vector([4, 5, 6], name='a'),
		           Vector([7, 8, 9], name='b')])
	with pytest.raises(SerifKeyError, match="ambiguous"):
		t.rename({'a': 'alpha'})


def test_Table_rename_duplicates_by_index():
	"""Integer keys are the unambiguous escape for same-named columns."""
	with pytest.warns(UserWarning, match="Duplicate column name 'a'"):
		t = Table([Vector([1, 2, 3], name='a'),
		           Vector([4, 5, 6], name='a'),
		           Vector([7, 8, 9], name='a')])
	t2 = t.rename({0: 'x', 1: 'y', 2: 'z'})
	assert t2.column_names() == ['x', 'y', 'z']
	assert list(t2['x']) == [1, 2, 3]
	assert list(t2['z']) == [7, 8, 9]


def test_Table_rename_index_out_of_range():
	t = Table({'a': [1, 2]})
	with pytest.raises(SerifKeyError, match="out of range"):
		t.rename({5: 'x'})


def test_Table_rename_getattr_after():
	"""Dot-access follows the renamed (new) table; the original is unchanged."""
	t = Table({'a': [1, 2, 3], 'b': [4, 5, 6]})
	t2 = t.rename({'a': 'alpha'})

	assert list(t.a) == [1, 2, 3]       # original still has 'a'
	assert list(t2.alpha) == [1, 2, 3]  # new table has 'alpha'
	with pytest.raises(AttributeError):
		_ = t2.a                        # ...and not 'a'


def test_rename_preserves_data():
	"""Test that renaming doesn't affect the data"""
	v = Vector([1, 2, 3], name="old")
	original_data = list(v)

	v.vector_name = "new"

	assert list(v) == original_data
	assert v.vector_name == "new"


def test_Table_rename_preserves_data():
	"""Test that renaming columns doesn't affect the data"""
	t = Table({'a': [1, 2, 3], 'b': [4, 5, 6]})
	t2 = t.rename({'a': 'x', 'b': 'y'})
	assert list(t2['x']) == [1, 2, 3]
	assert list(t2['y']) == [4, 5, 6]


# ---------------------------------------------------------------------------
# Table.drop — returns a NEW table without the named column(s)
# ---------------------------------------------------------------------------

def test_drop_single_column():
	t = Table({'a': [1, 2], 'b': [3, 4], 'c': [5, 6]})
	t2 = t.drop('b')
	assert t2.column_names() == ['a', 'c']
	assert list(t2['a']) == [1, 2]
	# non-mutating: original is unchanged
	assert t.column_names() == ['a', 'b', 'c']


def test_drop_multiple_varargs():
	t = Table({'a': [1, 2], 'b': [3, 4], 'c': [5, 6]})
	assert t.drop('a', 'c').column_names() == ['b']


def test_drop_list_form():
	t = Table({'a': [1, 2], 'b': [3, 4], 'c': [5, 6]})
	assert t.drop(['a', 'b']).column_names() == ['c']


def test_drop_missing_column_raises():
	t = Table({'a': [1, 2]})
	with pytest.raises(SerifKeyError):
		t.drop('nope')


def test_drop_does_not_alias_original():
	t = Table({'a': [1, 2], 'b': [3, 4]})
	t2 = t.drop('b')
	t2[0, 'a'] = 99
	assert list(t2['a']) == [99, 2]
	assert list(t['a']) == [1, 2]  # original column must be untouched
