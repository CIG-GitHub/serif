import pytest
from serif import Vector
from serif.naming import _sanitize_user_name
from serif import Table


def test_sanitize_simple_name():
	"""Test that simple valid names are lowercased"""
	assert _sanitize_user_name("column") == "column"
	assert _sanitize_user_name("col_1") == "col_1"
	assert _sanitize_user_name("MyColumn") == "mycolumn"


def test_sanitize_spaces():
	"""Test that spaces are replaced with underscores"""
	assert _sanitize_user_name("first name") == "first_name"
	assert _sanitize_user_name("a b c") == "a_b_c"


def test_sanitize_special_chars():
	"""Test that special characters are replaced with underscores"""
	assert _sanitize_user_name("name-with-dashes") == "name_with_dashes"
	assert _sanitize_user_name("price$") == "price"
	assert _sanitize_user_name("percent%") == "percent"
	assert _sanitize_user_name("name@domain") == "name_domain"
	assert _sanitize_user_name("col.with.dots") == "col_with_dots"


def test_sanitize_collapse_underscores():
	"""Test that user underscores are preserved (not collapsed)"""
	assert _sanitize_user_name("a__b") == "a__b"
	assert _sanitize_user_name("a___b") == "a___b"
	assert _sanitize_user_name("multiple____underscores") == "multiple____underscores"


def test_sanitize_strip_underscores():
	"""Test that leading/trailing underscores are stripped"""
	assert _sanitize_user_name("_leading") == "leading"
	assert _sanitize_user_name("trailing_") == "trailing"
	assert _sanitize_user_name("_both_") == "both"
	assert _sanitize_user_name("__multiple__") == "multiple"


def test_sanitize_starts_with_digit():
	"""Test that names starting with digits get prefixed with 'c'"""
	assert _sanitize_user_name("123") == "c123"
	assert _sanitize_user_name("2nd_column") == "c2nd_column"
	assert _sanitize_user_name("99problems") == "c99problems"


def test_sanitize_empty_result():
	"""Test that names that become empty return None"""
	assert _sanitize_user_name("") is None
	assert _sanitize_user_name("___") is None
	assert _sanitize_user_name("$$$") is None
	assert _sanitize_user_name("@#$%") is None


def test_sanitize_unicode():
	"""Test that unicode characters are replaced with underscores"""
	assert _sanitize_user_name("naïve") == "na_ve"  # ï becomes _
	assert _sanitize_user_name("café") == "caf"  # é becomes _, trailing _ stripped
	assert _sanitize_user_name("αβγ") is None  # Greek letters → all _ → empty after strip


def test_sanitize_mixed_complexity():
	"""Test complex real-world scenarios"""
	assert _sanitize_user_name("User ID") == "user_id"
	assert _sanitize_user_name("2023-Revenue ($)") == "c2023_revenue"
	assert _sanitize_user_name("__private__var__") == "private__var"
	assert _sanitize_user_name("column.1.data") == "column_1_data"


def test_sanitize_non_string_input():
	"""Test that non-string inputs are converted to strings first"""
	assert _sanitize_user_name(123) == "c123"
	assert _sanitize_user_name(45.67) == "c45_67"
	assert _sanitize_user_name(None) == "none"


def test_sanitize_preserves_valid_python_identifiers():
	"""Test that identifiers are lowercased and sanitized"""
	assert _sanitize_user_name("valid_identifier") == "valid_identifier"
	assert _sanitize_user_name("_private") == "private"  # Leading _ stripped
	assert _sanitize_user_name("CamelCase") == "camelcase"
	assert _sanitize_user_name("snake_case_name") == "snake_case_name"


def test_sanitize_csv_headers():
	"""Test sanitization of typical messy CSV column names"""
	assert _sanitize_user_name("First Name") == "first_name"
	assert _sanitize_user_name("Email Address (Primary)") == "email_address_primary"
	assert _sanitize_user_name("Price ($USD)") == "price_usd"
	assert _sanitize_user_name("Q1 2023 Revenue") == "q1_2023_revenue"



def test_table_getattr_with_spaces():
	"""Test that column names with spaces work via sanitized attribute access"""
	t = Table({
		'first name': [1, 2, 3],
		'last name': [4, 5, 6]
	})
	
	# Original names work with brackets
	assert list(t['first name']) == [1, 2, 3]
	assert list(t['last name']) == [4, 5, 6]
	
	# Sanitized names work with attribute access
	assert list(t.first_name) == [1, 2, 3]
	assert list(t.last_name) == [4, 5, 6]


def test_table_getattr_with_special_chars():
	"""Test that column names with special characters work via sanitized attributes"""
	t = Table({
		'price ($)': [10, 20, 30],
		'count@time': [1, 2, 3],
		'col.with.dots': [4, 5, 6]
	})
	
	# Original names work with brackets
	assert list(t['price ($)']) == [10, 20, 30]
	
	# Sanitized names work with attributes
	assert list(t.price) == [10, 20, 30]
	assert list(t.count_time) == [1, 2, 3]
	assert list(t.col_with_dots) == [4, 5, 6]


def test_table_getitem_sanitized():
	"""Test that __getitem__ accepts both original and sanitized names"""
	t = Table({
		'first name': [1, 2, 3],
		'price ($)': [10, 20, 30]
	})
	
	# Original names
	assert list(t['first name']) == [1, 2, 3]
	assert list(t['price ($)']) == [10, 20, 30]
	
	# Sanitized names also work
	assert list(t['first_name']) == [1, 2, 3]
	assert list(t['price']) == [10, 20, 30]


def test_table_dir_sanitized():
	"""Test that __dir__ returns sanitized names for tab completion"""
	t = Table({
		'first name': [1, 2, 3],
		'price ($)': [10, 20, 30],
		'count@time': [4, 5, 6]
	})
	
	dir_names = t.__dir__()
	
	# Sanitized names should appear
	assert 'first_name' in dir_names
	assert 'price' in dir_names
	assert 'count_time' in dir_names
	
	# Original unsanitized names should NOT appear
	assert 'first name' not in dir_names
	assert 'price ($)' not in dir_names


def test_table_unnamed_columns():
	"""Test that unnamed columns get col0_, col1_, etc."""
	col1 = Vector([1, 2, 3])
	col2 = Vector([4, 5, 6])
	col3 = Vector([7, 8, 9])
	t = Table([col1, col2, col3])
	
	# Attribute access with col{idx}_
	assert list(t.col0_) == [1, 2, 3]
	assert list(t.col1_) == [4, 5, 6]
	assert list(t.col2_) == [7, 8, 9]
	
	# __dir__ should show col0_, col1_, col2_
	dir_names = t.__dir__()
	assert 'col0_' in dir_names
	assert 'col1_' in dir_names
	assert 'col2_' in dir_names


def test_table_mixed_named_unnamed():
	"""Test mix of named and unnamed columns"""
	col1 = Vector([1, 2, 3], name='alpha')
	col2 = Vector([4, 5, 6])  # No name
	col3 = Vector([7, 8, 9], name='gamma')
	t = Table([col1, col2, col3])
	
	# Named columns work
	assert list(t.alpha) == [1, 2, 3]
	assert list(t.gamma) == [7, 8, 9]
	
	# Unnamed column accessible as col1_
	assert list(t.col1_) == [4, 5, 6]


def test_table_getattr_starts_with_digit():
	"""Test that column names starting with digits get prefixed with 'c'"""
	t = Table({
		'2023 Revenue': [100, 200, 300],
		'1st Place': [1, 2, 3]
	})
	
	# Sanitized names with 'c' prefix (lowercase)
	assert list(t.c2023_revenue) == [100, 200, 300]
	assert list(t.c1st_place) == [1, 2, 3]


def test_table_getitem_priority():
	"""Test that exact match takes priority over sanitized match"""
	# Create a table where sanitized name might conflict - warning expected
	col1 = Vector([1, 2, 3], name='first_name')
	col2 = Vector([4, 5, 6], name='first name')  # Sanitizes to same thing
	with pytest.warns(UserWarning, match="Duplicate column name 'first_name' detected"):
		t = Table([col1, col2])
	
	# Exact matches should work
	assert list(t['first_name']) == [1, 2, 3]
	assert list(t['first name']) == [4, 5, 6]
	
	# Attribute access gets first match (first_name is exact)
	assert list(t.first_name) == [1, 2, 3]


def test_table_empty_column_name():
	"""Test that empty/special-only column names get system names"""
	t = Table({
		'': [1, 2, 3],
		'   ': [4, 5, 6],  # All spaces
		'$$$': [7, 8, 9]   # All special chars
	})
	
	# Original names work with exact match in __getitem__
	assert list(t['']) == [1, 2, 3]
	assert list(t['   ']) == [4, 5, 6]
	assert list(t['$$$']) == [7, 8, 9]
	
	# All three sanitize to None, so they get system names col0_, col1_, col2_
	assert list(t.col0_) == [1, 2, 3]
	assert list(t.col1_) == [4, 5, 6]
	assert list(t.col2_) == [7, 8, 9]


def test_sanitization_preserves_camelcase():
	"""Test that CamelCase and other valid identifiers work"""
	t = Table({
		'CamelCase': [1, 2, 3],
		'snake_case': [4, 5, 6],
		'UPPERCASE': [7, 8, 9]
	})
	
	assert list(t.camelcase) == [1, 2, 3]  # case-insensitive
	assert list(t.snake_case) == [4, 5, 6]
	assert list(t.uppercase) == [7, 8, 9]  # case-insensitive



