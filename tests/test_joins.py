import pytest
from serif import Vector
from serif import Table
from serif.errors import SerifTypeError


def test_inner_join_basic():
	"""Test basic inner join with single key"""
	left = Table({
		'id': [1, 2, 3],
		'name': ['Alice', 'Bob', 'Charlie']
	})
	right = Table({
		'id': [2, 3, 4],
		'age': [25, 30, 35]
	})
	
	# Single key join using string syntax - duplicate 'id' column expected
	with pytest.warns(UserWarning, match="Duplicate column name 'id' detected"):
		result = left.inner_join(right, left_on='id', right_on='id')
	
	assert len(result) == 2
	assert list(result['name']) == ['Bob', 'Charlie']
	assert list(result['age']) == [25, 30]


def test_inner_join_multi_key():
	"""Test inner join with composite keys"""
	left = Table({
		'customer_id': [1, 1, 2],
		'date': ['2023-01-01', '2023-01-02', '2023-01-01'],
		'amount': [100, 200, 300]
	})
	right = Table({
		'customer_id': [1, 2, 1],
		'date': ['2023-01-01', '2023-01-01', '2023-01-03'],
		'status': ['active', 'active', 'inactive']
	})
	
	# Multi-key join - duplicate 'customer_id' and 'date' columns expected
	with pytest.warns(UserWarning, match="Duplicate column name '(customer_id|date)' detected"):
		result = left.inner_join(
			right, 
			left_on=['customer_id', 'date'],
			right_on=['customer_id', 'date']
		)
	
	assert len(result) == 2
	assert list(result['amount']) == [100, 300]
	assert list(result['status']) == ['active', 'active']


def test_join_left():
	"""Test left join preserves all left rows"""
	left = Table({
		'id': [1, 2, 3],
		'name': ['Alice', 'Bob', 'Charlie']
	})
	right = Table({
		'id': [2, 4],
		'age': [25, 35]
	})
	
	# Left join - duplicate 'id' column expected
	with pytest.warns(UserWarning, match="Duplicate column name 'id' detected"):
		result = left.join(right, left_on='id', right_on='id')
	
	assert len(result) == 3
	assert list(result['name']) == ['Alice', 'Bob', 'Charlie']
	assert list(result['age']) == [None, 25, None]


def test_full_join():
	"""Test full outer join includes all rows from both tables"""
	left = Table({
		'id': [1, 2],
		'left_val': ['A', 'B']
	})
	right = Table({
		'id': [2, 3],
		'right_val': ['X', 'Y']
	})
	
	# Full join - duplicate 'id' column expected
	with pytest.warns(UserWarning, match="Duplicate column name 'id' detected"):
		result = left.full_join(right, left_on='id', right_on='id')
	
	assert len(result) == 3
	# Check that we have rows for id 1, 2, and 3
	assert None in list(result['left_val'])  # id=3 has no left match
	assert None in list(result['right_val'])  # id=1 has no right match


def test_many_to_one_cardinality():
	"""Test that many_to_one default catches duplicate right keys"""
	left = Table({
		'order_id': [1, 2, 3],
		'customer_id': [100, 100, 200]
	})
	right = Table({
		'customer_id': [100, 100, 200],  # Duplicate 100!
		'name': ['Alice', 'Alice2', 'Bob']
	})
	
	with pytest.raises(ValueError, match="many_to_one.*violated.*Right side has duplicate keys"):
		left.inner_join(right, left_on='customer_id', right_on='customer_id')


def test_one_to_one_cardinality():
	"""Test that one_to_one catches duplicates on both sides"""
	left = Table({
		'id': [1, 1, 2],  # Duplicate 1!
		'left_val': ['A', 'A2', 'B']
	})
	right = Table({
		'id': [1, 2],
		'right_val': ['X', 'Y']
	})
	
	with pytest.raises(ValueError, match="one_to_one.*violated.*Left side has duplicate key"):
		left.inner_join(right, left_on='id', right_on='id', expect='one_to_one')


def test_many_to_many_allows_duplicates():
	"""Test that many_to_many allows duplicates on both sides"""
	left = Table({
		'key': [1, 1, 2],
		'left_val': ['A', 'B', 'C']
	})
	right = Table({
		'key': [1, 1, 2],
		'right_val': ['X', 'Y', 'Z']
	})
	
	# Many-to-many join - duplicate 'key' column expected
	with pytest.warns(UserWarning, match="Duplicate column name 'key' detected"):
		result = left.inner_join(right, left_on='key', right_on='key', expect='many_to_many')
	
	# 1 matches 1 twice = 4 combinations (A-X, A-Y, B-X, B-Y)
	# 2 matches 2 once = 1 combination (C-Z)
	assert len(result) == 5


def test_one_to_many_cardinality():
	"""Test that one_to_many allows multiple right matches per left"""
	left = Table({
		'id': [1, 2],
		'name': ['Alice', 'Bob']
	})
	right = Table({
		'id': [1, 1, 2],
		'order': ['Order1', 'Order2', 'Order3']
	})
	
	# One-to-many join - duplicate 'id' column expected
	with pytest.warns(UserWarning, match="Duplicate column name 'id' detected"):
		result = left.inner_join(right, left_on='id', right_on='id', expect='one_to_many')
	
	assert len(result) == 3
	assert list(result['name']) == ['Alice', 'Alice', 'Bob']
	assert list(result['order']) == ['Order1', 'Order2', 'Order3']


def test_join_empty_result():
	"""Test that join with no matches returns empty table"""
	left = Table({
		'id': [1, 2],
		'val': ['A', 'B']
	})
	right = Table({
		'id': [3, 4],
		'val': ['X', 'Y']
	})
	
	result = left.inner_join(right, left_on='id', right_on='id')
	
	assert len(result) == 0


def test_join_preserves_column_names():
	"""Test that column names are preserved after join"""
	left = Table({
		'customer_id': [1, 2],
		'order_total': [100, 200]
	})
	right = Table({
		'id': [1, 2],
		'customer_name': ['Alice', 'Bob']
	})
	
	result = left.inner_join(right, left_on='customer_id', right_on='id')
	
	# Check column names are accessible
	assert list(result.customer_id) == [1, 2]
	assert list(result.order_total) == [100, 200]
	assert list(result.customer_name) == ['Alice', 'Bob']


def test_left_join_with_multiple_right_matches():
	"""Test left join when right side has multiple matches (many_to_many)"""
	left = Table({
		'id': [1],
		'name': ['Alice']
	})
	right = Table({
		'id': [1, 1],
		'order': ['Order1', 'Order2']
	})
	
	# Left join - duplicate 'id' column expected
	with pytest.warns(UserWarning, match="Duplicate column name 'id' detected"):
		result = left.join(right, left_on='id', right_on='id', expect='one_to_many')
	
	assert len(result) == 2
	assert list(result['name']) == ['Alice', 'Alice']
	assert list(result['order']) == ['Order1', 'Order2']


def test_full_join_no_matches():
	"""Test full join when no keys match"""
	left = Table({
		'id': [1, 2],
		'left_val': ['A', 'B']
	})
	right = Table({
		'id': [3, 4],
		'right_val': ['X', 'Y']
	})
	
	# Full join - duplicate 'id' column expected
	with pytest.warns(UserWarning, match="Duplicate column name 'id' detected"):
		result = left.full_join(right, left_on='id', right_on='id')
	
	assert len(result) == 4
	# All left_val should have 2 values and 2 Nones
	assert list(result['left_val']).count(None) == 2
	# All right_val should have 2 values and 2 Nones
	assert list(result['right_val']).count(None) == 2


def test_join_different_column_names():
	"""Test join where left and right use different column names"""
	left = Table({
		'user_id': [1, 2, 3],
		'name': ['Alice', 'Bob', 'Charlie']
	})
	right = Table({
		'customer_id': [2, 3, 4],
		'purchases': [5, 10, 15]
	})
	
	result = left.inner_join(right, left_on='user_id', right_on='customer_id')
	
	assert len(result) == 2
	assert list(result['name']) == ['Bob', 'Charlie']
	assert list(result['purchases']) == [5, 10]


def test_join_with_Vector_columns():
	"""Test join using Vector columns directly instead of strings"""
	left = Table({'id': [1, 2]})
	right = Table({'id': [2, 3]})
	
	# Join with Vector columns - duplicate 'id' column expected
	with pytest.warns(UserWarning, match="Duplicate column name 'id' detected"):
		result = left.inner_join(right, left_on=left.id, right_on=right.id)
	
	assert len(result) == 1
	assert list(result['id']) == [2]


def test_join_multi_key_with_Vectors():
	"""Test multi-key join using Vector columns"""
	left = Table({
		'a': [1, 1, 2],
		'b': [10, 20, 10],
		'val': ['x', 'y', 'z']
	})
	right = Table({
		'a': [1, 2, 1],
		'b': [10, 10, 30],
		'data': ['p', 'q', 'r']
	})
	
	# Multi-key join - duplicate 'a' and 'b' columns expected
	with pytest.warns(UserWarning, match="Duplicate column name '(a|b)' detected"):
		result = left.inner_join(right, left_on=[left.a, left.b], right_on=[right.a, right.b])
	
	assert len(result) == 2
	assert list(result['val']) == ['x', 'z']
	assert list(result['data']) == ['p', 'q']


def test_join_key_validation_length_mismatch():
	"""Test that mismatched left_on/right_on lengths raise error"""
	left = Table({'id': [1, 2], 'date': ['a', 'b']})
	right = Table({'id': [2, 3]})
	
	with pytest.raises(ValueError, match="same length"):
		left.inner_join(right, left_on=['id', 'date'], right_on=['id'])


def test_join_key_validation_missing_column():
	"""Test that non-existent column names raise error"""
	left = Table({'id': [1, 2]})
	right = Table({'id': [2, 3]})
    
	from serif.errors import SerifKeyError
	with pytest.raises(SerifKeyError):
		left.inner_join(right, left_on='missing_col', right_on='id')


def test_join_with_computed_keys():
	"""Test that float columns are rejected as join keys"""
	left = Table({
		'price': [100.0, 200.0, 300.0],
		'name': ['A', 'B', 'C']
	})
	right = Table({
		'price_with_tax': [108.0, 216.0, 324.0],
		'quantity': [1, 2, 3]
	})
	
	# Float keys should be rejected due to precision issues
	with pytest.raises(SerifTypeError, match="Invalid join key dtype 'float'"):
		left.inner_join(right, left_on=left['price'] * 1.08, right_on=right['price_with_tax'])


def test_join_with_constant_vector():
	"""Test joining with a constant Vector (broadcast join pattern)"""
	left = Table({
		'id': [1, 2],
		'val': ['x', 'y']
	})
	right = Table({
		'flag': [1, 1, 1],
		'data': ['a', 'b', 'c']
	})
	
	# Create constant vector matching left table length
	constant_key = Vector([1, 1])
	
	# This creates a cartesian-like product where every left row matches every right row
	result = left.inner_join(right, left_on=constant_key, right_on=right['flag'], expect='many_to_many')
	
	assert len(result) == 6  # 2 left rows * 3 right rows


def test_join_sanitized_column_name_lookup():
	"""Test that string lookup works with both exact and sanitized names"""
	left = Table({
		'Customer ID': [1, 2, 3],
		'Name': ['Alice', 'Bob', 'Charlie']
	})
	right = Table({
		'CUSTOMER_ID': [2, 3, 4],
		'Age': [25, 30, 35]
	})
	
	# Should find column using sanitized name (case-insensitive) - duplicate 'customer_id' expected
	with pytest.warns(UserWarning, match="Duplicate column name 'customer_id' detected"):
		result = left.inner_join(right, left_on='customer_id', right_on='customer_id')
	
	assert len(result) == 2
	assert list(result['Name']) == ['Bob', 'Charlie']


def test_join_key_wrong_length_left():
	"""Test that left_on Vector with wrong length raises error"""
	left = Table({'id': [1, 2, 3]})
	right = Table({'id': [2, 3]})
	
	# Create Vector with wrong length
	wrong_length_key = Vector([1, 2])  # Length 2, but left table has 3 rows
	
	with pytest.raises(ValueError, match="Left join key.*has length 2.*left table has 3 rows"):
		left.inner_join(right, left_on=wrong_length_key, right_on=right.id)


def test_join_key_wrong_length_right():
	"""Test that right_on Vector with wrong length raises error"""
	left = Table({'id': [1, 2]})
	right = Table({'id': [2, 3, 4]})
	
	# Create Vector with wrong length
	wrong_length_key = Vector([1, 2])  # Length 2, but right table has 3 rows
	
	with pytest.raises(ValueError, match="Right join key.*has length 2.*right table has 3 rows"):
		left.inner_join(right, left_on=left.id, right_on=wrong_length_key)


def test_join_multi_key_computed():
	"""Test multi-key join with mix of columns and computed values"""
	left = Table({
		'year': [2023, 2023, 2024],
		'month': [1, 2, 1],
		'amount': [100, 200, 300]
	})
	right = Table({
		'period_code': [202301, 202302, 202401],
		'budget': [150, 250, 350]
	})
	
	# Compute YYYYMM code from year and month
	left_period = left.year * 100 + left.month
	
	result = left.inner_join(right, left_on=left_period, right_on=right.period_code)
	
	assert len(result) == 3
	assert list(result['amount']) == [100, 200, 300]
	assert list(result['budget']) == [150, 250, 350]




