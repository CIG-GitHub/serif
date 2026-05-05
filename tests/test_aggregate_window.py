import pytest
import warnings
from serif import Table
from serif import Vector


class TestAggregate:
	"""Tests for Table.aggregate() method"""
	
	def test_aggregate_single_partition_single_aggregation(self):
		"""Basic aggregation with one partition key and one aggregation"""
		table = Table({
			'customer': ['A', 'B', 'A', 'C', 'B', 'A'],
			'sales': [100, 200, 150, 300, 250, 175]
		})
		
		result = table.aggregate(
			over=table.customer,
			sum_over=table.sales
		)
		
		# Should have one row per customer
		assert len(result) == 3
		
		# Check aggregated values (order may vary)
		sales_sums = {result.customer[i]: result.sales_sum[i] for i in range(len(result))}
		assert sales_sums['A'] == 425  # 100 + 150 + 175
		assert sales_sums['B'] == 450  # 200 + 250
		assert sales_sums['C'] == 300
	
	def test_aggregate_multiple_partitions(self):
		"""Aggregate with multiple partition keys"""
		table = Table({
			'year': [2023, 2023, 2024, 2024, 2023, 2024],
			'month': [1, 2, 1, 2, 1, 1],
			'revenue': [100, 200, 150, 300, 50, 175]
		})
		
		result = table.aggregate(
			over=[table.year, table.month],
			sum_over=table.revenue
		)
		
		# Should have one row per (year, month) combination
		assert len(result) == 4  # (2023,1), (2023,2), (2024,1), (2024,2)
		
		# Find the sum for 2023, month 1
		for i in range(len(result)):
			if result.year[i] == 2023 and result.month[i] == 1:
				assert result.revenue_sum[i] == 150  # 100 + 50
				break
	
	def test_aggregate_multiple_aggregations(self):
		"""Multiple aggregation functions on same partition"""
		table = Table({
			'group': ['X', 'Y', 'X', 'Y', 'X'],
			'value': [10, 20, 30, 40, 50]
		})
		
		result = table.aggregate(
			over=table.group,
			sum_over=table.value,
			mean_over=table.value,
			min_over=table.value,
			max_over=table.value,
			count_over=table.value
		)
		
		assert len(result) == 2
		
		# Find group X
		for i in range(len(result)):
			if result.group[i] == 'X':
				assert result.value_sum[i] == 90  # 10 + 30 + 50
				assert result.value_mean[i] == 30  # 90 / 3
				assert result.value_min[i] == 10
				assert result.value_max[i] == 50
				assert result.value_count[i] == 3
				break
	
	def test_aggregate_with_none_values(self):
		"""Aggregations should handle None values correctly"""
		table = Table({
			'category': ['A', 'A', 'B', 'B'],
			'amount': [10, None, 20, 30]
		})
		
		result = table.aggregate(
			over=table.category,
			sum_over=table.amount,
			count_over=table.amount,
			mean_over=table.amount
		)
		
		# Find category A
		for i in range(len(result)):
			if result.category[i] == 'A':
				assert result.amount_sum[i] == 10  # None is excluded
				assert result.amount_count[i] == 1  # Only non-None counted
				assert result.amount_mean[i] == 10  # 10 / 1
				break
	
	def test_aggregate_stdev(self):
		"""Test standard deviation aggregation"""
		table = Table({
			'group': ['A', 'A', 'A', 'B', 'B'],
			'value': [2, 4, 6, 10, 20]
		})
		
		result = table.aggregate(
			over=table.group,
			stdev_over=table.value
		)
		
		# Find group A: values [2, 4, 6], mean=4, stdev=2
		for i in range(len(result)):
			if result.group[i] == 'A':
				assert abs(result.value_stdev[i] - 2.0) < 0.001
				break
	
	def test_aggregate_custom_apply(self):
		"""Test custom aggregation functions"""
		table = Table({
			'team': ['Red', 'Blue', 'Red', 'Blue'],
			'score': [10, 20, 30, 40]
		})
		
		# Custom function: product of all values
		def product(values):
			result = 1
			for v in values:
				if v is not None:
					result *= v
			return result
		
		result = table.aggregate(
			over=table.team,
			apply={'score_product': (table.score, product)}
		)
		
		# Find Red team
		for i in range(len(result)):
			if result.team[i] == 'Red':
				assert result.score_product[i] == 300  # 10 * 30
				break
	
	def test_aggregate_multiple_columns_same_aggregation(self):
		"""Aggregate multiple columns with same function"""
		table = Table({
			'region': ['North', 'South', 'North'],
			'sales': [100, 200, 150],
			'costs': [60, 120, 90]
		})
		
		result = table.aggregate(
			over=table.region,
			sum_over=[table.sales, table.costs]
		)
		
		# Find North
		for i in range(len(result)):
			if result.region[i] == 'North':
				assert result.sales_sum[i] == 250  # 100 + 150
				assert result.costs_sum[i] == 150  # 60 + 90
				break
	
	def test_aggregate_name_deduplication(self):
		"""Column names should be deduplicated"""
		table = Table({
			'id': [1, 2, 1, 2],
			'value': [10, 20, 30, 40]
		})
		
		result = table.aggregate(
			over=table.id,
			sum_over=[table.value, table.value]  # Same column twice
		)
		
		# Should have value_sum and value_sum2 (deduplicated)
		assert 'value_sum' in [col._name for col in result._storage]
		assert 'value_sum2' in [col._name for col in result._storage]


class TestWindow:
	"""Tests for Table.window() method"""
	
	def test_window_maintains_row_count(self):
		"""Window functions should return same number of rows"""
		table = Table({
			'customer': ['A', 'B', 'A', 'C', 'B', 'A'],
			'sales': [100, 200, 150, 300, 250, 175]
		})
		
		result = table.window(
			over=table.customer,
			sum_over=table.sales
		)
		
		# Should have same number of rows as input
		assert len(result) == 6
		assert len(result.customer) == 6
		assert len(result.sales_sum) == 6
	
	def test_window_repeats_aggregated_values(self):
		"""Aggregated values should repeat for each row in partition"""
		table = Table({
			'group': ['X', 'X', 'Y', 'Y', 'X'],
			'amount': [10, 20, 30, 40, 50]
		})
		
		result = table.window(
			over=table.group,
			sum_over=table.amount
		)
		
		# Group X appears at indices 0, 1, 4 with sum 80
		assert result.amount_sum[0] == 80
		assert result.amount_sum[1] == 80
		assert result.amount_sum[4] == 80
		
		# Group Y appears at indices 2, 3 with sum 70
		assert result.amount_sum[2] == 70
		assert result.amount_sum[3] == 70
	
	def test_window_multiple_partitions(self):
		"""Window with multiple partition keys"""
		table = Table({
			'year': [2023, 2023, 2024, 2024, 2023],
			'quarter': [1, 1, 1, 2, 1],
			'revenue': [100, 200, 150, 300, 50]
		})
		
		result = table.window(
			over=[table.year, table.quarter],
			sum_over=table.revenue
		)
		
		assert len(result) == 5
		
		# Rows 0, 1, 4 are (2023, Q1) with sum 350
		assert result.revenue_sum[0] == 350
		assert result.revenue_sum[1] == 350
		assert result.revenue_sum[4] == 350
		
		# Row 2 is (2024, Q1) with sum 150
		assert result.revenue_sum[2] == 150
		
		# Row 3 is (2024, Q2) with sum 300
		assert result.revenue_sum[3] == 300
	
	def test_window_multiple_aggregations(self):
		"""Multiple window functions simultaneously"""
		table = Table({
			'category': ['A', 'B', 'A', 'B'],
			'value': [10, 20, 30, 40]
		})
		
		result = table.window(
			over=table.category,
			sum_over=table.value,
			mean_over=table.value,
			count_over=table.value
		)
		
		# Category A at indices 0, 2
		assert result.value_sum[0] == 40
		assert result.value_mean[0] == 20
		assert result.value_count[0] == 2
		
		assert result.value_sum[2] == 40
		assert result.value_mean[2] == 20
		assert result.value_count[2] == 2
	
	def test_window_running_total_example(self):
		"""Practical example: running total per customer"""
		table = Table({
			'customer_id': [101, 102, 101, 101, 102],
			'order_amount': [50, 100, 75, 25, 150]
		})
		
		# Get total amount per customer repeated for each order
		result = table.window(
			over=table.customer_id,
			sum_over=table.order_amount
		)
		
		# Customer 101 has total 150 (50+75+25) across 3 orders
		for i in range(len(result)):
			if result.customer_id[i] == 101:
				assert result.order_amount_sum[i] == 150
		
		# Customer 102 has total 250 (100+150) across 2 orders
		for i in range(len(result)):
			if result.customer_id[i] == 102:
				assert result.order_amount_sum[i] == 250
	
	def test_window_custom_apply(self):
		"""Custom window function"""
		table = Table({
			'team': ['A', 'B', 'A', 'B'],
			'score': [10, 20, 30, 40]
		})
		
		def product(values):
			result = 1
			for v in values:
				if v is not None:
					result *= v
			return result
		
		result = table.window(
			over=table.team,
			apply={'score_product': (table.score, product)}
		)
		
		# Team A at indices 0, 2: product is 300
		assert result.score_product[0] == 300
		assert result.score_product[2] == 300
		
		# Team B at indices 1, 3: product is 800
		assert result.score_product[1] == 800
		assert result.score_product[3] == 800
	
	def test_window_with_none_values(self):
		"""Window functions should handle None correctly"""
		table = Table({
			'group': ['X', 'X', 'Y', 'Y'],
			'amount': [10, None, 20, 30]
		})
		
		result = table.window(
			over=table.group,
			sum_over=table.amount,
			count_over=table.amount
		)
		
		# Group X: sum is 10, count is 1 (None excluded)
		assert result.amount_sum[0] == 10
		assert result.amount_count[0] == 1
		assert result.amount_sum[1] == 10
		assert result.amount_count[1] == 1
		
		# Group Y: sum is 50, count is 2
		assert result.amount_sum[2] == 50
		assert result.amount_count[2] == 2
	
	def test_window_stdev(self):
		"""Window standard deviation"""
		table = Table({
			'category': ['A', 'A', 'A', 'B', 'B'],
			'value': [2, 4, 6, 10, 20]
		})
		
		result = table.window(
			over=table.category,
			stdev_over=table.value
		)
		
		# Category A: stdev is 2
		for i in range(3):
			assert abs(result.value_stdev[i] - 2.0) < 0.001


class TestAggregateWindowEdgeCases:
	"""Edge cases and error conditions"""
	
	def test_aggregate_wrong_length_partition_key(self):
		"""Should raise error if partition key has wrong length"""
		table = Table({
			'a': [1, 2, 3],
			'b': [4, 5, 6]
		})
		
		bad_key = Vector([1, 2])  # Wrong length
		
		with pytest.raises(ValueError, match="Partition key.*has length 2.*table has 3 rows"):
			table.aggregate(over=bad_key, sum_over=table.b)
	
	def test_aggregate_wrong_length_aggregation_column(self):
		"""Should raise error if aggregation column has wrong length"""
		table = Table({
			'a': [1, 1, 2],
			'b': [4, 5, 6]
		})
		
		bad_col = Vector([10, 20])  # Wrong length
		
		with pytest.raises(ValueError, match="wrong length"):
			table.aggregate(over=table.a, sum_over=bad_col)
	
	def test_window_wrong_length_partition_key(self):
		"""Window should raise error if partition key has wrong length"""
		table = Table({
			'a': [1, 2, 3],
			'b': [4, 5, 6]
		})
		
		bad_key = Vector([1, 2, 3, 4])  # Wrong length
		
		with pytest.raises(ValueError, match="Partition key.*has length 4.*table has 3 rows"):
			table.window(over=bad_key, sum_over=table.b)
	
	def test_window_wrong_length_aggregation_column(self):
		"""Window should raise error if aggregation column has wrong length"""
		table = Table({
			'a': [1, 1, 2],
			'b': [4, 5, 6]
		})
		
		bad_col = Vector([10])  # Wrong length
		
		with pytest.raises(ValueError, match="wrong length"):
			table.window(over=table.a, sum_over=bad_col)
	
	def test_aggregate_empty_table(self):
		"""Aggregate on empty table should return empty result"""
		table = Table({
			'x': [],
			'y': []
		})
		
		result = table.aggregate(over=table.x, sum_over=table.y)
		assert len(result) == 0
	
	def test_window_empty_table(self):
		"""Window on empty table should return empty result"""
		table = Table({
			'x': [],
			'y': []
		})
		
		result = table.window(over=table.x, sum_over=table.y)
		assert len(result) == 0


	def test_aggregate_over_no_warnings_and_correct_keys(self):
		# Create a small table with year/month partition keys
		year = Vector([2020, 2020, 2021, 2021], name='year')
		month = Vector([1, 2, 1, 2], name='month')
		val = Vector([10, 20, 30, 40], name='val')

		table = Table([year, month, val])

		# Capture warnings
		with warnings.catch_warnings(record=True) as w:
			warnings.simplefilter("always")
			res = table.aggregate(over=[year, month], sum_over=val)

		# No warnings should have been raised
		assert len(w) == 0, f"Unexpected warnings: {[str(x.message) for x in w]}"

		# Result should have 4 rows (unique year,month pairs)
		assert len(res) == 4

		# Partition key columns should match unique combinations in insertion order
		res_year = res['year']
		res_month = res['month']
		expected_keys = list({(year[i], month[i]) for i in range(len(year))})
		actual_keys = list(zip(res_year._storage, res_month._storage))
		assert set(actual_keys) == set(expected_keys)




