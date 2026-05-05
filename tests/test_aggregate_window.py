import pytest
import warnings
from serif import Table
from serif import Vector


class TestAggregate:
	"""Tests for Table.aggregate() method"""
	
	def test_aggregate_single_partition_single_aggregation(self):
		table = Table({
			'customer': ['A', 'B', 'A', 'C', 'B', 'A'],
			'sales': [100, 200, 150, 300, 250, 175]
		})
		
		result = table.aggregate(
			groupby=table.customer,
			aggregations={'sales_sum': table.sales.sum}
		)
		
		assert len(result) == 3
		sales_sums = {result.customer[i]: result.sales_sum[i] for i in range(len(result))}
		assert sales_sums['A'] == 425
		assert sales_sums['B'] == 450
		assert sales_sums['C'] == 300
	
	def test_aggregate_multiple_partitions(self):
		table = Table({
			'year': [2023, 2023, 2024, 2024, 2023, 2024],
			'month': [1, 2, 1, 2, 1, 1],
			'revenue': [100, 200, 150, 300, 50, 175]
		})
		result = table.aggregate(
			groupby=[table.year, table.month],
			aggregations={'revenue_sum': table.revenue.sum}
		)
		assert len(result) == 4
		for i in range(len(result)):
			if result.year[i] == 2023 and result.month[i] == 1:
				assert result.revenue_sum[i] == 150
				break
	
	def test_aggregate_multiple_aggregations(self):
		table = Table({
			'group': ['X', 'Y', 'X', 'Y', 'X'],
			'value': [10, 20, 30, 40, 50]
		})
		result = table.aggregate(
			groupby=table.group,
			aggregations={
				'value_sum':   table.value.sum,
				'value_mean':  table.value.mean,
				'value_min':   table.value.min,
				'value_max':   table.value.max,
				'value_count': table.value.count,
			}
		)
		assert len(result) == 2
		for i in range(len(result)):
			if result.group[i] == 'X':
				assert result.value_sum[i] == 90
				assert result.value_mean[i] == 30
				assert result.value_min[i] == 10
				assert result.value_max[i] == 50
				assert result.value_count[i] == 3
				break
	
	def test_aggregate_with_none_values(self):
		table = Table({
			'category': ['A', 'A', 'B', 'B'],
			'amount': [10, None, 20, 30]
		})
		result = table.aggregate(
			groupby=table.category,
			aggregations={
				'amount_sum':   table.amount.sum,
				'amount_count': table.amount.count,
				'amount_mean':  table.amount.mean,
			}
		)
		for i in range(len(result)):
			if result.category[i] == 'A':
				assert result.amount_sum[i] == 10
				assert result.amount_count[i] == 1
				assert result.amount_mean[i] == 10
				break
	
	def test_aggregate_stdev(self):
		table = Table({
			'group': ['A', 'A', 'A', 'B', 'B'],
			'value': [2, 4, 6, 10, 20]
		})
		result = table.aggregate(
			groupby=table.group,
			aggregations={'value_stdev': table.value.stdev}
		)
		for i in range(len(result)):
			if result.group[i] == 'A':
				assert abs(result.value_stdev[i] - 2.0) < 0.001
				break
	
	def test_aggregate_custom_callable(self):
		table = Table({
			'team': ['Red', 'Blue', 'Red', 'Blue'],
			'score': [10, 20, 30, 40]
		})
		def product(g):
			result = 1
			for v in g.score:
				if v is not None:
					result *= v
			return result
		
		result = table.aggregate(
			groupby=table.team,
			aggregations={'score_product': product}
		)
		for i in range(len(result)):
			if result.team[i] == 'Red':
				assert result.score_product[i] == 300
				break
	
	def test_aggregate_multiple_columns_same_aggregation(self):
		table = Table({
			'region': ['North', 'South', 'North'],
			'sales': [100, 200, 150],
			'costs': [60, 120, 90]
		})
		result = table.aggregate(
			groupby=table.region,
			aggregations={
				'sales_sum': table.sales.sum,
				'costs_sum': table.costs.sum,
			}
		)
		for i in range(len(result)):
			if result.region[i] == 'North':
				assert result.sales_sum[i] == 250
				assert result.costs_sum[i] == 150
				break
	
	def test_aggregate_groupby_only(self):
		"""aggregate with no aggregations returns just the unique keys"""
		table = Table({'x': [1, 2, 1, 2], 'y': [10, 20, 30, 40]})
		result = table.aggregate(groupby=table.x)
		assert len(result) == 2


class TestWindow:
	"""Tests for Table.window() method"""

	def test_window_maintains_row_count(self):
		"""Window functions should return same number of rows"""
		table = Table({
			'customer': ['A', 'B', 'A', 'C', 'B', 'A'],
			'sales': [100, 200, 150, 300, 250, 175]
		})
		result = table.window(
			groupby=table.customer,
			aggregations={'sales_sum': table.sales.sum}
		)
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
			groupby=table.group,
			aggregations={'amount_sum': table.amount.sum}
		)
		assert result.amount_sum[0] == 80
		assert result.amount_sum[1] == 80
		assert result.amount_sum[4] == 80
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
			groupby=[table.year, table.quarter],
			aggregations={'revenue_sum': table.revenue.sum}
		)
		assert len(result) == 5
		assert result.revenue_sum[0] == 350
		assert result.revenue_sum[1] == 350
		assert result.revenue_sum[4] == 350
		assert result.revenue_sum[2] == 150
		assert result.revenue_sum[3] == 300

	def test_window_multiple_aggregations(self):
		"""Multiple window functions simultaneously"""
		table = Table({
			'category': ['A', 'B', 'A', 'B'],
			'value': [10, 20, 30, 40]
		})
		result = table.window(
			groupby=table.category,
			aggregations={
				'value_sum':   table.value.sum,
				'value_mean':  table.value.mean,
				'value_count': table.value.count,
			}
		)
		assert result.value_sum[0] == 40
		assert result.value_mean[0] == 20.0
		assert result.value_count[0] == 2
		assert result.value_sum[2] == 40
		assert result.value_mean[2] == 20.0
		assert result.value_count[2] == 2

	def test_window_running_total_example(self):
		"""Practical example: running total per customer"""
		table = Table({
			'customer_id': [101, 102, 101, 101, 102],
			'order_amount': [50, 100, 75, 25, 150]
		})
		result = table.window(
			groupby=table.customer_id,
			aggregations={'order_amount_sum': table.order_amount.sum}
		)
		for i in range(len(result)):
			if result.customer_id[i] == 101:
				assert result.order_amount_sum[i] == 150
		for i in range(len(result)):
			if result.customer_id[i] == 102:
				assert result.order_amount_sum[i] == 250

	def test_window_custom_callable(self):
		"""Custom window function via callable-receives-Table"""
		table = Table({
			'team': ['A', 'B', 'A', 'B'],
			'score': [10, 20, 30, 40]
		})

		def product(g):
			result = 1
			for v in g.score:
				if v is not None:
					result *= v
			return result

		result = table.window(
			groupby=table.team,
			aggregations={'score_product': product}
		)
		assert result.score_product[0] == 300
		assert result.score_product[2] == 300
		assert result.score_product[1] == 800
		assert result.score_product[3] == 800

	def test_window_with_none_values(self):
		"""Window functions should handle None correctly"""
		table = Table({
			'group': ['X', 'X', 'Y', 'Y'],
			'amount': [10, None, 20, 30]
		})
		result = table.window(
			groupby=table.group,
			aggregations={
				'amount_sum':   table.amount.sum,
				'amount_count': table.amount.count,
			}
		)
		assert result.amount_sum[0] == 10
		assert result.amount_count[0] == 1
		assert result.amount_sum[1] == 10
		assert result.amount_count[1] == 1
		assert result.amount_sum[2] == 50
		assert result.amount_count[2] == 2

	def test_window_stdev(self):
		"""Window standard deviation"""
		table = Table({
			'category': ['A', 'A', 'A', 'B', 'B'],
			'value': [2, 4, 6, 10, 20]
		})
		result = table.window(
			groupby=table.category,
			aggregations={'value_stdev': table.value.stdev}
		)
		for i in range(3):
			assert abs(result.value_stdev[i] - 2.0) < 0.001


class TestAggregateWindowEdgeCases:
	"""Edge cases and error conditions"""
	
	def test_aggregate_wrong_length_partition_key(self):
		table = Table({'a': [1, 2, 3], 'b': [4, 5, 6]})
		bad_key = Vector([1, 2])
		with pytest.raises(ValueError, match="groupby key.*has length 2.*table has 3 rows"):
			table.aggregate(groupby=bad_key)
	
	def test_aggregate_wrong_length_aggregation_column(self):
		table = Table({'a': [1, 1, 2], 'b': [4, 5, 6]})
		bad_col = Vector([10, 20])
		with pytest.raises(ValueError, match="vector length.*!= table length"):
			table.aggregate(
				groupby=table.a,
				aggregations={'x': bad_col.sum}
			)
	
	def test_window_wrong_length_partition_key(self):
		"""Window should raise error if partition key has wrong length"""
		table = Table({
			'a': [1, 2, 3],
			'b': [4, 5, 6]
		})
		bad_key = Vector([1, 2, 3, 4])  # Wrong length
		with pytest.raises(ValueError, match="Partition key.*has length 4.*table has 3 rows"):
			table.window(groupby=bad_key)
	
	def test_window_wrong_length_aggregation_column(self):
		"""Window should raise error if aggregation column has wrong length"""
		table = Table({
			'a': [1, 1, 2],
			'b': [4, 5, 6]
		})
		bad_col = Vector([10])  # Wrong length
		with pytest.raises(ValueError, match="vector length.*!= table length"):
			table.window(
				groupby=table.a,
				aggregations={'x': bad_col.sum}
			)
	
	def test_aggregate_empty_table(self):
		table = Table({'x': [], 'y': []})
		result = table.aggregate(groupby=table.x)
		assert len(result) == 0

	def test_aggregate_no_groupby_whole_table(self):
		table = Table({'sales': [100, 200, 300], 'price': [10.0, 20.0, 30.0]})
		result = table.aggregate(aggregations={
			'total_sales': table.sales.sum,
			'avg_price':   table.price.mean,
			'n':           table.sales.count,
		})
		assert len(result) == 1
		assert result.total_sales[0] == 600
		assert result.avg_price[0] == 20.0
		assert result.n[0] == 3

	def test_aggregate_no_groupby_positional(self):
		table = Table({'x': [1, 2, 3]})
		result = table.aggregate({'x_sum': table.x.sum})
		assert len(result) == 1
		assert result.x_sum[0] == 6

	def test_window_empty_table(self):
		table = Table({'x': [], 'y': []})
		result = table.window(groupby=table.x)
		assert len(result) == 0


	def test_aggregate_over_no_warnings_and_correct_keys(self):
		year = Vector([2020, 2020, 2021, 2021], name='year')
		month = Vector([1, 2, 1, 2], name='month')
		val = Vector([10, 20, 30, 40], name='val')
		table = Table([year, month, val])

		with warnings.catch_warnings(record=True) as w:
			warnings.simplefilter("always")
			res = table.aggregate(
				groupby=[table.year, table.month],
				aggregations={'val_sum': table.val.sum}
			)

		assert len(w) == 0, f"Unexpected warnings: {[str(x.message) for x in w]}"
		assert len(res) == 4
		expected_keys = {(year[i], month[i]) for i in range(len(year))}
		actual_keys = set(zip(res['year']._storage, res['month']._storage))
		assert actual_keys == expected_keys




