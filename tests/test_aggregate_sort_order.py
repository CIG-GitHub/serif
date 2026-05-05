"""Test that aggregate preserves order of first appearance (stable sort behavior)."""
import pytest
from serif import Vector
from serif.table import Table


class TestAggregateSortOrder:
	def test_aggregate_preserves_first_appearance_order(self):
		categories = Vector(['B', 'A', 'C', 'B', 'A'], name='category')
		values = Vector([10, 20, 30, 15, 25], name='value')
		table = Table([categories, values])
		result = table.aggregate(
			groupby=table.category,
			aggregations={'value_sum': table.value.sum}
		)
		assert list(result.category) == ['B', 'A', 'C']
		assert list(result.value_sum) == [25, 45, 30]

	def test_aggregate_multiple_partition_keys_preserves_order(self):
		col1 = Vector([2, 1, 2, 1], name='num')
		col2 = Vector(['X', 'Y', 'Y', 'X'], name='letter')
		values = Vector([100, 200, 300, 400], name='val')
		table = Table([col1, col2, values])
		result = table.aggregate(
			groupby=[table.num, table.letter],
			aggregations={'val_sum': table.val.sum}
		)
		assert list(result.num) == [2, 1, 2, 1]
		assert list(result.letter) == ['X', 'Y', 'Y', 'X']
		assert list(result.val_sum) == [100, 200, 300, 400]

	def test_aggregate_with_interleaved_groups(self):
		groups = Vector(['A', 'B', 'A', 'B', 'A', 'C', 'B'], name='group')
		values = Vector([1, 2, 3, 4, 5, 6, 7], name='value')
		table = Table([groups, values])
		result = table.aggregate(
			groupby=table.group,
			aggregations={'value_sum': table.value.sum}
		)
		assert list(result.group) == ['A', 'B', 'C']
		assert list(result.value_sum) == [9, 13, 6]

	def test_aggregate_numeric_groups_preserve_order_not_value_order(self):
		numbers = Vector([3, 1, 2, 3, 1], name='num')
		values = Vector([10, 20, 30, 40, 50], name='val')
		table = Table([numbers, values])
		result = table.aggregate(
			groupby=table.num,
			aggregations={'val_sum': table.val.sum}
		)
		assert list(result.num) == [3, 1, 2]
		assert list(result.val_sum) == [50, 70, 30]

	def test_aggregate_with_none_values_in_groups(self):
		groups = Vector(['A', None, 'B', None, 'A'], name='group')
		values = Vector([1, 2, 3, 4, 5], name='value')
		table = Table([groups, values])
		result = table.aggregate(
			groupby=table.group,
			aggregations={'value_sum': table.value.sum}
		)
		assert list(result.group) == ['A', None, 'B']
		assert list(result.value_sum) == [6, 6, 3]




