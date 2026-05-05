"""Test that aggregate preserves order of first appearance (stable sort behavior)."""
import pytest
from serif import Vector
from serif.table import Table


class TestAggregateSortOrder:
	def test_aggregate_preserves_first_appearance_order(self):
		"""Verify that aggregate results are ordered by first appearance of each group."""
		# Create data where groups appear in specific order: B, A, C, B, A
		categories = Vector(['B', 'A', 'C', 'B', 'A'], name='category')
		values = Vector([10, 20, 30, 15, 25], name='value')
		
		table = Table([categories, values])
		result = table.aggregate(over=table.category, sum_over=table.value)
		
		# Groups should appear in order of first appearance: B, A, C
		assert list(result.category) == ['B', 'A', 'C']
		assert list(result.value_sum) == [25, 45, 30]  # B: 10+15, A: 20+25, C: 30

	def test_aggregate_multiple_partition_keys_preserves_order(self):
		"""Test order preservation with multiple partition keys."""
		# Data with groups appearing as: (2, 'X'), (1, 'Y'), (2, 'Y'), (1, 'X')
		col1 = Vector([2, 1, 2, 1], name='num')
		col2 = Vector(['X', 'Y', 'Y', 'X'], name='letter')
		values = Vector([100, 200, 300, 400], name='val')
		
		table = Table([col1, col2, values])
		result = table.aggregate(over=[table.num, table.letter], sum_over=table.val)
		
		# Groups should be: (2, 'X'), (1, 'Y'), (2, 'Y'), (1, 'X')
		assert list(result.num) == [2, 1, 2, 1]
		assert list(result.letter) == ['X', 'Y', 'Y', 'X']
		assert list(result.val_sum) == [100, 200, 300, 400]

	def test_aggregate_with_interleaved_groups(self):
		"""Test with groups that appear multiple times in interleaved fashion."""
		# Pattern: A, B, A, B, A, C, B
		groups = Vector(['A', 'B', 'A', 'B', 'A', 'C', 'B'], name='group')
		values = Vector([1, 2, 3, 4, 5, 6, 7], name='value')
		
		table = Table([groups, values])
		result = table.aggregate(over=table.group, sum_over=table.value)
		
		# First appearance order: A (index 0), B (index 1), C (index 5)
		assert list(result.group) == ['A', 'B', 'C']
		assert list(result.value_sum) == [9, 13, 6]  # A: 1+3+5, B: 2+4+7, C: 6

	def test_aggregate_numeric_groups_preserve_order_not_value_order(self):
		"""Verify numeric groups are NOT sorted by value, but by first appearance."""
		# Numbers appearing as: 3, 1, 2, 3, 1
		numbers = Vector([3, 1, 2, 3, 1], name='num')
		values = Vector([10, 20, 30, 40, 50], name='val')
		
		table = Table([numbers, values])
		result = table.aggregate(over=table.num, sum_over=table.val)
		
		# Should be in first-appearance order: 3, 1, 2 (NOT sorted as 1, 2, 3)
		assert list(result.num) == [3, 1, 2]
		assert list(result.val_sum) == [50, 70, 30]  # 3: 10+40, 1: 20+50, 2: 30

	def test_aggregate_with_none_values_in_groups(self):
		"""Test that None in partition keys maintains order."""
		groups = Vector(['A', None, 'B', None, 'A'], name='group')
		values = Vector([1, 2, 3, 4, 5], name='value')
		
		table = Table([groups, values])
		result = table.aggregate(over=table.group, sum_over=table.value)
		
		# First appearance: A (index 0), None (index 1), B (index 2)
		assert list(result.group) == ['A', None, 'B']
		assert list(result.value_sum) == [6, 6, 3]  # A: 1+5, None: 2+4, B: 3




