"""Tests for Vector >> operator building Tables"""
import pytest
from serif import Vector


class TestConstructionThroughPipe:
    """Vector >> dict should produce a named Table"""

    def test_rshift_dict_builds_table(self):
        a = Vector([1, 2, 3], name='a')
        t = a >> {'b': a ** 2}
        assert type(t).__name__ == 'Table'
        cols = t.cols()
        assert cols[0]._name == 'a'
        assert cols[1]._name == 'b'
        assert list(cols[1]) == [1, 4, 9]

    def test_rshift_dict_row_iteration(self):
        a = Vector(range(5), name='a')
        t = a >> {'b': a ** 2}
        results = [row[0] + row[1] for row in t]
        assert results == [x + x ** 2 for x in range(5)]
