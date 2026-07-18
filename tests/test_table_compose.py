"""Tests for Vector >> operator building Tables"""
import warnings

import pytest
from serif import Vector


class TestMixedKindComposeIsSilent:
    """Composing columns of DIFFERENT kinds is normal table building and
    must not warn. The object-degrade warning used to fire spuriously here:
    _collect_and_infer ran scalar promotion over the column objects
    themselves before discovering the collection was a table."""

    def test_rshift_mixed_kinds_no_warning(self):
        q = Vector(range(10), name='n')
        with warnings.catch_warnings():
            warnings.simplefilter('error')
            t = q >> {'squares': q ** 2, 'strings': [f's{i}' for i in range(10)]}
        assert [c.schema().kind for c in t.cols()] == [int, int, str]

    def test_vector_of_mixed_columns_no_warning(self):
        with warnings.catch_warnings():
            warnings.simplefilter('error')
            t = Vector((Vector([1, 2]), Vector(['a', 'b'])))
        assert type(t).__name__ == 'Table'

    def test_genuine_vector_as_element_still_warns(self):
        # NOT a table: a Vector mixed with a scalar is a real object-dtype
        # degradation, and the deliberate signal must survive the fix.
        with pytest.warns(UserWarning, match='Degrading'):
            v = Vector([Vector([1, 2]), 5])
        assert v.schema().kind is object


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
