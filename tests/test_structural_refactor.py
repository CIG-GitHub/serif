"""
Pins for behavior changes that ride along with the structural refactor
(commit 1): dtype/backend preservation through sort/join/window, the
unified duplicate-name disambiguation rule, and is_na() derivation rules.
"""

import importlib.util
import warnings

import pytest

from serif import Vector, Table
from serif._vector import Schema
from serif._vector.categorical import _Category
from serif._vector.storage import ArrayStorage


# ---------------------------------------------------------------------------
# Vector has one canonical public class identity
# ---------------------------------------------------------------------------

def test_vector_public_module_is_canonical():
    from serif.vector import Vector as PublicVector
    from serif._vector import Vector as PrivateCompatibilityVector

    assert Vector is PublicVector
    assert Vector is PrivateCompatibilityVector
    assert Vector.__module__ == 'serif.vector'


def test_vector_backends_do_not_own_public_classes():
    from serif._vector import operators as semantic_ops
    from serif._vector import reductions as semantic_reductions
    from serif._vector import selection as semantic_selection
    from serif._vector._arrow import operators as arrow_ops
    from serif._vector._numpy import operators as numpy_ops
    from serif._vector._numpy import reductions as numpy_reductions
    from serif._vector._numpy import selection as numpy_selection
    from serif._vector._python import operators as python_ops
    from serif._vector._python import reductions as python_reductions
    from serif._vector._python import selection as python_selection

    for module in (
        semantic_ops,
        semantic_reductions,
        semantic_selection,
        arrow_ops,
        numpy_ops,
        numpy_reductions,
        numpy_selection,
        python_ops,
        python_reductions,
        python_selection,
    ):
        assert 'Vector' not in vars(module)
        assert 'Table' not in vars(module)


def test_legacy_accel_api_no_longer_dispatches_vector_operations():
    from serif._accel import api

    for name in (
        '_accel_binop',
        '_accel_compare',
        '_accel_invert',
        '_accel_logical',
        '_accel_reduce',
        '_accel_filter',
        '_accel_popcount',
        '_accel_take',
        '_accel_take_pad',
        '_take',
    ):
        assert not hasattr(api, name)


def test_legacy_accel_mask_module_is_removed():
    assert importlib.util.find_spec('serif._accel.mask') is None


# ---------------------------------------------------------------------------
# Table.sort_by preserves column backends and subclasses
# ---------------------------------------------------------------------------

def _cat_table():
    c = Vector(['b', 'a', 'b']).categorize(['b', 'a']).alias('cat')
    n = Vector([3, 1, 2], name='n')
    return Table([c, n])


def test_table_sort_preserves_categorical():
    t = _cat_table()
    assert isinstance(t['cat'], _Category)

    t2 = t.sort_by('n')

    col = t2['cat']
    assert isinstance(col, _Category), "sort_by must not demote _Category to _String"
    assert col.categories == ('b', 'a')
    assert list(col) == ['a', 'b', 'b']
    assert list(t2['n']) == [1, 2, 3]


def test_table_sort_preserves_int_arraystorage():
    t = Table({'x': [3, 1, 2], 'y': [1.0, 2.0, 3.0]})
    t2 = t.sort_by('x')
    assert type(t2['x']._storage).__name__ == 'ArrayStorage'
    assert list(t2['x']) == [1, 2, 3]
    assert list(t2['y']) == [2.0, 3.0, 1.0]


# ---------------------------------------------------------------------------
# Duplicate-name disambiguation: one rule shared by map, getitem, and repr
# ---------------------------------------------------------------------------

def test_duplicate_reserved_name_bracket_lookup_matches_column_map():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        t = Table([Vector([1], name='sum'), Vector([2], name='sum')])

    # The key stored in the column map must be resolvable via brackets.
    assert 'sum__1' in t._column_map
    assert t['sum__1'][0] == 2
    # Attribute access agrees.
    assert t.sum__1[0] == 2


def test_duplicate_plain_name_bracket_lookup():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        t = Table([Vector([1], name='total'), Vector([2], name='total')])

    assert 'total__1' in t._column_map
    assert t['total__1'][0] == 2


# ---------------------------------------------------------------------------
# Join results preserve source schemas (and backends) without re-inference
# ---------------------------------------------------------------------------

def test_left_join_right_columns_become_nullable():
    t1 = Table({'id': [1, 2, 3]})
    t2 = Table({'id': [1], 'v': [10]})

    j = t1.left_join(t2, 'id', 'id')

    assert list(j['v']) == [10, None, None]
    assert j['v'].schema() == Schema(int, True)
    # Left side keeps its exact schema.
    assert j['id'].schema() == Schema(int, False)


def test_inner_join_int_column_stays_arraystorage(tmp_path):
    t1 = Table({'id': [1, 2, 3], 'a': [10, 20, 30]})
    t2 = Table({'id': [2, 3], 'b': [200, 300]})

    j = t1.inner_join(t2, 'id', 'id')

    assert list(j['a']) == [20, 30]
    assert list(j['b']) == [200, 300]
    assert isinstance(j['b']._storage, ArrayStorage)

    # The join result must remain Parquet-writable (int columns need
    # ArrayStorage backing).
    p = str(tmp_path / 'join.parquet')
    j.to_parquet(p)
    back = Table.from_parquet(p)
    assert list(back['b']) == [200, 300]


def test_full_join_both_sides_nullable():
    t1 = Table({'id': [1, 2], 'a': [10, 20]})
    t2 = Table({'id': [2, 3], 'b': [200, 300]})

    j = t1.full_join(t2, 'id', 'id')

    assert j['a'].schema() == Schema(int, True)
    assert j['b'].schema() == Schema(int, True)
    assert list(j['id']) == [1, 2, None]
    assert list(j['a']) == [10, 20, None]
    assert list(j['b']) == [None, 200, 300]


# ---------------------------------------------------------------------------
# window() groupby keys keep their subclass
# ---------------------------------------------------------------------------

def test_window_key_stays_categorical():
    t = _cat_table()
    w = t.window('cat', {'total': t.n.sum})
    assert isinstance(w['cat'], _Category)
    assert list(w['total']) == [5, 1, 5]


# ---------------------------------------------------------------------------
# is_na(): derived, unnamed, plain bool Vector on every backend
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('data', [
    [1, None, 3],                 # int → ArrayStorage + mask
    [1.5, None, 2.5],             # float → ArrayStorage + mask
    ['a', None, 'c'],             # str → StringStorage + mask
    [1, 2, 3],                    # no nulls, no mask
])
def test_is_na_returns_unnamed_plain_bool_vector(data):
    v = Vector(data, name='src')
    m = v.is_na()
    assert m.vector_name is None, "derived vectors start unnamed (invariant 5)"
    assert type(m).__name__ == 'Vector'
    assert m.schema() == Schema(bool, False)
    assert list(m) == [x is None for x in data]
