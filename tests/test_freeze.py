"""
freeze(): a one-way promise that an object never changes in place again.

- t.freeze() / v.freeze() return the same object; there is no unfreeze.
- Every in-place write, column replacement, rename, and batch() on a
  frozen object raises SerifTypeError, and the message names the fix:
  a private copy.
- Anything derived (copies, selections, compositions, results) is a new,
  unfrozen object. `>>=` rebinds the name to a new table, as with tuples.
- Freezing is ownership, not a separate flag: a frozen object's owner is
  the permanent FROZEN sentinel, and a column of a frozen table reports
  frozen through its owning table.
"""

import pickle

import pytest

from serif import Table, Vector, read_parquet
from serif.errors import SerifTypeError, SerifValueError
from serif._vector.mutation import FROZEN


def make_table():
    return Table({
        'a': [1, 2, 3],
        'f': [1.5, 2.5, 3.5],
        's': ['x', 'y', 'z'],
    })


TABLE_FROZEN = r"(?s)Table is frozen.*permanent.*t = t\.copy\(\)"
COLUMN_FROZEN = r"(?s)This column's Table is frozen.*v = v\.copy\(\)"
VECTOR_FROZEN = r"(?s)Vector is frozen.*permanent.*v = v\.copy\(\)"


# ---------------------------------------------------------------------------
# The API
# ---------------------------------------------------------------------------

def test_freeze_returns_self_and_reports_frozen():
    t = make_table()
    assert not t.frozen
    assert t.freeze() is t
    assert t.frozen


def test_freeze_is_idempotent():
    t = make_table().freeze()
    assert t.freeze() is t
    assert t.frozen


def test_there_is_no_unfreeze():
    for cls in (Vector, Table):
        assert not hasattr(cls, 'unfreeze')
        assert not hasattr(cls, 'thaw')
    with pytest.raises(AttributeError):
        make_table().frozen = False


def test_freezing_keeps_values_and_names():
    t = make_table()
    t.table_name = 'orders'
    t.freeze()
    assert t.column_names() == ['a', 'f', 's']
    assert t.table_name == 'orders'
    assert list(t.a) == [1, 2, 3]


# ---------------------------------------------------------------------------
# Every in-place route into a frozen Table raises
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('write', [
    lambda t: t.__setitem__((0, 'a'), 9),
    lambda t: t.__setitem__((t.a > 1, 'a'), 0),
    lambda t: t.__setitem__((0, slice(None)), [9, 9.5, 'q']),
    lambda t: t.__setitem__(('a', 0), 9),
    lambda t: t.__setitem__(['a', 'f'], 0),
    lambda t: setattr(t, 'a', [7, 8, 9]),
    lambda t: setattr(t, 'table_name', 'renamed'),
    lambda t: setattr(t, 'not_a_column', 1),
    lambda t: t.alias('renamed'),
], ids=[
    'cell', 'masked', 'row', 'column-first', 'column-list',
    'column-replace', 'table-name', 'new-attribute', 'alias',
])
def test_frozen_table_rejects_in_place_writes(write):
    t = make_table().freeze()
    with pytest.raises(SerifTypeError, match=TABLE_FROZEN):
        write(t)
    assert list(t.a) == [1, 2, 3]
    assert t.table_name is None


def test_augmented_column_assignment_raises():
    # t.a += 1 desugars to t.a = t.a + 1: a column replacement.
    t = make_table().freeze()
    with pytest.raises(SerifTypeError, match=TABLE_FROZEN):
        t.a += 1
    assert list(t.a) == [1, 2, 3]


def test_frozen_table_rejects_batch():
    t = make_table().freeze()
    with pytest.raises(SerifTypeError, match=TABLE_FROZEN):
        with t.batch():
            pass
    assert t._batch_edit is None


def test_batch_scope_created_before_freeze_cannot_enter():
    t = make_table()
    scope = t.batch()
    t.freeze()
    with pytest.raises(SerifTypeError, match=TABLE_FROZEN):
        with scope:
            pass


def test_cannot_freeze_inside_own_batch():
    t = make_table()
    with t.batch() as m:
        with pytest.raises(SerifValueError, match="inside its own batch"):
            m.freeze()
        m[0, 'a'] = 10
    assert not t.frozen
    assert list(t.a) == [10, 2, 3]


# ---------------------------------------------------------------------------
# Columns of a frozen Table
# ---------------------------------------------------------------------------

def test_frozen_table_columns_report_frozen():
    t = make_table()
    a = t.a
    t.freeze()
    assert a.frozen          # read out before freezing: same owner
    assert t.a.frozen
    assert all(col.frozen for col in t.cols())


@pytest.mark.parametrize('write', [
    lambda col: col.__setitem__(0, 5),
    lambda col: col.alias('renamed'),
    lambda col: setattr(col, 'vector_name', 'renamed'),
], ids=['setitem', 'alias', 'vector_name'])
def test_frozen_table_columns_reject_writes_with_the_freeze_message(write):
    t = make_table().freeze()
    with pytest.raises(SerifTypeError, match=COLUMN_FROZEN):
        write(t.a)


def test_categorical_column_of_frozen_table_rejects_writes():
    t = Table({'c': Vector(['lo', 'hi', 'lo']).categorize()}).freeze()
    with pytest.raises(SerifTypeError, match=COLUMN_FROZEN):
        t.c[0] = 'hi'
    with pytest.raises(SerifTypeError, match=TABLE_FROZEN):
        t[0, 'c'] = 'hi'


def test_row_of_frozen_table_stays_read_only():
    t = make_table().freeze()
    with pytest.raises(SerifTypeError):
        t[0]['a'] = 9


# ---------------------------------------------------------------------------
# Reads and derived values
# ---------------------------------------------------------------------------

def test_frozen_table_reads_normally():
    t = make_table().freeze()
    assert t.a.sum() == 6
    assert t.shape == (3, 3)
    assert [row.a for row in t] == [1, 2, 3]
    agg = t.aggregate(groupby=t.s, aggregations={'total': t.a.sum})
    assert list(agg.total) == [1, 2, 3]
    other = Table({'a': [2, 3], 'w': ['p', 'q']}).freeze()
    joined = t.inner_join(other, left_on='a', right_on='a')
    assert list(joined.w) == ['p', 'q']
    repr(t)


@pytest.mark.parametrize('derive', [
    lambda t: t.copy(),
    lambda t: t[t.a > 1],
    lambda t: t[0:2],
    lambda t: t[['a', 's']],
    lambda t: t >> {'b': t.a * 2},
    lambda t: t << t,
    lambda t: t.rename_columns({'a': 'z'}),
    lambda t: t.drop('f'),
    lambda t: t.sort_by(t.a, reverse=True),
    lambda t: t.drop('s').fillna(0),
], ids=[
    'copy', 'mask', 'slice', 'project', 'compose', 'append',
    'rename', 'drop', 'sort', 'fillna',
])
def test_derived_tables_are_unfrozen_and_independent(derive):
    t = make_table().freeze()
    u = derive(t)
    assert not u.frozen
    assert not any(col.frozen for col in u.cols())
    u[0, u.column_names()[0]] = 0
    assert list(t.a) == [1, 2, 3]


def test_copy_is_the_escape_hatch():
    t = make_table().freeze()
    c = t.copy()
    c[0, 'a'] = 99
    with c.batch() as m:
        m.a[1] = 98
    assert list(c.a) == [99, 98, 3]
    assert list(t.a) == [1, 2, 3]
    assert t.frozen


def test_shift_assign_rebinds_instead_of_mutating():
    t = make_table().freeze()
    shared = t
    t >>= {'b': t.a * 2}
    assert t is not shared
    assert not t.frozen
    assert shared.frozen
    assert shared.column_names() == ['a', 'f', 's']


def test_column_read_out_of_frozen_table_copies_to_a_mutable_vector():
    t = make_table().freeze()
    v = t.a.copy()
    assert not v.frozen
    v[0] = 99
    assert list(v) == [99, 2, 3]
    assert list(t.a) == [1, 2, 3]


# ---------------------------------------------------------------------------
# Lazy tables
# ---------------------------------------------------------------------------

def test_frozen_deferred_selection_freezes_columns_gathered_later():
    t = make_table()
    q = t[t.a > 1].freeze()
    a = q.a                  # gathered after the freeze
    assert a.frozen
    assert list(a) == [2, 3]
    with pytest.raises(SerifTypeError, match=COLUMN_FROZEN):
        a[0] = 0
    with pytest.raises(SerifTypeError, match=TABLE_FROZEN):
        q[0, 's'] = 'w'
    assert list(q.s) == ['y', 'z']


def test_frozen_lazy_parquet_table(tmp_path):
    path = str(tmp_path / 't.parquet')
    make_table().to_parquet(path)
    t = read_parquet(path).freeze()
    assert t.frozen
    assert t.a.frozen
    with pytest.raises(SerifTypeError, match=TABLE_FROZEN):
        t[0, 'a'] = 9
    recent = t[t.a >= 2]
    assert not recent.frozen
    assert list(recent.s) == ['y', 'z']


# ---------------------------------------------------------------------------
# Standalone Vectors
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('write', [
    lambda v: v.__setitem__(0, 9),
    lambda v: v.__setitem__(v > 1, 0),
    lambda v: v.__setitem__(slice(0, 2), [7, 8]),
    lambda v: v.alias('renamed'),
    lambda v: setattr(v, 'vector_name', 'renamed'),
], ids=['setitem', 'masked', 'slice', 'alias', 'vector_name'])
def test_frozen_vector_rejects_in_place_writes(write):
    v = Vector([1, 2, 3], name='v').freeze()
    with pytest.raises(SerifTypeError, match=VECTOR_FROZEN):
        write(v)
    assert list(v) == [1, 2, 3]
    assert v.vector_name == 'v'


def test_frozen_vector_derives_unfrozen_values():
    v = Vector([1, 2, 3]).freeze()
    for derived in (v.copy(), v + 1, v[v > 1], v[0:2], v.copy(name='x')):
        assert not derived.frozen
    w = v
    w += 1                   # no __iadd__: rebinds w only
    assert list(v) == [1, 2, 3]
    assert list(w) == [2, 3, 4]


def test_frozen_vector_enters_tables_as_a_mutable_snapshot():
    v = Vector([1, 2, 3]).freeze()
    t = Table({'a': v})
    t[0, 'a'] = 9
    t >>= {'b': v}
    t.b = v
    assert not t.a.frozen
    assert list(t.a) == [9, 2, 3]
    assert list(v) == [1, 2, 3]
    assert v.frozen


# ---------------------------------------------------------------------------
# A frozen column object inside an unfrozen Table never changes
# ---------------------------------------------------------------------------

def test_frozen_column_object_survives_owner_writes():
    t = make_table()
    a = t.a.freeze()
    t[0, 'a'] = 10
    assert list(a) == [1, 2, 3]
    assert list(t.a) == [10, 2, 3]
    assert a.frozen
    assert not t.a.frozen


def test_frozen_column_object_survives_batch():
    t = make_table()
    a = t.a.freeze()
    with t.batch() as m:
        m[0, 'a'] = 10
        m[1, 'a'] = 20
        m.f[0] = 0.0         # unfrozen columns still take the fast path
    assert list(a) == [1, 2, 3]
    assert a.frozen
    assert list(t.a) == [10, 20, 3]
    assert list(t.f) == [0.0, 2.5, 3.5]
    with pytest.raises(SerifTypeError, match=VECTOR_FROZEN):
        a[0] = 0


def test_untouched_frozen_column_stays_frozen_through_batch():
    t = make_table()
    a = t.a.freeze()
    with t.batch() as m:
        m[0, 'f'] = 0.0
        with pytest.raises(SerifTypeError, match=VECTOR_FROZEN):
            m.a[0] = 0
    assert t.a is a
    assert a.frozen


def test_column_frozen_inside_batch_is_never_written_in_place():
    t = make_table()
    with t.batch() as m:
        a = m.a.freeze()
        m[0, 'a'] = 10
    assert list(a) == [1, 2, 3]
    assert list(t.a) == [10, 2, 3]
    assert a.frozen


# ---------------------------------------------------------------------------
# Freezing is ownership
# ---------------------------------------------------------------------------

def test_freezing_is_ownership_not_a_flag():
    v = Vector([1, 2]).freeze()
    t = make_table().freeze()
    assert v._owner is FROZEN
    assert t._owner is FROZEN
    assert t.a._owner() is t
    assert not hasattr(Vector, '_frozen')


def test_frozen_sentinel_pickles_to_itself():
    assert pickle.loads(pickle.dumps(FROZEN)) is FROZEN
