"""
Deferred boolean-mask selection: `t[mask]` outside a batch() scope
returns a MaskedTable that gathers a column only when someone asks
for it.

The contract under test:

- Results are IDENTICAL to the old eager gather — which ran the very
  same per-column program, col[mask] — in values, nulls, names,
  schemas, and storage types, on both the accel and pure tiers.
- Laziness is real: single-column access gathers exactly that column;
  len / shape / column_names gather nothing.
- The capture is a frozen snapshot: writes to the source after deferral
  (owner-addressed or batch()) never show through, in any order.
- Everything unanticipated falls through materialization (the latch)
  and behaves as a plain Table; derived results are plain Tables — the
  deferred type never escapes as a result type.
"""

import warnings

import pytest

from serif import Table, Vector
from serif.table import MaskedTable
from serif.errors import SerifKeyError, SerifTypeError, SerifValueError
from serif._vector import selection as vector_selection
from serif._vector._numpy import selection as numpy_selection


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def eager(t, mask):
    """The old eager path, verbatim: gather every column through the
    same per-column program the deferred table runs on demand."""
    return Table(tuple(c[mask] for c in t.cols()))


def assert_same_table(result, expected):
    assert len(result) == len(expected)
    assert result.shape == expected.shape
    assert result.column_names() == expected.column_names()
    for rc, ec in zip(result.cols(), expected.cols()):
        assert rc.vector_name == ec.vector_name
        assert type(rc) is type(ec)
        assert type(rc._storage) is type(ec._storage)
        if ec.schema() is None:
            assert rc.schema() is None
        else:
            assert rc.schema().kind is ec.schema().kind
            assert rc.schema().nullable is ec.schema().nullable
        assert list(rc) == list(ec)


def make_table():
    return Table({
        'a': [1, 2, 3, 4, 5, 6],
        'b': [1.5, -2.0, 3.25, 4.0, 5.5, 6.75],
        's': ['x', 'y', 'z', 'x', 'y', 'z'],
        'n': [10, None, 30, None, 50, 60],
        'f': [True, False, True, None, False, True],
    })


MASKS = [
    ("predicate", lambda t: t.a > 3),
    ("literal_list", lambda t: [True, False, True, False, True, False]),
    ("nullable", lambda t: Vector([True, None, True, False, None, True])),
    ("all_true", lambda t: Vector([True] * 6)),
    ("all_false", lambda t: Vector([False] * 6)),
]


@pytest.fixture(params=['ambient', 'pure'])
def tier(request):
    """Run each test on the ambient tier (numpy when installed) AND with
    the accelerator forced off — deferral is independent of the tier."""
    if request.param == 'pure':
        saved = numpy_selection._USE_NUMPY
        numpy_selection._USE_NUMPY = False
        try:
            yield 'pure'
        finally:
            numpy_selection._USE_NUMPY = saved
    else:
        yield 'ambient'


# ---------------------------------------------------------------------------
# Equivalence with the eager path
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("mask_name,make_mask", MASKS, ids=[m[0] for m in MASKS])
def test_equivalence_with_eager(tier, mask_name, make_mask):
    t = make_table()
    assert_same_table(t[make_mask(t)], eager(t, make_mask(t)))


def test_masking_returns_deferred_subtype(tier):
    t = make_table()
    q = t[t.a > 3]
    assert type(q) is MaskedTable
    assert isinstance(q, Table)
    # list masks defer too
    assert type(t[[True] * 6]) is MaskedTable


def test_categorical_column_round_trip(tier):
    c = Vector(['lo', 'hi', 'lo', 'mid', 'hi', 'lo'], name='c')
    t = Table([Vector([1, 2, 3, 4, 5, 6], name='a'),
               c.categorize(['lo', 'mid', 'hi'])])
    mask = t.a > 2
    q = t[mask]
    assert type(q.c) is type(t.c)  # stays categorical through the gather
    assert_same_table(q, eager(t, mask))


def test_empty_source_table(tier):
    t = Table({'a': [], 'b': []})
    mask = t.a > 1
    q = t[mask]
    assert len(q) == 0
    assert q.shape == (0, 2)
    assert_same_table(q, eager(t, mask))


def test_iteration_matches_eager(tier):
    t = make_table()
    mask = t.a > 2
    q, e = t[mask], eager(t, mask)
    assert [tuple(row) for row in q] == [tuple(row) for row in e]


def test_repr_matches_eager(tier):
    t = make_table()
    mask = t.a > 2
    assert repr(t[mask]) == repr(eager(t, mask))


# ---------------------------------------------------------------------------
# Laziness: what gathers, what doesn't
# ---------------------------------------------------------------------------

def test_len_shape_names_gather_nothing(tier):
    t = make_table()
    q = t[t.a > 3]
    assert len(q) == 3
    assert q.shape == (3, 5)
    assert q.column_names() == ['a', 'b', 's', 'n', 'f']
    assert q._mat is None
    assert q._gathered == {}


def test_nullable_mask_popcount_excludes_nulls(tier):
    t = make_table()
    q = t[Vector([True, None, True, False, None, True])]
    assert len(q) == 3
    assert q._mat is None


def test_single_column_access_gathers_only_that_column(tier):
    t = make_table()
    q = t[t.a > 3]
    assert list(q.b) == [4.0, 5.5, 6.75]
    assert set(q._gathered) == {1}
    assert list(q['s']) == ['x', 'y', 'z']
    assert set(q._gathered) == {1, 2}
    assert q._mat is None


def test_gathered_column_is_cached(tier):
    t = make_table()
    q = t[t.a > 3]
    assert q.b is q.b  # same object, one gather — like eager t.b is t.b


def test_multi_column_selection_stays_deferred(tier):
    t = make_table()
    mask = t.a > 3
    q = t[mask]
    r = q[('a', 'b')]
    assert type(r) is Table
    assert_same_table(r, eager(t, mask)[('a', 'b')])
    assert q._mat is None
    assert set(q._gathered) == {0, 1}


def test_positional_cols_access_gathers_one(tier):
    t = make_table()
    q = t[t.a > 3]
    assert list(q.cols(1)) == [4.0, 5.5, 6.75]
    assert list(q.cols(-1)) == [None, False, True]
    assert set(q._gathered) == {1, 4}
    assert q._mat is None


def test_case_insensitive_and_unnamed_access(tier):
    t = Table([Vector([1, 2, 3], name='Amount'), Vector([4, 5, 6])])
    q = t[Vector([True, False, True])]
    assert list(q['amount']) == [1, 3]
    assert list(q.col1_) == [4, 6]
    assert q._mat is None


def test_duplicate_names_resolve_through_map(tier):
    with pytest.warns(UserWarning, match="Duplicate column name"):
        t = Table([Vector([1, 2, 3], name='x'),
                   Vector([4, 5, 6], name='x').alias('x')])
    q = t[Vector([True, False, True])]
    assert list(q.x__1) == [4, 6]
    assert q._mat is None


def test_missing_key_raises_without_materializing(tier):
    t = make_table()
    q = t[t.a > 3]
    with pytest.raises(SerifKeyError):
        q['nope']
    assert q._mat is None


def test_mask_length_mismatch_raises_eagerly(tier):
    t = make_table()
    with pytest.raises(SerifValueError, match="length mismatch"):
        t[Vector([True, False])]
    with pytest.raises(SerifValueError, match="length mismatch"):
        t[[True, False]]


# ---------------------------------------------------------------------------
# Snapshot semantics: `q = t[mask]` means "t as it was", forever
# ---------------------------------------------------------------------------

def test_owner_write_after_defer_does_not_leak(tier):
    t = make_table()
    q = t[t.a > 3]
    t[3, 'a'] = 999          # row 3 is a survivor (a=4)
    t[4, 'b'] = -1.0         # row 4 is a survivor (b=5.5)
    assert list(q.a) == [4, 5, 6]
    assert list(q.b) == [4.0, 5.5, 6.75]
    assert list(t.a) == [1, 2, 3, 999, 5, 6]  # the write itself landed


def test_write_between_gathers_does_not_leak(tier):
    t = make_table()
    q = t[t.a > 3]
    assert list(q.b) == [4.0, 5.5, 6.75]   # gathered BEFORE the write
    t[3, 'a'] = 999
    t[3, 'b'] = -1.0
    assert list(q.a) == [4, 5, 6]          # gathered AFTER — same snapshot
    assert list(q.b) == [4.0, 5.5, 6.75]


def test_batch_write_after_defer_does_not_leak(tier):
    t = make_table()
    q = t[t.a > 3]
    with t.batch() as m:
        m[3, 'a'] = 999
        m[5, 'n'] = 0
    assert list(q.a) == [4, 5, 6]
    assert list(q.n) == [None, 50, 60]


def test_column_replacement_after_defer_does_not_leak(tier):
    t = make_table()
    q = t[t.a > 3]
    t.a = [0, 0, 0, 0, 0, 0]
    assert list(q.a) == [4, 5, 6]


def test_source_alias_after_defer_is_rejected_and_keeps_capture_names(tier):
    t = make_table()
    q = t[t.a > 3]
    with pytest.raises(SerifTypeError, match="metadata is frozen"):
        t.a.alias('z')
    assert q.column_names() == ['a', 'b', 's', 'n', 'f']
    assert list(q.a) == [4, 5, 6]
    assert q._mat is None


def test_gathered_columns_are_frozen_like_eager(tier):
    t = make_table()
    mask = t.a > 3
    q, e = t[mask], eager(t, mask)
    with pytest.raises(SerifTypeError, match="owned by a Table"):
        e.a[0] = 0
    with pytest.raises(SerifTypeError, match="owned by a Table"):
        q.a[0] = 0


# ---------------------------------------------------------------------------
# batch() scopes: deferral keys off t._unlocked
# ---------------------------------------------------------------------------

def test_mask_inside_batch_scope_is_eager(tier):
    t = make_table()
    with t.batch() as m:
        m[0, 'a'] = 100
        r = m[m.a > 3]
        assert type(r) is Table          # not deferred: buffers are live
        assert list(r.a) == [100, 4, 5, 6]


# ---------------------------------------------------------------------------
# Materialization: the latch
# ---------------------------------------------------------------------------

def test_latch_releases_snapshot_and_behaves_plain(tier):
    t = make_table()
    mask = t.a > 3
    q = t[mask]
    repr(q)                              # any full access latches
    assert q._mat is not None
    assert q._captured is None           # snapshot released
    assert q._gathered is None
    assert_same_table(q, eager(t, mask))
    assert list(q.a) == [4, 5, 6]        # post-latch access still works


def test_chained_masks_materialize_the_first(tier):
    t = make_table()
    q1 = t[t.a > 1]
    m2 = q1.a > 3
    q2 = q1[m2]
    assert type(q2) is MaskedTable
    assert q1._mat is not None           # v1: the second mask latches q1
    assert_same_table(q2, eager(eager(t, t.a > 1), m2))


def test_derived_results_are_plain_tables(tier):
    t = make_table()
    q = t[t.a > 3]
    assert type(q.copy()) is Table
    assert type(q[0:2]) is Table
    assert type(q[('a', 'b')]) is Table


def test_row_access_materializes_correctly(tier):
    t = make_table()
    mask = t.a > 3
    q = t[mask]
    assert tuple(q[0]) == tuple(eager(t, mask)[0])


# ---------------------------------------------------------------------------
# Doctrine: read through the column, write through the table.
# On a deferred result the write latches first, then swaps as usual.
# ---------------------------------------------------------------------------

def test_owner_write_latches_then_swaps(tier):
    t = make_table()
    q = t[t.a > 3]
    v = q.a                      # handed out before the write
    q[0, 'a'] = 99               # row 0 OF THE SELECTION (source row 3)
    assert q._mat is not None    # the write materialized the selection
    assert list(q.a) == [99, 5, 6]
    assert list(v) == [4, 5, 6]  # swap-on-write: read-outs are stable
    assert list(t.a) == [1, 2, 3, 4, 5, 6]  # source untouched


def test_owner_write_mask_addressed(tier):
    t = make_table()
    mask = t.a > 3
    q, e = t[mask], eager(t, mask)
    q[q.a > 4, 'a'] = 0
    e[e.a > 4, 'a'] = 0
    assert_same_table(q, e)


def test_column_replacement_write_on_deferred(tier):
    t = make_table()
    q = t[t.a > 3]
    q.a = [7, 8, 9]
    assert list(q.a) == [7, 8, 9]
    assert list(t.a) == [1, 2, 3, 4, 5, 6]


def test_vector_addressed_write_raises_post_latch(tier):
    t = make_table()
    q = t[t.a > 3]
    repr(q)  # latch
    with pytest.raises(SerifTypeError, match="owned by a Table"):
        q.a[0] = 0


def test_batch_on_deferred_table(tier):
    t = make_table()
    q = t[t.a > 3]
    with q.batch() as m:
        m[0, 'a'] = 40           # point write, table-addressed
        m.a[1] = 50              # vector-addressed works on thawed columns
    assert list(q.a) == [40, 50, 6]
    assert list(t.a) == [1, 2, 3, 4, 5, 6]   # source untouched
    with pytest.raises(SerifTypeError, match="owned by a Table"):
        q.a[0] = 0               # refrozen on scope exit


def test_mask_inside_deferred_batch_scope_is_eager(tier):
    t = make_table()
    q = t[t.a > 3]
    with q.batch() as m:
        r = m[m.a > 4]
        assert type(r) is Table
        assert list(r.a) == [5, 6]


def test_redefer_after_write_snapshots_current_state(tier):
    t = make_table()
    q = t[t.a > 3]
    q[0, 'a'] = 99
    q2 = q[q.a > 5]              # defers on the WRITTEN state of q
    assert type(q2) is MaskedTable
    assert list(q2.a) == [99, 6]
    q[1, 'a'] = 0                # later writes to q don't reach q2
    assert list(q2.a) == [99, 6]


# ---------------------------------------------------------------------------
# Fallthrough: everything downstream of the latch behaves as a plain Table
# ---------------------------------------------------------------------------

def test_sort_by_matches_eager(tier):
    t = make_table()
    mask = t.a > 1
    q, e = t[mask], eager(t, mask)
    qs, es = q.sort_by('a', reverse=True), e.sort_by('a', reverse=True)
    assert type(qs) is Table
    assert_same_table(qs, es)
    assert_same_table(q.sort_by('n', na_last=False),
                      e.sort_by('n', na_last=False))


def test_join_with_deferred_left_side(tier):
    t = make_table()
    lookup = Table({'s': ['x', 'y', 'z'], 'rank': [1, 2, 3]})
    mask = t.a > 2
    q, e = t[mask], eager(t, mask)
    qj = q.left_join(lookup, 's', 's')
    assert type(qj) is Table
    assert_same_table(qj, e.left_join(lookup, 's', 's'))


def test_join_with_deferred_right_side(tier):
    t = make_table()
    lookup = Table({'s': ['x', 'y', 'z'], 'rank': [1, 2, 3]})
    mask = t.a > 3   # survivors' s = ['x', 'y', 'z'] — unique right keys
    qj = lookup.left_join(t[mask], 's', 's')
    ej = lookup.left_join(eager(t, mask), 's', 's')
    assert type(qj) is Table
    assert_same_table(qj, ej)


def test_aggregate_matches_eager(tier):
    t = make_table()
    mask = t.a > 1
    q, e = t[mask], eager(t, mask)
    qa = q.aggregate(groupby=q.s, aggregations={'total': q.a.sum})
    ea = e.aggregate(groupby=e.s, aggregations={'total': e.a.sum})
    assert type(qa) is Table
    assert_same_table(qa, ea)
    assert_same_table(
        q.aggregate(groupby='s', aggregations={'hi': q.b.max}),
        e.aggregate(groupby='s', aggregations={'hi': e.b.max}))


def test_transpose_matches_eager(tier):
    t = Table({'a': [1, 2, 3, 4], 'b': [5, 6, 7, 8]})
    mask = t.a > 1
    qt, et = t[mask].T, eager(t, mask).T
    assert type(qt) is Table
    assert [list(c) for c in qt.cols()] == [list(c) for c in et.cols()]


def test_to_dict_matches_eager(tier):
    t = make_table()
    mask = t.a > 3
    assert t[mask].to_dict() == eager(t, mask).to_dict()


def test_compose_adds_column(tier):
    t = make_table()
    mask = t.a > 3
    q, e = t[mask], eager(t, mask)
    qc = q >> {'w': [7, 8, 9]}
    assert type(qc) is Table
    assert_same_table(qc, e >> {'w': [7, 8, 9]})


def test_drop_and_rename_are_plain_and_nonmutating(tier):
    t = make_table()
    mask = t.a > 3
    q, e = t[mask], eager(t, mask)
    qd = q.drop('a')
    assert type(qd) is Table
    assert_same_table(qd, e.drop('a'))
    qr = q.rename({'a': 'z'})
    assert type(qr) is Table
    assert qr.column_names() == ['z', 'b', 's', 'n', 'f']
    assert q.column_names() == ['a', 'b', 's', 'n', 'f']  # non-mutating


def test_arithmetic_matches_eager(tier):
    t = Table({'a': [1, 2, 3, 4], 'b': [5.0, 6.0, 7.0, 8.0]})
    mask = t.a > 1
    q, e = t[mask], eager(t, mask)
    assert type(q + 1) is Table
    assert_same_table(q + 1, e + 1)
    assert_same_table(q * 2, e * 2)


def test_schema_view_matches_eager(tier):
    t = make_table()
    mask = t.a > 3
    assert repr(t[mask]._) == repr(eager(t, mask)._)


# ---------------------------------------------------------------------------
# Warnings policy: name warnings fire once at the source and CARRY.
# A deferred filter neither re-fires them (the eager path rebuilt the map
# per filter and re-warned every time — the carry is a deliberate change)
# nor swallows warnings for new sins committed after the defer.
# ---------------------------------------------------------------------------

def test_collision_warning_does_not_refire_on_defer_or_latch(tier):
    with pytest.warns(UserWarning, match="reserved"):
        t = Table({'sum': [1, 2, 3, 4], 'a': [5, 6, 7, 8]})
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        q = t[t.a > 6]                    # defer: quiet
        assert list(q.sum_) == [3, 4]     # single-column gather: quiet
        assert q._mat is None
        assert list(q['sum']) == [3, 4]   # bracket escape: quiet
        repr(q)                           # latch (map reused): quiet


def test_postlatch_map_rebuild_does_not_refire_carried_collision(tier):
    with pytest.warns(UserWarning, match="reserved"):
        t = Table({'sum': [1, 2, 3, 4], 'a': [5, 6, 7, 8]})
    q = t[t.a > 6]
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        q.a = [10, 20]                    # column swap → map rebuild: quiet
        assert list(q.a) == [10, 20]


def test_new_collision_after_defer_still_warns(tier):
    t = Table({'a': [1, 2, 3], 'b': [4, 5, 6]})
    q = t[t.a > 1]
    with pytest.warns(UserWarning, match="reserved"):
        qr = q.rename({'b': 'sum'})       # fresh table, fresh sin
    assert list(qr['sum']) == [5, 6]


def test_duplicate_warning_fires_at_source_not_on_defer(tier):
    with pytest.warns(UserWarning, match="Duplicate column name"):
        t = Table([Vector([1, 2, 3], name='x'), Vector([4, 5, 6], name='x')])
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        q = t[Vector([True, False, True])]
        assert list(q.x) == [1, 3]
        assert list(q.x__1) == [4, 6]
        repr(q)


def test_compose_existing_name_warning_survives_deferral(tier):
    t = make_table()
    q = t[t.a > 3]
    # Two warnings, same as eager: 'already exists' from >> and the
    # duplicate-name warning from constructing the widened table.
    with pytest.warns(UserWarning) as rec:
        qc = q >> {'a': [7, 8, 9]}
    msgs = [str(w.message) for w in rec]
    assert any("already exists" in m for m in msgs)
    assert any("Duplicate column name" in m for m in msgs)
    assert type(qc) is Table


def test_alias_on_gathered_column_is_rejected_without_latching(tier):
    # Metadata follows the same value doctrine as element buffers. Rejection
    # happens on the gathered column without forcing the other columns.
    t = make_table()
    q = t[t.a > 3]
    with pytest.raises(SerifTypeError, match="metadata is frozen"):
        q.b.alias('height')
    assert q.column_names() == ['a', 'b', 's', 'n', 'f']
    assert q._mat is None


def test_rename_on_deferred_table_is_owner_addressed(tier):
    t = make_table()
    q = t[t.a > 3]
    with pytest.warns(UserWarning, match="reserved"):
        renamed = q.rename({'b': 'sum'})
    assert list(renamed.sum_) == [4.0, 5.5, 6.75]
    assert q.column_names() == ['a', 'b', 's', 'n', 'f']


# ---------------------------------------------------------------------------
# Popcount conformance (accel vs pure)
# ---------------------------------------------------------------------------

def test_popcount_conformance():
    pytest.importorskip("numpy")
    for values in ([True, False, True], [True, None, False, True],
                   [None, None], [], [False, False]):
        mask = Vector(values, dtype=None) if values else Vector([True])[0:0]
        fast = vector_selection.popcount(mask._storage)
        pure = sum(1 for v in mask._storage if v)
        assert fast == pure
