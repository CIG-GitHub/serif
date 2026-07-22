"""
Conformance tests for the OPTIONAL Arrow string-key bucketing under
serif._table._arrow.grouping behind aggregate, plus adjacent join isolation.

The guarantee under test — accelerators widen transport, never
semantics: every string-keyed aggregate and join must return IDENTICAL
results with the arrow backend on or off — same values, same nulls,
same schemas, same column names, same FIRST-APPEARANCE group order, and
the same error text when a cardinality expectation is violated.
Declines (nullable keys, non-string storages, either backend switched
off) must be invisible.

group_strings is a two-backend composition (arrow encodes, numpy
buckets), so this suite skips unless BOTH are installed; _pure()
toggles the Arrow switches, isolating this commit's tier.
"""

import pytest

pytest.importorskip("numpy")
pytest.importorskip("pyarrow")

from serif import Table, Vector
from serif._execution import DECLINED
from serif._table._arrow import aggregation as aggregation_mod
from serif._table._arrow import grouping as group_mod
from serif._table._arrow import joins as join_mod
from serif._table._numpy import grouping as numpy_grouping
from serif.errors import SerifValueError


# ---------------------------------------------------------------------------
# Helpers (same harness as the other accel suites)
# ---------------------------------------------------------------------------

def _pure(fn):
    saved_aggregation = aggregation_mod._USE_ARROW
    saved_grouping = group_mod._USE_ARROW
    saved_join = join_mod._USE_ARROW
    aggregation_mod._USE_ARROW = False
    group_mod._USE_ARROW = False
    join_mod._USE_ARROW = False
    try:
        return fn()
    finally:
        aggregation_mod._USE_ARROW = saved_aggregation
        group_mod._USE_ARROW = saved_grouping
        join_mod._USE_ARROW = saved_join


def _assert_identical(pure_v, fast_v):
    assert len(fast_v) == len(pure_v)
    assert fast_v.vector_name == pure_v.vector_name
    assert type(fast_v._storage) is type(pure_v._storage)
    if pure_v.schema() is None:
        assert fast_v.schema() is None
    else:
        assert fast_v.schema().kind is pure_v.schema().kind
        assert fast_v.schema().nullable is pure_v.schema().nullable
    import math
    for i, (p, f) in enumerate(zip(pure_v, fast_v)):
        if p is None:
            assert f is None, f"[{i}]: expected None, got {f!r}"
        elif isinstance(p, float) and math.isnan(p):
            assert isinstance(f, float) and math.isnan(f), f"[{i}]: {f!r} is not nan"
        else:
            assert f == p, f"[{i}]: {f!r} != {p!r}"
            assert type(f) is type(p), f"[{i}]: {type(f)} vs {type(p)}"


def _assert_tables_identical(pure_t, fast_t):
    assert fast_t.column_names() == pure_t.column_names()
    assert len(fast_t) == len(pure_t)
    for i in range(len(pure_t.cols())):
        _assert_identical(pure_t.cols(i), fast_t.cols(i))


# ---------------------------------------------------------------------------
# group_strings — storage-level conformance vs the pure dict loop
# ---------------------------------------------------------------------------

def _pure_partition(vals):
    index = {}
    for i, v in enumerate(vals):
        index.setdefault((v,), []).append(i)
    return index


@pytest.mark.parametrize("vals", [
    ['b', 'a', 'b', 'c', 'a', 'b'],
    ['same', 'same', 'same'],
    ['', 'x', '', 'x', ''],
    ['e', 'd', 'c', 'b', 'a'],
    ['solo'],
    [chr(0xE9), 'e' + chr(0x301), chr(0xE9), '\U0001f600'],   # NFC vs NFD
    ['a' * 500, 'b', 'a' * 500],
], ids=["dupes", "single_key", "empty_strings", "all_unique_desc",
        "one_row", "unicode", "long_strings"])
def test_group_strings_matches_pure_dict(vals):
    fast = group_mod.group_strings(Vector(vals)._storage)
    pure = _pure_partition(vals)
    assert fast is not DECLINED
    assert fast == pure
    assert all(type(bucket) is list for bucket in fast.values())
    assert all(
        type(index) is int
        for bucket in fast.values()
        for index in bucket
    )
    assert list(fast.keys()) == list(pure.keys())    # first-appearance order
    assert all(type(k[0]) is str for k in fast.keys())   # python out


def test_group_strings_declines_unsupported():
    assert group_mod.group_strings(
        Vector(['a', None, 'b'])._storage
    ) is DECLINED  # nullable
    assert group_mod.group_strings(
        Vector([1, 2])._storage
    ) is DECLINED  # ints
    assert group_mod.group_strings(
        Vector(['a'])[:0]._storage
    ) is DECLINED  # empty


def test_group_strings_gates_on_both_switches(monkeypatch):
    storage = Vector(['a', 'b', 'a'])._storage
    assert group_mod.group_strings(storage) is not DECLINED
    monkeypatch.setattr(group_mod, '_USE_ARROW', False)
    assert group_mod.group_strings(storage) is DECLINED
    monkeypatch.undo()
    monkeypatch.setattr(numpy_grouping, '_USE_NUMPY', False)
    assert group_mod.group_strings(storage) is DECLINED


# ---------------------------------------------------------------------------
# Aggregate / window — end-to-end conformance
# ---------------------------------------------------------------------------

def test_aggregate_string_groupby_conforms():
    def run():
        t = Table({'g': ['ny', 'sf', 'ny', 'la', 'sf', 'ny'],
                   'x': [1.5, 2.5, None, 4.0, 0.5, 3.0]})
        return t.aggregate(groupby=t.g, aggregations={'total': t.x.sum})
    fast, pure = run(), _pure(run)
    _assert_tables_identical(pure, fast)
    assert list(fast.cols(0)) == ['ny', 'sf', 'la']  # first-appearance order


def test_aggregate_nullable_string_groupby_conforms():
    # None is a legitimate group key: the accelerator declines, and the
    # pure loop's grouping (None bucket included) is what both modes see.
    def run():
        t = Table({'g': ['a', None, 'a', None], 'x': [1, 2, 3, 4]})
        return t.aggregate(groupby=t.g, aggregations={'total': t.x.sum})
    _assert_tables_identical(_pure(run), run())


def test_window_string_groupby_conforms():
    # track_row_keys declines the bucket accelerator; results unchanged.
    def run():
        t = Table({'g': ['a', 'b', 'a', 'b'], 'x': [1, 2, 3, None]})
        return t.window(groupby=t.g, aggregations={'gsum': t.x.sum})
    _assert_tables_identical(_pure(run), run())


# ---------------------------------------------------------------------------
# Joins — conformance remains independent of grouping dispatch
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("flavor", ["inner_join", "left_join", "full_join"])
def test_string_key_join_conforms(flavor):
    def run():
        left = Table({'k': ['a', 'b', 'c', 'd', 'b'],
                      'x': [1.5, None, 2.5, 4.0, 0.5]})
        right = Table({'k': ['b', 'c', 'e', 'b'],   # dup 'b' fans out; 'e' unmatched
                       'y': [20, 30, None, 21]})
        return getattr(left, flavor)(right, 'k', 'k',
                                     expect_right_unique=False)
    _assert_tables_identical(_pure(run), run())


def test_string_key_expect_right_unique_error_matches_pure():
    def run():
        left = Table({'k': ['q', 'p']})
        right = Table({'k': ['q', 'p', 'q', 'p', 'p'], 'v': [1, 2, 3, 4, 5]})
        return left.inner_join(right, 'k', 'k')
    with pytest.raises(SerifValueError) as fast_err:
        run()
    with pytest.raises(SerifValueError) as pure_err:
        _pure(run)
    assert str(fast_err.value) == str(pure_err.value)


# ---------------------------------------------------------------------------
# The fast path actually engages (guards against silent decline rot)
# ---------------------------------------------------------------------------

def test_string_group_fallback_engages_when_fused_sum_declines(monkeypatch):
    calls = []
    orig = group_mod.group_strings

    def spy(*args, **kwargs):
        result = orig(*args, **kwargs)
        calls.append(result is not DECLINED)
        return result

    monkeypatch.setattr(group_mod, 'group_strings', spy)
    # Exercise string bucketing as a fallback after the fused grouped-sum
    # path declines; that earlier path has its own engagement test.
    monkeypatch.setattr(
        aggregation_mod,
        'grouped_sums',
        lambda *args, **kwargs: DECLINED,
    )

    t = Table({'g': ['a', 'b', 'a'], 'x': [1.0, 2.0, 3.0]})
    t.aggregate(groupby=t.g, aggregations={'m': t.x.sum})
    assert calls == [True]

    calls.clear()
    left = Table({'k': ['a', 'b'], 'x': [1, 2]})
    right = Table({'k': ['b'], 'y': [2.5]})
    left.left_join(right, 'k', 'k')
    assert calls == []        # the arrow join PROBE answers string joins
    #                           now; the right-index build never runs

    calls.clear()
    t2 = Table({'g': [1, 2, 1], 'x': [1.0, 2.0, 3.0]})
    t2.aggregate(groupby=t2.g, aggregations={'m': t2.x.sum})
    assert calls == []        # int keys: numpy got there first — the
    #                           cascade never even consults arrow
