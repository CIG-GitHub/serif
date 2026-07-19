"""
Conformance tests for the OPTIONAL arrow-accelerated string-key join
probe (serif._accel.arrow.join_probe_strings).

The guarantee under test — accelerators widen transport, never
semantics: every string-keyed join must return IDENTICAL results with
the arrow backend on or off — same values, same nulls, same schemas,
same column names, same row order, and the same error text when a
cardinality expectation is violated. Declines (nullable keys, empty
sides, non-string storages) must be invisible.

The design point worth its own test here: both key columns encode
through ONE shared dictionary. Encoding the sides separately would give
every left string absent from the right the same "missing" code, and
two DISTINCT unmatched left keys would falsely trip
expect_left_unique — test_left_unique_distinct_unmatched_keys_pass is
the regression guard.

Like the string bucketing, the probe is a two-backend composition
(arrow encodes, numpy probes), so this suite skips unless BOTH are
installed; _pure() toggles only the arrow switch.
"""

import pytest

np = pytest.importorskip("numpy")
pa = pytest.importorskip("pyarrow")

from serif import Table, Vector
from serif.errors import SerifValueError
from serif._accel import arrow as bridge


# ---------------------------------------------------------------------------
# Helpers (same harness as the other accel suites)
# ---------------------------------------------------------------------------

def _pure(fn):
    saved = bridge._USE_ARROW
    bridge._USE_ARROW = False
    try:
        return fn()
    finally:
        bridge._USE_ARROW = saved


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
# End-to-end conformance, all three flavors
# ---------------------------------------------------------------------------

def _join_tables():
    left = Table({
        'k': ['a', 'b', 'c', 'd', 'b'],   # dup 'b'; 'a'/'d' unmatched
        'x': [1.5, None, 2.5, 4.0, 0.5],
        's': ['p', None, 'q', 'r', 's'],
        'b': [True, False, None, True, False],
    })
    right = Table({
        'k':     ['b', 'c', 'e', 'b'],    # dup 'b': fan-out; 'e' unmatched
        'other': [20, 30, None, 21],
        'tag':   ['p', 'q', None, 'r'],
    })
    return left, right


@pytest.mark.parametrize("flavor", ["inner_join", "left_join", "full_join"])
def test_string_join_conforms(flavor):
    def run():
        left, right = _join_tables()
        return getattr(left, flavor)(right, 'k', 'k',
                                     expect_right_unique=False)
    _assert_tables_identical(_pure(run), run())


def test_string_join_no_matches_conforms():
    def run():
        left = Table({'k': ['a', 'b'], 'x': [1.0, 2.0]})
        right = Table({'k': ['c', 'd'], 'y': [3.0, 4.0]})
        return left.inner_join(right, 'k', 'k')
    _assert_tables_identical(_pure(run), run())


def test_unicode_keys_conform():
    nfc, nfd = chr(0xE9), 'e' + chr(0x301)   # different keys to BOTH paths
    def run():
        left = Table({'k': [nfc, nfd, '\U0001f600'], 'x': [1, 2, 3]})
        right = Table({'k': [nfd, nfc], 'y': [10, 20]})
        return left.left_join(right, 'k', 'k')
    fast, pure = run(), _pure(run)
    _assert_tables_identical(pure, fast)
    assert list(fast['y']) == [20, 10, None]   # normal forms stay distinct


def test_left_join_empty_right_conforms():
    def run():
        left = Table({'k': ['a', 'b'], 'x': [1.5, 2.5]})
        right = Table({'k': Vector([], dtype=str),
                       'y': Vector([], dtype=float)})
        return left.left_join(right, 'k', 'k')
    fast, pure = run(), _pure(run)
    _assert_tables_identical(pure, fast)
    assert list(fast['y']) == [None, None]


# ---------------------------------------------------------------------------
# Cardinality expectations — same raise, same text
# ---------------------------------------------------------------------------

def test_expect_right_unique_error_matches_pure():
    def run():
        left = Table({'k': ['q', 'p']})
        right = Table({'k': ['q', 'p', 'q', 'p', 'p'], 'v': [1, 2, 3, 4, 5]})
        return left.inner_join(right, 'k', 'k')
    with pytest.raises(SerifValueError) as fast_err:
        run()
    with pytest.raises(SerifValueError) as pure_err:
        _pure(run)
    assert str(fast_err.value) == str(pure_err.value)


def test_expect_left_unique_error_matches_pure():
    def run():
        left = Table({'k': ['m', 'n', 'm'], 'x': [1, 2, 3]})
        right = Table({'k': ['m', 'n'], 'y': [10, 20]})
        return left.inner_join(right, 'k', 'k', expect_left_unique=True)
    with pytest.raises(SerifValueError) as fast_err:
        run()
    with pytest.raises(SerifValueError) as pure_err:
        _pure(run)
    assert str(fast_err.value) == str(pure_err.value)


def test_left_unique_distinct_unmatched_keys_pass():
    # THE shared-dictionary regression guard: 'a' and 'd' are both absent
    # from the right side. Per-side encoding would give them one shared
    # "missing" code and report a phantom duplicate; the shared dictionary
    # keeps them distinct, and expect_left_unique passes — as pure does.
    def run():
        left = Table({'k': ['a', 'b', 'c', 'd'], 'x': [1, 2, 3, 4]})
        right = Table({'k': ['b', 'c'], 'y': [20, 30]})
        return left.left_join(right, 'k', 'k', expect_left_unique=True)
    _assert_tables_identical(_pure(run), run())


# ---------------------------------------------------------------------------
# Declines that must stay invisible
# ---------------------------------------------------------------------------

def test_nullable_keys_decline_and_conform():
    # The pure loop joins None keys like any value ((None,) == (None,)),
    # which codes cannot carry — so nullable declines, and both modes run
    # the pure matcher, None-matches-None included.
    left = Table({'k': ['a', None, 'b'], 'x': [1, 2, 3]})
    right = Table({'k': [None, 'b'], 'y': [10, 20]})
    assert bridge.join_probe_strings(
        left.cols(0)._storage, right.cols(0)._storage,
        False, False, True, False) is None

    def run():
        return left.left_join(right, 'k', 'k')
    fast, pure = run(), _pure(run)
    _assert_tables_identical(pure, fast)
    assert list(fast['y']) == [None, 10, 20]   # the None↔None match, pinned


def test_mixed_key_kinds_decline():
    left = Table({'k': ['a', 'b'], 'x': [1, 2]})
    right = Table({'k': [1, 2], 'y': [10, 20]})
    assert bridge.join_probe_strings(
        left.cols(0)._storage, right.cols(0)._storage,
        False, False, True, False) is None


# ---------------------------------------------------------------------------
# The fast path actually engages (guards against silent decline rot)
# ---------------------------------------------------------------------------

def test_string_sort_fallback_engages_when_hash_probe_declines(monkeypatch):
    calls = []
    orig = bridge.join_probe_strings

    def spy(*args, **kwargs):
        result = orig(*args, **kwargs)
        calls.append(result is not None)
        return result

    monkeypatch.setattr(bridge, 'join_probe_strings', spy)
    monkeypatch.setattr(bridge, 'join_probe_strings_hash',
                        lambda *args, **kwargs: None)

    left = Table({'k': ['a', 'b', 'c'], 'x': [1.0, 2.0, 3.0]})
    right = Table({'k': ['b', 'c'], 'y': ['u', 'v']})
    left.left_join(right, 'k', 'k')
    assert calls == [True]

    calls.clear()
    ileft = Table({'id': [1, 2], 'x': [1.0, 2.0]})
    iright = Table({'id': [2], 'y': [5.0]})
    ileft.left_join(iright, 'id', 'id')
    assert calls == []    # int keys: numpy's probe answered first
