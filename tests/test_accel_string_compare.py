"""
Conformance tests for the OPTIONAL arrow-accelerated string comparisons.

The guarantee under test — python in → python out, backend-independent:
for any string vector and any scalar or second vector, `v <op> other`
must return an IDENTICAL vector whether the arrow backend runs or not.
Same values,
same nulls in the same slots, same schema, same storage type — and every
surfaced value a concrete Python bool, never an arrow or numpy scalar.

The equivalence being pinned: UTF-8 byte order IS codepoint order, so
arrow's bytewise compare and Python's str compare agree on all six
operators — including across multi-byte codepoints, and including
NFC/NFD lookalikes (different codepoints, so both paths call them
unequal).

_pure() switches off only the ARROW tier; numpy stays live on both
sides, which is exactly the isolation this commit needs (numpy declines
string content anyway — numpy-off conformance is test_accel_ops's
department).

Skipped entirely when pyarrow isn't installed.
"""

import operator

import pytest

pa = pytest.importorskip("pyarrow")

from serif import Vector
from serif._execution import DECLINED
from serif._vector._arrow import operators as bridge
from serif._vector.storage import BoolStorage, StringStorage


OPS = [operator.eq, operator.ne, operator.lt,
       operator.le, operator.gt, operator.ge]


# ---------------------------------------------------------------------------
# Helpers
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
    for i, (p, f) in enumerate(zip(pure_v, fast_v)):
        if p is None:
            assert f is None, f"[{i}]: expected None, got {f!r}"
        else:
            assert f == p, f"[{i}]: {f!r} != {p!r}"
            assert type(f) is bool, f"[{i}]: {type(f)} is not bool"


def _conform(v, op, scalar):
    pure = _pure(lambda: op(v, scalar))
    fast = op(v, scalar)
    _assert_identical(pure, fast)


# ---------------------------------------------------------------------------
# All six operators, python-identical results
# ---------------------------------------------------------------------------

def test_all_ops_with_nulls():
    v = Vector(['apple', 'banana', None, '', 'cherry', 'Banana', 'banana'])
    for op in OPS:
        _conform(v, op, 'banana')


def test_all_ops_no_nulls():
    v = Vector(['delta', 'alpha', 'echo', '', 'alpha'])
    for op in OPS:
        _conform(v, op, 'alpha')


def test_all_null_column():
    v = Vector(['x', None, None])[1:]   # StringStorage with every lane null
    assert type(v._storage) is StringStorage   # the premise, not the test
    for op in OPS:
        _conform(v, op, 'x')


def test_empty_string_scalar():
    v = Vector(['', 'a', None, ''])
    for op in OPS:
        _conform(v, op, '')


def test_unicode_ordering_and_equality():
    # NFC e-acute (one codepoint) vs NFD (e + combining acute) — different
    # codepoints, so BOTH paths call them unequal; multi-byte ordering must
    # agree bytewise. Built with chr(): the two normal forms are visually
    # identical in source and editors love to silently renormalize them.
    nfc, nfd = chr(0xE9), 'e' + chr(0x301)   # one codepoint vs two
    v = Vector([nfc, 'e', nfd, '\U0001f600', '日本語', 'z'])
    for scalar in (nfc, nfd, '\U0001f600', 'z'):
        for op in OPS:
            _conform(v, op, scalar)


# ---------------------------------------------------------------------------
# The fast path actually engages (not silently declining everywhere)
# ---------------------------------------------------------------------------

def test_fast_path_engages():
    v = Vector(['apple', None, 'banana'])
    st = bridge.compare_strings(v._storage, 'apple', operator.eq)
    assert type(st) is BoolStorage
    assert list(st) == [True, None, False]


# ---------------------------------------------------------------------------
# Declines that must stay invisible
# ---------------------------------------------------------------------------

def test_str_subclass_scalar_declines():
    # A str subclass may override comparison — the pure path would honor
    # it, so the guard is `type(rhs) is str` exactly.
    class Loud(str):
        pass
    v = Vector(['apple', 'banana'])
    assert bridge.compare_strings(
        v._storage,
        Loud('apple'),
        operator.eq,
    ) is DECLINED
    _conform(v, operator.eq, Loud('apple'))   # pure both sides, still identical


def test_non_string_scalar():
    v = Vector(['apple', 'banana', None])
    _conform(v, operator.eq, 5)      # 'apple' == 5 is False, both paths
    _conform(v, operator.ne, 5)
    with pytest.raises(TypeError):
        v < 5                        # str < int raises, both paths
    with pytest.raises(TypeError):
        _pure(lambda: v < 5)


def test_none_scalar_warns_and_yields_null():
    v = Vector(['apple', 'banana'])
    with pytest.warns(UserWarning, match='Null comparison'):
        fast = v == None                       # noqa: E711 — the point
    with pytest.warns(UserWarning, match='Null comparison'):
        pure = _pure(lambda: v == None)        # noqa: E711
    _assert_identical(pure, fast)
    assert list(fast) == [None, None]


def test_non_string_storage_untouched():
    # Int compare rides numpy exactly as before; arrow never sees it.
    v = Vector([1, 2, None, 3])
    pure = _pure(lambda: v == 2)
    _assert_identical(pure, v == 2)


# ---------------------------------------------------------------------------
# Downstream: masks from arrow compares feed Kleene chaining and filters
# ---------------------------------------------------------------------------

def test_kleene_chaining_and_filter():
    v = Vector(['a', 'b', None, 'a', 'c', 'a'])
    w = Vector(['x', 'x', 'x', None, 'x', 'y'])

    def compound():
        return (v == 'a') & (w == 'x')

    pure_mask = _pure(compound)
    fast_mask = compound()
    _assert_identical(pure_mask, fast_mask)

    pure_rows = _pure(lambda: list(v[compound()]))
    fast_rows = list(v[compound()])
    assert fast_rows == pure_rows


# ---------------------------------------------------------------------------
# Vector vs vector
# ---------------------------------------------------------------------------

def test_vector_vector_all_ops_with_nulls():
    # Nulls staggered across the two sides: left-only, right-only, both.
    v = Vector(['apple', 'banana', None, '',   'pear', None, 'kiwi'])
    w = Vector(['apple', 'apple',  'x',  None, 'pear', None, 'lime'])
    for op in OPS:
        _conform(v, op, w)


def test_vector_vector_no_nulls():
    v = Vector(['delta', 'alpha', '', 'echo'])
    w = Vector(['delta', 'beta', 'a', ''])
    for op in OPS:
        _conform(v, op, w)


def test_vector_vector_unicode():
    nfc, nfd = chr(0xE9), 'e' + chr(0x301)   # one codepoint vs two
    v = Vector([nfc, nfd, '\U0001f600', 'z'])
    w = Vector([nfd, nfc, '\U0001f600', 'a'])
    for op in OPS:
        _conform(v, op, w)


def test_vector_vector_engages():
    v = Vector(['apple', None, 'b'])
    w = Vector(['apple', 'c', None])
    st = bridge.compare_strings(v._storage, w._storage, operator.eq)
    assert type(st) is BoolStorage
    assert list(st) == [True, None, None]


def test_vector_vector_empty():
    v = Vector(['a'])[:0]
    assert type(v._storage) is StringStorage   # the premise, not the test
    for op in OPS:
        _conform(v, op, v)


def test_vector_vector_mixed_kind_declines():
    # String column vs int column: eq/ne compare unequal (False), ordering
    # raises — both identical to pure, arrow never engages (rhs storage is
    # not a StringStorage).
    v = Vector(['apple', 'banana', None])
    w = Vector([1, 2, 3])
    assert bridge.compare_strings(
        v._storage,
        w._storage,
        operator.eq,
    ) is DECLINED
    _conform(v, operator.eq, w)
    _conform(v, operator.ne, w)
    with pytest.raises(TypeError):
        v < w
    with pytest.raises(TypeError):
        _pure(lambda: v < w)
