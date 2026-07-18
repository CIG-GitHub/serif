"""
Mutation guardrail: owned vectors are frozen; mutation happens inside an
explicit `with t.mutable() as m:` scope (copy-on-enter → raw in-place
writes → refreeze-on-exit).

The invariant under test: a vector read out of a table cannot mutate the
table, and nothing outside a mutable() scope can observe writes made
inside one — snapshots taken before entry hold the old buffers.
"""

import pytest

from serif import Table, Vector
from serif.errors import SerifTypeError, SerifValueError


def make_table():
    return Table({
        'a': [1, 2, 3],
        'f': [1.5, 2.5, 3.5],
        'b': [True, False, True],
        's': ['x', 'y', 'z'],
    })


# ---------------------------------------------------------------------------
# Frozen by default
# ---------------------------------------------------------------------------

def test_owned_column_setitem_raises():
    t = make_table()
    with pytest.raises(SerifTypeError, match="mutable\\(\\)"):
        t.a[0] = 99
    assert t.a[0] == 1


def test_owned_column_via_string_key_raises():
    t = make_table()
    with pytest.raises(SerifTypeError, match="frozen"):
        t['a'][0] = 99


def test_table_cell_assignment_raises():
    t = make_table()
    with pytest.raises(SerifTypeError, match="mutable\\(\\)"):
        t[0, 'a'] = 99
    assert t.a[0] == 1


def test_table_region_assignment_raises():
    t = make_table()
    with pytest.raises(SerifTypeError, match="mutable\\(\\)"):
        t[:, 'a'] = [7, 8, 9]


def test_read_out_alias_cannot_mutate_table():
    t = make_table()
    v = t.a
    with pytest.raises(SerifTypeError):
        v[0] = 99
    assert t.a[0] == 1


def test_copy_of_owned_column_is_mutable():
    t = make_table()
    c = t.a.copy()
    c[0] = 99
    assert c[0] == 99
    assert t.a[0] == 1


def test_standalone_vector_still_mutable():
    v = Vector([1, 2, 3])
    v[0] = 99
    assert v[0] == 99


def test_source_vector_stays_mutable_after_table_build():
    v = Vector([1, 2, 3], name='v')
    t = Table([v])
    v[0] = 99          # v was copied in; the original is still the user's
    assert v[0] == 99
    assert t.v[0] == 1


def test_owned_categorical_setitem_raises():
    c = Vector(['lo', 'hi', 'lo']).categorize(['lo', 'hi']).alias('c')
    t = Table([c])
    with pytest.raises(SerifTypeError, match="frozen"):
        t.c[0] = 'hi'


# ---------------------------------------------------------------------------
# The mutable() scope
# ---------------------------------------------------------------------------

def test_scope_point_write_lands_on_table():
    t = make_table()
    with t.mutable() as m:
        m.a[0] = 99
    assert t.a[0] == 99


def test_m_is_t():
    t = make_table()
    with t.mutable() as m:
        assert m is t


def test_copy_before_scope_is_untouched():
    # The acceptance example: t2 shares buffers with t until the scope
    # un-shares them on enter.
    t = make_table()
    t2 = t.copy()
    with t.mutable() as m:
        m.a[0] = 99
    assert t.a[0] == 99
    assert t2.a[0] == 1


def test_slice_before_scope_is_untouched():
    t = make_table()
    head = t[0:2]
    with t.mutable() as m:
        m.a[0] = 99
    assert head.a[0] == 1


def test_filtered_result_before_scope_is_untouched():
    t = make_table()
    kept = t[t.a > 0]      # all rows
    with t.mutable() as m:
        m.a[0] = 99
    assert kept.a[0] == 1


def test_cell_and_region_assignment_inside_scope():
    t = make_table()
    with t.mutable() as m:
        m[0, 'a'] = 42
        m[:, 'f'] = [9.0, 8.0, 7.0]
    assert t.a[0] == 42
    assert list(t.f) == [9.0, 8.0, 7.0]


def test_mask_assignment_inside_scope():
    t = make_table()
    with t.mutable() as m:
        m.a[Vector([True, False, True])] = 0
    assert list(t.a) == [0, 2, 0]


def test_escaped_column_ref_refreezes_on_exit():
    t = make_table()
    with t.mutable() as m:
        leak = m.a
        leak[1] = 55       # legal: it IS the thawed column
    assert t.a[1] == 55
    with pytest.raises(SerifTypeError):
        leak[0] = 1


def test_nesting_raises():
    t = make_table()
    with t.mutable():
        with pytest.raises(SerifValueError, match="nesting"):
            with t.mutable():
                pass


def test_sequential_scopes_work():
    t = make_table()
    with t.mutable() as m:
        m.a[0] = 10
    with t.mutable() as m:
        m.a[1] = 20
    assert list(t.a) == [10, 20, 3]


def test_exception_mid_scope_keeps_partial_writes_and_refreezes():
    t = make_table()
    with pytest.raises(RuntimeError):
        with t.mutable() as m:
            m.a[0] = 99
            raise RuntimeError("boom")
    assert t.a[0] == 99                 # no rollback — documented unsafe path
    with pytest.raises(SerifTypeError):
        t.a[1] = 1                      # refrozen despite the exception
    with t.mutable() as m:              # and the scope is re-enterable
        m.a[1] = 2
    assert t.a[1] == 2


# ---------------------------------------------------------------------------
# In-place fast path vs rebuild fallback (both must be correct)
# ---------------------------------------------------------------------------

def test_int_write_is_in_place():
    t = make_table()
    with t.mutable() as m:
        st = m.a._storage
        m.a[0] = 99
        assert m.a._storage is st       # raw write, no rebuild


def test_bool_write_is_in_place():
    t = make_table()
    with t.mutable() as m:
        st = m.b._storage
        m.b[1] = True
        assert m.b._storage is st
    assert t.b[1] is True


def test_float_column_accepts_int_write():
    t = make_table()
    with t.mutable() as m:
        m.f[0] = 3
    assert t.f[0] == 3.0


def test_write_none_sets_null_and_widens_schema():
    t = make_table()
    with t.mutable() as m:
        m.a[0] = None
    assert t.a[0] is None
    assert t.a.schema().nullable is True
    assert t.a.count() == 2


def test_overwrite_null_with_value():
    t = Table({'a': [1, None, 3]})
    with t.mutable() as m:
        m.a[1] = 2
    assert list(t.a) == [1, 2, 3]


def test_string_column_write_rebuilds():
    t = make_table()
    with t.mutable() as m:
        m.s[0] = 'hello'
    assert t.s[0] == 'hello'
    assert list(t.s) == ['hello', 'y', 'z']


def test_promoting_write_declines_to_rebuild():
    t = make_table()
    with t.mutable() as m:
        m.a[0] = 1.5                    # float into int column → promotion
    assert t.a[0] == 1.5
    assert t.a.schema().kind is float


def test_bigint_write_declines_to_rebuild():
    t = make_table()
    big = 2 ** 80
    with t.mutable() as m:
        m.a[0] = big                    # can't live in an int64 buffer
    assert t.a[0] == big


def test_categorical_write_inside_scope():
    c = Vector(['lo', 'hi', 'lo']).categorize(['lo', 'hi']).alias('c')
    t = Table([c])
    with t.mutable() as m:
        m.c[0] = 'hi'
    assert t.c[0] == 'hi'


# ---------------------------------------------------------------------------
# Fingerprints
# ---------------------------------------------------------------------------

def test_fingerprint_invalidated_by_scope_write():
    t = make_table()
    before = t.a.fingerprint()
    with t.mutable() as m:
        m.a[0] = 99
    assert t.a.fingerprint() != before


def test_fingerprint_stable_without_writes():
    t = make_table()
    before = t.a.fingerprint()
    with t.mutable():
        pass
    assert t.a.fingerprint() == before
