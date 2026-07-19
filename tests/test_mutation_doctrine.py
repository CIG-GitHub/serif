"""
The mutation doctrine: READ THROUGH THE COLUMN, WRITE THROUGH THE TABLE.

- Anything read out of a table is a value: owned columns are frozen
  (vector-addressed writes raise), and owner writes swap in a fresh
  column object, so a previously read-out column never changes.
- The table itself is a normal Python object you mutate by addressing
  it: t[mask, 'v'] = ..., t[3, 'v'] = ..., t.v = ... — no ceremony,
  rebuild-on-write, everything derived stays safe.
- `with t.batch() as m:` is the bulk fast path (copy-on-enter → raw
  in-place writes → refreeze-on-exit) for read-modify-write loops.
  Same observable semantics, O(1) per write.
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
# Vector-addressed writes raise (the aliasing lie stays dead)
# ---------------------------------------------------------------------------

def test_owned_column_setitem_raises():
    t = make_table()
    with pytest.raises(SerifTypeError, match="Write through the owning table"):
        t.a[0] = 99
    assert t.a[0] == 1


def test_error_message_names_the_column():
    t = make_table()
    with pytest.raises(SerifTypeError, match="t\\[key, 'a'\\]"):
        t.a[0] = 99


def test_owned_column_via_string_key_raises():
    t = make_table()
    with pytest.raises(SerifTypeError, match="frozen"):
        t['a'][0] = 99


def test_carried_away_alias_cannot_mutate_table():
    t = make_table()
    v = t.a
    with pytest.raises(SerifTypeError):
        v[0] = 99
    assert t.a[0] == 1


def test_filtered_result_column_is_frozen():
    t = make_table()
    with pytest.raises(SerifTypeError):
        t[t.a > 1].a[0] = 99


def test_owned_categorical_setitem_raises():
    c = Vector(['lo', 'hi', 'lo']).categorize(['lo', 'hi']).alias('c')
    t = Table([c])
    with pytest.raises(SerifTypeError, match="frozen"):
        t.c[0] = 'hi'


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


# ---------------------------------------------------------------------------
# Owner-addressed writes work, no ceremony
# ---------------------------------------------------------------------------

def test_conditional_update_the_analyst_one_liner():
    t = Table({'v': ['keep', 'old', 'keep', 'old']})
    t[t.v == 'old', 'v'] = 'new'
    assert list(t.v) == ['keep', 'new', 'keep', 'new']


def test_cell_write():
    t = make_table()
    t[0, 'a'] = 99
    assert t.a[0] == 99


def test_region_write():
    t = make_table()
    t[0:2, 'a'] = 0
    assert list(t.a) == [0, 0, 3]


def test_row_write():
    t = make_table()
    t[0, :] = [9, 9.9, False, 'q']
    assert t.a[0] == 9 and t.s[0] == 'q'


def test_column_replacement_still_legal():
    t = make_table()
    t.a = t.a + 1
    assert list(t.a) == [2, 3, 4]


def test_column_iadd_desugars_to_replacement():
    t = make_table()
    t.a += 10                     # no __iadd__: this is t.a = t.a + 10
    assert list(t.a) == [11, 12, 13]


def test_categorical_owner_addressed_write():
    c = Vector(['lo', 'hi', 'lo']).categorize(['lo', 'hi']).alias('c')
    t = Table([c])
    t[0, 'c'] = 'hi'
    assert t.c[0] == 'hi'
    with pytest.raises(SerifValueError):
        t[0, 'c'] = 'nope'        # outside the category list still raises


def test_write_none_widens_schema():
    t = make_table()
    t[0, 'a'] = None
    assert t.a[0] is None
    assert t.a.schema().nullable is True


def test_promoting_write():
    t = make_table()
    t[0, 'a'] = 1.5
    assert t.a[0] == 1.5
    assert t.a.schema().kind is float


# ---------------------------------------------------------------------------
# Owner writes never reach anything already read out (swap-on-write)
# ---------------------------------------------------------------------------

def test_copy_is_untouched_by_owner_write():
    t = make_table()
    t2 = t.copy()
    t[0, 'a'] = 99
    assert t2.a[0] == 1


def test_slice_is_untouched_by_owner_write():
    t = make_table()
    head = t[0:2]
    t[0, 'a'] = 99
    assert head.a[0] == 1


def test_filtered_result_is_untouched_by_owner_write():
    t = make_table()
    kept = t[t.a > 0]
    t[0, 'a'] = 99
    assert kept.a[0] == 1


def test_read_out_column_is_a_stable_snapshot():
    # Owner writes SWAP the column object; a previously read-out column
    # is a value, not a live view of later table writes.
    t = make_table()
    v = t.a
    t[0, 'a'] = 99
    assert v[0] == 1
    assert t.a[0] == 99


def test_source_vector_untouched_by_owner_write():
    v = Vector([1, 2, 3], name='v')
    t = Table([v])
    t[0, 'v'] = 99
    assert list(v) == [1, 2, 3]


def test_owner_write_changes_table_and_column_fingerprint():
    t = make_table()
    fp_col = t.a.fingerprint()
    t[0, 'a'] = 99
    assert t.a.fingerprint() != fp_col


# ---------------------------------------------------------------------------
# batch(): the bulk fast path
# ---------------------------------------------------------------------------

def test_batch_point_writes_land_on_table():
    t = make_table()
    with t.batch() as m:
        m.a[0] = 99                # vector-addressed works inside: thawed
        m[1, 'a'] = 88             # table-addressed works too
    assert list(t.a) == [99, 88, 3]


def test_batch_m_is_t():
    t = make_table()
    with t.batch() as m:
        assert m is t


def test_batch_write_is_raw_in_place():
    t = make_table()
    with t.batch() as m:
        st = m.a._storage
        m.a[0] = 99
        assert m.a._storage is st  # no rebuild inside the scope


def test_copy_before_batch_is_untouched():
    t = make_table()
    t2 = t.copy()
    with t.batch() as m:
        m.a[0] = 99
    assert t2.a[0] == 1


def test_batch_read_modify_write_loop():
    t = Table({'a': list(range(100))})
    with t.batch() as m:
        col = m.a
        for i in range(1, 100):
            col[i] = col[i] + col[i - 1]   # running sum, order-dependent
    assert t.a[99] == sum(range(100))


def test_escaped_column_ref_refreezes_after_batch():
    t = make_table()
    with t.batch() as m:
        leak = m.a
        leak[1] = 55
    assert t.a[1] == 55
    with pytest.raises(SerifTypeError):
        leak[0] = 1


def test_batch_nesting_raises():
    t = make_table()
    with t.batch():
        with pytest.raises(SerifValueError, match="batch\\(\\) scope"):
            with t.batch():
                pass


def test_sequential_batches_work():
    t = make_table()
    with t.batch() as m:
        m.a[0] = 10
    with t.batch() as m:
        m.a[1] = 20
    assert list(t.a) == [10, 20, 3]


def test_exception_mid_batch_keeps_partial_writes_and_refreezes():
    t = make_table()
    with pytest.raises(RuntimeError):
        with t.batch() as m:
            m.a[0] = 99
            raise RuntimeError("boom")
    assert t.a[0] == 99                 # no rollback — documented
    with pytest.raises(SerifTypeError):
        t.a[1] = 1                      # refrozen despite the exception


def test_batch_write_none_in_place():
    t = make_table()
    with t.batch() as m:
        m.a[0] = None
    assert t.a[0] is None
    assert t.a.schema().nullable is True


def test_batch_promoting_write_declines_to_rebuild():
    t = make_table()
    with t.batch() as m:
        m.a[0] = 1.5
    assert t.a[0] == 1.5


def test_batch_bigint_write_declines_to_rebuild():
    t = make_table()
    with t.batch() as m:
        m.a[0] = 2 ** 80
    assert t.a[0] == 2 ** 80


def test_batch_string_column_write_rebuilds():
    t = make_table()
    with t.batch() as m:
        m.s[0] = 'hello'
    assert t.s[0] == 'hello'


def test_fingerprint_stable_across_empty_batch():
    t = make_table()
    before = t.a.fingerprint()
    with t.batch():
        pass
    assert t.a.fingerprint() == before
