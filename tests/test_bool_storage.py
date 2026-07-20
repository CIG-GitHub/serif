"""
BoolStorage — byte-packed boolean backend.

One 0/1 byte per element in a bytearray, nulls in a BitMask (the
ArrayStorage split). Replaces TupleStorage-of-interned-bools: ~8x less
memory, plain byte indexing in pure Python, zero-copy viewable by numpy,
one pack/unpack pass from Arrow/Parquet's bit-packed BOOLEAN.

Generic protocol behavior is covered by test_storage_protocol.py; this
file pins what's specific to bools: which operations EMIT the backend,
the python-out boundary, and the Parquet fast paths on both readers.
"""

from serif import Table, Vector
from serif._vector.storage import BoolStorage


# ---------------------------------------------------------------------------
# Construction and the python-out boundary
# ---------------------------------------------------------------------------

def test_from_iterable_dense_and_nullable():
    dense = BoolStorage.from_iterable([True, False, True])
    assert bytes(dense._data) == b'\x01\x00\x01'
    assert dense._mask is None

    nullable = BoolStorage.from_iterable([True, None, False])
    assert bytes(nullable._data) == b'\x01\x00\x00'  # null slot = 0 sentinel
    assert nullable._mask is not None
    assert list(nullable) == [True, None, False]


def test_getitem_returns_real_python_bools():
    v = Vector([True, False, None])
    assert type(v[0]) is bool
    assert type(v[1]) is bool
    assert v[2] is None
    assert all(type(x) is bool for x in v if x is not None)


# ---------------------------------------------------------------------------
# Emission: every bool-producing operation lands on the packed backend
# ---------------------------------------------------------------------------

def test_comparisons_emit_bool_storage():
    v = Vector([1, 2, 3])
    assert isinstance((v > 2)._storage, BoolStorage)
    assert isinstance((v == 2)._storage, BoolStorage)


def test_is_na_emits_bool_storage():
    assert isinstance(Vector([1, None, 3]).is_na()._storage, BoolStorage)
    assert isinstance(Vector(['a', 'b']).is_na()._storage, BoolStorage)
    assert list(Vector([1, None, 3]).is_na()) == [False, True, False]


def test_kleene_logic_emits_bool_storage():
    a = Vector([True, None, False])
    b = Vector([True, True, None])
    both = a & b
    assert isinstance(both._storage, BoolStorage)
    assert list(both) == [True, None, False]  # False & None = False (Kleene)


def test_mask_filter_roundtrip():
    v = Vector([10, 20, 30, 40])
    mask = v > 15
    assert isinstance(mask._storage, BoolStorage)
    assert list(v[mask]) == [20, 30, 40]


# ---------------------------------------------------------------------------
# Parquet: both readers build BoolStorage directly from bit-packed pages
# ---------------------------------------------------------------------------

def _roundtrip(t, tmp_path, use_arrow):
    import serif.io.parquet as parquet_mod
    p = str(tmp_path / 'b.parquet')
    t.to_parquet(p)
    saved = parquet_mod._USE_ARROW
    parquet_mod._USE_ARROW = use_arrow and saved
    try:
        return parquet_mod.read_parquet(p)
    finally:
        parquet_mod._USE_ARROW = saved


def test_pure_reader_builds_bool_storage(tmp_path):
    t = Table({'b': [True, False, None, True] * 5})
    out = _roundtrip(t, tmp_path, use_arrow=False)
    assert isinstance(out['b']._storage, BoolStorage)
    assert list(out['b']) == [True, False, None, True] * 5


def test_pure_reader_dense_bools(tmp_path):
    # 9 values: the last bit-packed byte is partial — exercises the trim.
    t = Table({'b': [True, False, True, True, False, False, True, False, True]})
    out = _roundtrip(t, tmp_path, use_arrow=False)
    assert isinstance(out['b']._storage, BoolStorage)
    assert out['b']._storage._mask is None
    assert list(out['b']) == list(t['b'])


def test_arrow_reader_builds_bool_storage(tmp_path):
    import pytest
    pytest.importorskip('pyarrow')
    t = Table({'b': [True, None, False] * 4})
    out = _roundtrip(t, tmp_path, use_arrow=True)
    assert isinstance(out['b']._storage, BoolStorage)
    assert list(out['b']) == [True, None, False] * 4
