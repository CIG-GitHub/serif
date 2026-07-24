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

import operator

import pytest

from serif import Table, Vector
from serif import SerifValueError
from serif._vector import operators as vector_ops
from serif._vector._python import operators as python_ops
from serif._vector.storage import BoolStorage


# ---------------------------------------------------------------------------
# Construction and the python-out boundary
# ---------------------------------------------------------------------------

def test_from_iterable_dense_and_nullable():
    dense = BoolStorage.from_iterable(
        value for value in [True, False, True]
    )
    assert bytes(dense._data) == b'\x01\x00\x01'
    assert dense._mask is None

    nullable = BoolStorage.from_iterable(
        value for value in [True, None, False]
    )
    assert bytes(nullable._data) == b'\x01\x00\x00'  # null slot = 0 sentinel
    assert nullable._mask is not None
    assert bytes(nullable._mask._buf) == b'\x05'
    assert list(nullable) == [True, None, False]


def test_from_iterable_preserves_non_null_truthiness():
    storage = BoolStorage.from_iterable(
        value for value in [1, 0, 'yes', '']
    )

    assert bytes(storage._data) == b'\x01\x00\x01\x00'
    assert storage._mask is None
    assert list(storage) == [True, False, True, False]


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


def test_pure_comparison_kernels_return_bool_storage_directly():
    left = Vector([1, None, 3])._storage
    right = Vector([1, 2, 4])._storage

    vector_result = python_ops.compare_vector(left, right, operator.eq)
    scalar_result = python_ops.compare_scalar(left, 2, operator.gt)

    assert isinstance(vector_result, BoolStorage)
    assert list(vector_result) == [True, None, False]
    assert isinstance(scalar_result, BoolStorage)
    assert list(scalar_result) == [False, None, True]


def test_iterable_comparison_uses_storage_mask_for_nullable_schema():
    result = Vector([1, 2, 3]) == [1, None, 4]
    dense = Vector([1, 2, 3]) == [1, 0, 3]

    assert isinstance(result._storage, BoolStorage)
    assert result.schema().kind is bool
    assert result.schema().nullable is True
    assert list(result) == [True, None, False]
    assert dense.schema().nullable is False
    assert list(dense) == [True, False, True]


def test_comparison_length_errors_precede_storage_construction():
    with pytest.raises(SerifValueError, match="Length mismatch: 2 != 1"):
        Vector([1, 2]) == Vector([1])
    with pytest.raises(SerifValueError, match="Length mismatch: 2 != 1"):
        Vector([1, 2]) == [1]


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


def test_pure_kleene_kernels_return_bool_storage_directly():
    left = Vector([True, None, False])._storage
    right = Vector([None, True, None])._storage

    vector_result = python_ops.logical_vector(
        left,
        right,
        vector_ops._kleene_and,
    )
    scalar_result = python_ops.logical_scalar(
        left,
        True,
        vector_ops._kleene_or,
    )
    inverted = python_ops.invert_bool(left)

    assert isinstance(vector_result, BoolStorage)
    assert list(vector_result) == [None, None, False]
    assert isinstance(scalar_result, BoolStorage)
    assert list(scalar_result) == [True, True, True]
    assert isinstance(inverted, BoolStorage)
    assert list(inverted) == [False, None, True]


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
