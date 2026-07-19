"""
Conformance tests for the OPTIONAL pyarrow-accelerated read path.

The guarantee under test — python in → python out, backend-independent:
for any file BOTH paths accept, read_parquet must return an IDENTICAL
Table whether pyarrow is installed or not. Same values, same nulls in the
same slots, same schema, and every surfaced value a concrete Python type
(int/float/str — never a pyarrow or numpy scalar).

Also under test: accelerators may widen transport, never semantics — any
column type outside the arrow path's subset declines the whole file to the
pure reader.

Skipped entirely when pyarrow isn't installed.
"""
import os
import tempfile
from datetime import date, datetime
from decimal import Decimal

import pytest

pa = pytest.importorskip("pyarrow")
import pyarrow.parquet as pq

from serif import Table, Vector
from serif.errors import SerifTypeError
import serif.io.parquet as parquet_mod
from serif.io import _arrow


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write(t):
    path = tempfile.mktemp(suffix='.parquet')
    t.to_parquet(path)
    return path


def _read_pure(path):
    saved = parquet_mod._USE_ARROW
    parquet_mod._USE_ARROW = False
    try:
        return parquet_mod.read_parquet(path)
    finally:
        parquet_mod._USE_ARROW = saved


def _assert_identical(pure_t, arrow_t):
    """Cell-for-cell equality plus the exact-concrete-type boundary check."""
    assert arrow_t is not None, "arrow path declined a file it should accept"
    assert arrow_t.column_names() == pure_t.column_names()
    assert len(arrow_t) == len(pure_t)
    for name in pure_t.column_names():
        pv, av = pure_t[name], arrow_t[name]
        assert av.schema().kind is pv.schema().kind, name
        assert av.schema().nullable is pv.schema().nullable, name
        # Schema carries only (kind, nullable); a decimal's scale/precision
        # live in DecimalStorage, so the two paths must agree there too or a
        # column could round-trip the right values at the wrong exponent.
        if pv.schema().kind is Decimal:
            from serif._vector.storage import DecimalStorage
            ps, as_ = pv._storage, av._storage
            assert isinstance(ps, DecimalStorage), f"{name}: pure not DecimalStorage"
            assert isinstance(as_, DecimalStorage), f"{name}: arrow not DecimalStorage"
            assert as_._scale == ps._scale, \
                f"{name}: scale {as_._scale} != {ps._scale}"
            assert as_._precision == ps._precision, \
                f"{name}: precision {as_._precision} != {ps._precision}"
        for i, (p, a) in enumerate(zip(pv, av)):
            if p is None:
                assert a is None, f"{name}[{i}]: expected None, got {a!r}"
            else:
                assert a == p, f"{name}[{i}]: {a!r} != {p!r}"
                # python in → python out: concrete Python types only, no
                # pyarrow/numpy scalars leaking through the boundary
                assert type(a) is type(p), f"{name}[{i}]: {type(a)} vs {type(p)}"
                assert type(a) in (int, float, str, bool, date, datetime, Decimal), \
                    f"{name}[{i}]: {type(a)}"


def _conform(t):
    path = _write(t)
    try:
        _assert_identical(_read_pure(path), _arrow.try_read(path))
    finally:
        if os.path.exists(path):
            os.unlink(path)


# ---------------------------------------------------------------------------
# Conformance: both paths must return identical Tables
# ---------------------------------------------------------------------------

def test_conformance_dense_columns():
    _conform(Table({
        'n': [1, 2, 3, -9_007_199_254_740_993],   # beyond float53 exactness
        'f': [1.5, -2.25, 0.0, 1e300],
        's': ['alice', '', '日本語 🎉', 'carol'],
    }))


def test_conformance_nullable_columns():
    _conform(Table({
        'n': [10, None, 30, None],
        'f': [None, 2.5, None, -0.0],
        's': [None, 'bob', '', None],
    }))


def test_conformance_null_heavy_and_edges():
    _conform(Table({
        'n': [None, None, None, 7],
        's': ['', None, '', None],
    }))


def test_conformance_larger_table():
    n = 5_000
    _conform(Table({
        'n': [i if i % 7 else None for i in range(n)],
        'f': [i * 0.5 if i % 3 else None for i in range(n)],
        's': [f'row_{i}' if i % 5 else None for i in range(n)],
    }))


def test_dispatch_returns_same_table_either_way():
    # Wiring check: public read_parquet with the switch on vs off.
    t = Table({'n': [1, None, 3], 's': ['a', 'b', None]})
    path = _write(t)
    try:
        pure_t = _read_pure(path)
        saved = parquet_mod._USE_ARROW
        parquet_mod._USE_ARROW = True
        try:
            dispatched = parquet_mod.read_parquet(path)
        finally:
            parquet_mod._USE_ARROW = saved
        _assert_identical(pure_t, dispatched)
    finally:
        if os.path.exists(path):
            os.unlink(path)


def test_dispatch_carries_serif_mask_into_projected_arrow_batches():
    path = tempfile.mktemp(suffix='.parquet')
    pq.write_table(pa.table({
        'a': [1, 2, 3, 4, 5, 6],
        'payload': ['a', 'b', 'c', 'd', 'e', 'f'],
    }), path, row_group_size=2)
    try:
        source = parquet_mod.read_parquet(path)
        selected = source[source.a > 4]
        assert list(selected.payload) == ['e', 'f']
        assert source._mat is None
        assert selected._mat is None
    finally:
        if os.path.exists(path):
            os.unlink(path)


# ---------------------------------------------------------------------------
# The full serif-writable type set conforms — every serif-written file
# takes the fast path
# ---------------------------------------------------------------------------

def test_conformance_bool_date_datetime():
    _conform(Table({
        'b': [True, False, None, True],
        'd': [date(2024, 1, 1), None, date(2025, 6, 15), date(1969, 12, 31)],
        'ts': [datetime(2024, 1, 1, 12, 30, 45, 123456), None,
               datetime(1969, 6, 1, 0, 0, 0, 1), datetime(2030, 12, 31, 23, 59)],
    }))


def test_conformance_all_six_kinds_together():
    _conform(Table({
        'n': [1, None, 3],
        'f': [1.5, 2.5, None],
        's': ['a', None, 'c'],
        'b': [None, True, False],
        'd': [date(2024, 1, 1), date(2024, 2, 2), None],
        'ts': [None, datetime(2024, 1, 1), datetime(2025, 1, 1, 6)],
    }))


def test_conformance_decimal():
    # decimal128 (16-byte FIXED_LEN_BYTE_ARRAY) now conforms: both paths
    # reinflate the same DecimalStorage — same values, same scale/precision,
    # nulls in the same slots. Two columns with different scales prove scale
    # is tracked per column.
    _conform(Table({
        'amt':  [Decimal('123.45'), Decimal('-0.01'), Decimal('999999.99'), None],
        'rate': [Decimal('0.001'), Decimal('1.500'), None, Decimal('-2.750')],
    }))


def test_conformance_without_numpy(monkeypatch):
    # numpy is OPTIONAL: pyarrow >= 25 no longer requires it, so the
    # accelerator must run with pyarrow alone. The numpy-accelerated steps
    # are the decimal byte-swap and the bool bit-unpack (validity bitmaps
    # memcpy straight into BitMask, numpy or not) — force the pure-Python
    # fallbacks even where numpy IS installed so CI covers them. The other
    # columns confirm the rest of the reader never needs numpy at all.
    # Same identical-to-pure-reader guarantee.
    monkeypatch.setattr(_arrow, '_np', None)
    _conform(Table({
        'n':   [10, None, 30, None],
        'f':   [None, 2.5, None, -0.0],
        's':   [None, 'bob', '', 'δ 🎉'],
        'b':   [True, None, False, True],
        'amt': [Decimal('123.45'), Decimal('-0.01'), None, Decimal('999999.99')],
    }))


# ---------------------------------------------------------------------------
# Exact widenings: foreign types the pure reader ALSO accepts take the fast
# path via a C-level cast — int8/16/32, uint8/16 → int64; float32 → float64;
# large_string → string; date64 → date32. Same values out of both readers.
# ---------------------------------------------------------------------------

def _write_foreign(arrow_table, **write_kwargs):
    """Write with pyarrow such that the PURE reader can also read the file
    (PLAIN encoding, no compression), so conformance can compare both paths."""
    path = tempfile.mktemp(suffix='.parquet')
    pq.write_table(arrow_table, path, use_dictionary=False,
                   compression='NONE', **write_kwargs)
    return path


def _conform_foreign(arrow_table):
    path = _write_foreign(arrow_table)
    try:
        _assert_identical(_read_pure(path), _arrow.try_read(path))
    finally:
        os.unlink(path)


def test_conformance_widened_int_and_float_types():
    _conform_foreign(pa.table({
        'i8':  pa.array([-5, None, 127], type=pa.int8()),
        'i16': pa.array([-300, 300, None], type=pa.int16()),
        'i32': pa.array([None, -70_000, 70_000], type=pa.int32()),
        'u8':  pa.array([0, 255, None], type=pa.uint8()),
        'u16': pa.array([0, None, 65_535], type=pa.uint16()),
        'f32': pa.array([1.5, None, -2.25], type=pa.float32()),
    }))


def test_conformance_large_string():
    _conform_foreign(pa.table({
        's': pa.array(['alice', None, '日本語 🎉', ''], type=pa.large_string()),
    }))


def test_conformance_date64():
    _conform_foreign(pa.table({
        'd': pa.array([date(2024, 1, 1), None, date(1969, 12, 31)],
                      type=pa.date64()),
    }))


def test_conformance_tz_aware_timestamp():
    # Both readers surface the same NAIVE datetimes: the pure reader decodes
    # raw UTC micros, the arrow path casts the zone away keeping the same
    # stored instant.
    _conform_foreign(pa.table({
        'ts': pa.array([datetime(2024, 1, 1, 12, 30), None],
                       type=pa.timestamp('us', tz='America/New_York')),
    }))


def test_dictionary_string_column_reads_as_plain_str():
    # Parquet dictionary encoding is compression, not a categorical claim:
    # the column comes back as a plain str Vector with the decoded values.
    # (The pure reader cannot read dictionary pages at all, so this is pure
    # transport widening — no conformance pair exists to compare against.)
    path = tempfile.mktemp(suffix='.parquet')
    pq.write_table(pa.table({'c': pa.array(['x', 'y', None, 'x']).dictionary_encode()}),
                   path)
    try:
        t = parquet_mod.read_parquet(path)
        assert list(t['c']) == ['x', 'y', None, 'x']
        assert t['c'].schema().kind is str
        assert all(type(v) is str for v in t['c'] if v is not None)
    finally:
        os.unlink(path)


def test_int32_with_default_pyarrow_writer_options():
    # Regression: an int32 column in a default-written file (snappy +
    # dictionary) used to decline the WHOLE file to the pure reader, which
    # then failed on the encoding — making the file unreadable outright.
    path = tempfile.mktemp(suffix='.parquet')
    pq.write_table(pa.table({
        'a': pa.array([1, None, 3], type=pa.int32()),
        'b': pa.array([1.5, 2.5, None]),
    }), path)
    try:
        t = parquet_mod.read_parquet(path)
        assert list(t['a']) == [1, None, 3]
        assert list(t['b']) == [1.5, 2.5, None]
        assert t['a'].schema().kind is int
    finally:
        os.unlink(path)


def test_uint32_still_declines():
    # The pure reader rejects UINT_32 (decoding it as its INT32 physical
    # sign-flips large values). pyarrow could read it faithfully; semantics
    # stay serif's, so the arrow path must decline and surface that refusal.
    path = _write_foreign(pa.table({'u': pa.array([1, 2], type=pa.uint32())}))
    try:
        assert _arrow.try_read(path) is None
        with pytest.raises(SerifTypeError, match='UINT_32'):
            parquet_mod.read_parquet(path)
    finally:
        os.unlink(path)


def test_arrow_conversion_failure_declines_but_serif_bugs_stay_loud(monkeypatch):
    # Arrow-layer errors during conversion (cast overflow, concat past 2 GB)
    # decline to the pure reader like any parse failure; non-arrow exceptions
    # are serif bugs and must propagate.
    t = Table({'n': [1, 2, 3]})
    path = _write(t)
    try:
        def _arrow_boom(arr, name, nullable):
            raise pa.lib.ArrowInvalid('synthetic conversion failure')
        monkeypatch.setattr(_arrow, '_to_vector', _arrow_boom)
        assert _arrow.try_read(path) is None

        def _serif_boom(arr, name, nullable):
            raise IndexError('synthetic serif bug')
        monkeypatch.setattr(_arrow, '_to_vector', _serif_boom)
        with pytest.raises(IndexError):
            _arrow.try_read(path)
    finally:
        os.unlink(path)


# ---------------------------------------------------------------------------
# Semantics stay serif's: types serif rejects decline the WHOLE file, so
# the pure reader's refusals surface identically with pyarrow installed
# ---------------------------------------------------------------------------

def test_nanosecond_timestamps_still_raise():
    # TIMESTAMP(NANOS) is a semantic rejection (truncation would silently
    # change data). pyarrow could read it; serif must not.
    path = tempfile.mktemp(suffix='.parquet')
    pq.write_table(pa.table({'t': pa.array(
        [1, 2], type=pa.timestamp('ns'))}), path)
    try:
        assert _arrow.try_read(path) is None
        with pytest.raises(SerifTypeError, match='NANOS'):
            parquet_mod.read_parquet(path)
    finally:
        os.unlink(path)


def test_decimal256_declines():
    # serif has no 256-bit backend, so a decimal256 column declines the WHOLE
    # file to the pure reader, which raises its own error (only decimal128 /
    # 16-byte FIXED_LEN_BYTE_ARRAY is supported). 256-bit is a future PR.
    path = tempfile.mktemp(suffix='.parquet')
    pq.write_table(pa.table({'big': pa.array(
        [1234, 567], type=pa.decimal256(40, 2))}), path)
    try:
        assert _arrow.try_read(path) is None
        with pytest.raises(SerifTypeError, match='DECIMAL'):
            parquet_mod.read_parquet(path)
    finally:
        os.unlink(path)


def test_arrow_declines_garbage_file():
    # Parse failures decline too — serif's own error messages surface.
    path = tempfile.mktemp(suffix='.parquet')
    with open(path, 'wb') as f:
        f.write(b'PAR1 this is not a parquet file PAR1')
    try:
        assert _arrow.try_read(path) is None
    finally:
        os.unlink(path)
