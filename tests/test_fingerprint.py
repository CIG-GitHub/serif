"""Tests for Serif's single, deterministic fingerprint contract."""

import os
import subprocess
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

from serif import Schema, SerifTypeError, Table, Vector
from serif._vector.storage import DecimalStorage


def test_fingerprint_returns_blake2_hex_digest():
    digest = Vector([1, 2, 3]).fingerprint()

    assert isinstance(digest, str)
    assert len(digest) == 64
    int(digest, 16)


def test_fingerprint_is_stable_across_hash_seeds_and_processes():
    code = (
        "from serif import Vector; "
        "print(Vector(['analyst', 'flow']).fingerprint())"
    )
    results = []
    for seed in ('1', '987654'):
        env = dict(os.environ, PYTHONHASHSEED=seed)
        results.append(subprocess.check_output(
            [sys.executable, '-c', code], text=True, env=env).strip())

    assert results[0] == results[1]


def test_fingerprint_is_stable_for_repeated_calls_and_copies():
    vector = Vector([1, 2, 3], name='values')

    assert vector.fingerprint() == vector.fingerprint()
    assert vector.fingerprint() == vector.copy().fingerprint()


def test_fingerprint_changes_with_values_and_restores_on_revert():
    vector = Vector([1, 2, 3])
    original = vector.fingerprint()

    vector[1] = 99
    assert vector.fingerprint() != original

    vector[1] = 2
    assert vector.fingerprint() == original


def test_fingerprint_includes_dtype_nullability_and_vector_name():
    plain = Vector([1], dtype=Schema(int, False), name='x')
    nullable = Vector([1], dtype=Schema(int, True), name='x')
    floating = Vector([1.0], name='x')
    renamed = Vector([1], name='y')

    assert len({
        vector.fingerprint()
        for vector in (plain, nullable, floating, renamed)
    }) == 4


def test_table_fingerprint_includes_shape_names_values_and_table_name():
    a = Table({'a': [1, 2]})
    b = Table({'b': [1, 2]})
    c = Table({'a': [1, 3]})
    named = Table({'a': [1, 2]}, name='model_input')

    assert len({
        a.fingerprint(),
        b.fingerprint(),
        c.fingerprint(),
        named.fingerprint(),
    }) == 4
    assert a.fingerprint() != Vector([1, 2]).fingerprint()


def test_table_column_replacement_changes_fingerprint():
    table = Table({'a': [1, 2]})
    before = table.fingerprint()

    table.a = [3, 4]

    assert table.fingerprint() != before


def test_categorical_order_is_part_of_fingerprint():
    left = Vector(['a', 'b']).categorize(['a', 'b'])
    right = Vector(['a', 'b']).categorize(['b', 'a'])

    assert list(left) == list(right)
    assert left.fingerprint() != right.fingerprint()


def test_decimal_storage_metadata_is_part_of_fingerprint():
    values = [Decimal('1.20')]
    scale_2 = Vector._from_storage(
        DecimalStorage.from_iterable(values, scale=2, precision=3),
        Schema(Decimal, False),
    )
    scale_3 = Vector._from_storage(
        DecimalStorage.from_iterable(values, scale=3, precision=4),
        Schema(Decimal, False),
    )

    assert list(scale_2) == list(scale_3)
    assert scale_2.fingerprint() != scale_3.fingerprint()


@pytest.mark.parametrize(
    ('left', 'right'),
    [
        (float('nan'), float('nan')),
        (-0.0, 0.0),
        (complex(1, 2), complex(1, 2)),
        (date(2024, 1, 1), date(2024, 1, 1)),
        (datetime(2024, 1, 1, 12, 30), datetime(2024, 1, 1, 12, 30)),
        (timedelta(days=2, microseconds=3), timedelta(days=2, microseconds=3)),
        (Decimal('1.20'), Decimal('1.20')),
        ([1, 2], [1, 2]),
        ((1, 2), (1, 2)),
        ({1, 2}, {2, 1}),
        (frozenset({1, 2}), frozenset({2, 1})),
        ({'a': 1, 'b': 2}, {'b': 2, 'a': 1}),
        (b'abc', b'abc'),
        (bytearray(b'abc'), bytearray(b'abc')),
    ],
)
def test_supported_values_have_canonical_fingerprints(left, right):
    assert (
        Vector([left], dtype=object).fingerprint()
        == Vector([right], dtype=object).fingerprint()
    )


def test_unknown_object_raises_instead_of_hashing_repr():
    class AddressBearingRepr:
        pass

    vector = Vector([AddressBearingRepr()], dtype=object)

    with pytest.raises(SerifTypeError, match="does not know how to encode"):
        vector.fingerprint()


def test_semantic_fingerprint_api_does_not_exist():
    assert not hasattr(Vector([1]), 'semantic_fingerprint')
