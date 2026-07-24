"""Known-result element transforms build canonical storage directly."""

from datetime import date
from datetime import datetime
from datetime import timedelta

import pytest

from serif import Schema
from serif import SerifValueError
from serif import Vector
from serif._vector.storage import ArrayStorage
from serif._vector.storage import BoolStorage
from serif._vector.storage import StringStorage
from serif._vector.storage import TupleStorage


def test_fixed_string_methods_emit_known_storage():
    strings = Vector(['alpha', None, 'Gamma'])

    upper = strings.upper()
    found = strings.find('a')
    counted = strings.count('a')
    alpha = strings.isalpha()
    encoded = strings.encode()

    assert isinstance(upper._storage, StringStorage)
    assert upper.schema() == Schema(str, True)
    assert list(upper) == ['ALPHA', None, 'GAMMA']
    assert isinstance(found._storage, ArrayStorage)
    assert found._storage._data.typecode == 'q'
    assert list(found) == [0, None, 1]
    assert isinstance(counted._storage, ArrayStorage)
    assert list(counted) == [2, None, 2]
    assert isinstance(alpha._storage, BoolStorage)
    assert list(alpha) == [True, None, True]
    assert isinstance(encoded._storage, TupleStorage)
    assert encoded.schema() == Schema(bytes, True)
    assert list(encoded) == [b'alpha', None, b'Gamma']


def test_string_helpers_and_container_results_emit_known_storage():
    strings = Vector(['a-b', None])

    before = strings.before('-')
    split = strings.split('-')

    assert isinstance(before._storage, StringStorage)
    assert list(before) == ['a', None]
    assert isinstance(split._storage, TupleStorage)
    assert split.schema() == Schema(list, True)
    assert list(split) == [['a', 'b'], None]


def test_empty_fixed_transform_keeps_its_known_result_kind():
    result = Vector([], dtype=str).upper()

    assert isinstance(result._storage, StringStorage)
    assert result.schema() == Schema(str, False)
    assert result._wild is True


def test_fixed_date_elements_emit_known_storage():
    dates = Vector([date(2024, 2, 10), None])

    years = dates.year
    formatted = dates.isoformat()
    ordinals = dates.toordinal()
    replaced = dates.replace(day=1)
    month_ends = dates.eomonth()

    assert isinstance(years._storage, ArrayStorage)
    assert list(years) == [2024, None]
    assert isinstance(formatted._storage, StringStorage)
    assert list(formatted) == ['2024-02-10', None]
    assert isinstance(ordinals._storage, ArrayStorage)
    assert list(ordinals) == [date(2024, 2, 10).toordinal(), None]
    assert isinstance(replaced._storage, TupleStorage)
    assert replaced.schema() == Schema(date, True)
    assert list(replaced) == [date(2024, 2, 1), None]
    assert isinstance(month_ends._storage, TupleStorage)
    assert list(month_ends) == [date(2024, 2, 29), None]


def test_known_date_arithmetic_and_special_comparisons_emit_storage():
    dates = Vector([date(2024, 1, 2), None])

    shifted = dates + timedelta(days=1)
    days = dates - date(2024, 1, 1)
    compared = dates == '2024-01-02'

    assert isinstance(shifted._storage, TupleStorage)
    assert shifted.schema() == Schema(date, True)
    assert list(shifted) == [date(2024, 1, 3), None]
    assert isinstance(days._storage, ArrayStorage)
    assert days.schema() == Schema(int, True)
    assert list(days) == [1, None]
    assert isinstance(compared._storage, BoolStorage)
    assert compared.schema() == Schema(bool, True)
    assert list(compared) == [True, None]


def test_typed_cast_builds_final_storage_and_preserves_metadata():
    source = Vector(['1', None, '3'], name='source')

    result = source.cast(int)

    assert isinstance(result._storage, ArrayStorage)
    assert result.schema() == Schema(int, True)
    assert result.vector_name == 'source'
    assert result._wild is True
    assert list(result) == [1, None, 3]


def test_typed_cast_keeps_python_int_overflow_degradation():
    huge = 2 ** 80

    result = Vector([str(huge), '2']).cast(int)

    assert isinstance(result._storage, TupleStorage)
    assert result.schema() == Schema(int, False)
    assert list(result) == [huge, 2]


def test_typed_cast_preserves_indexed_conversion_diagnostic():
    with pytest.raises(SerifValueError, match=r"Cast failed at index 1"):
        Vector(['1', 'not-an-int']).cast(int)


def test_fillna_builds_known_storage_for_same_kind_and_promotion():
    integers = Vector([1, None, 3], name='values')

    filled = integers.fillna(0)
    promoted = integers.fillna(2.5)

    assert isinstance(filled._storage, ArrayStorage)
    assert filled.schema() == Schema(int, False)
    assert filled.vector_name == 'values'
    assert list(filled) == [1, 0, 3]
    assert isinstance(promoted._storage, ArrayStorage)
    assert promoted._storage._data.typecode == 'd'
    assert promoted.schema() == Schema(float, False)
    assert promoted.vector_name == 'values'
    assert list(promoted) == [1.0, 2.5, 3.0]


def test_fillna_date_to_datetime_promotion_builds_tuple_storage():
    source = Vector([date(2024, 1, 1), None])

    result = source.fillna(datetime(2024, 1, 2, 12))

    assert isinstance(result._storage, TupleStorage)
    assert result.schema() == Schema(datetime, False)
    assert list(result) == [
        datetime(2024, 1, 1),
        datetime(2024, 1, 2, 12),
    ]
