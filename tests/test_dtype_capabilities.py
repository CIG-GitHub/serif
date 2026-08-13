"""Curated scalar-derived capabilities on typed Vectors and Rows."""

from datetime import date

import pytest

from serif import Table
from serif import Vector


def test_date_properties_are_explicit_elementwise_capabilities():
    dates = Vector([date(2024, 1, 15), None, date(2024, 12, 25)])

    assert list(dates.year) == [2024, None, 2024]
    assert list(dates.month) == [1, None, 12]
    assert list(dates.day) == [15, None, 25]


def test_date_methods_are_explicit_elementwise_capabilities():
    dates = Vector([date(2024, 1, 15), None, date(2024, 12, 25)])

    assert list(dates.replace(year=2025)) == [
        date(2025, 1, 15),
        None,
        date(2025, 12, 25),
    ]
    assert list(dates.strftime('%Y-%m-%d')) == [
        '2024-01-15',
        None,
        '2024-12-25',
    ]
    assert list(dates.isoformat()) == [
        '2024-01-15',
        None,
        '2024-12-25',
    ]


def test_string_methods_are_explicit_elementwise_capabilities():
    words = Vector(['hello', None, 'world'])

    assert list(words.replace('o', '0')) == ['hell0', None, 'w0rld']
    assert list(words.upper()) == ['HELLO', None, 'WORLD']
    assert list(words.islower()) == [True, None, True]


def test_integer_capabilities_are_explicit_and_null_preserving():
    integers = Vector([1, None, 3, 4])

    assert list(integers.real) == [1, None, 3, 4]
    assert list(integers.imag) == [0, None, 0, 0]
    assert list(integers.bit_length()) == [1, None, 2, 3]
    assert list(integers.bit_count()) == [1, None, 2, 1]
    assert list(integers.to_bytes(1, 'big')) == [
        b'\x01',
        None,
        b'\x03',
        b'\x04',
    ]


def test_float_capabilities_are_explicit_and_null_preserving():
    floats = Vector([1.0, None, 2.5, 3.0])

    assert list(floats.real) == [1.0, None, 2.5, 3.0]
    assert list(floats.imag) == [0.0, None, 0.0, 0.0]
    assert list(floats.as_integer_ratio()) == [
        (1, 1),
        None,
        (5, 2),
        (3, 1),
    ]
    assert list(floats.is_integer()) == [True, None, False, True]
    assert list(floats.hex()) == [
        float.hex(1.0),
        None,
        float.hex(2.5),
        float.hex(3.0),
    ]


@pytest.mark.parametrize(
    'vector,capabilities',
    [
        (Vector([1]), ('bit_length', 'bit_count', 'to_bytes', 'real', 'imag')),
        (Vector([1.5]), ('as_integer_ratio', 'is_integer', 'hex', 'real', 'imag')),
        (Vector(['a']), ('upper', 'replace', 'islower')),
        (Vector([date(2024, 1, 1)]), ('year', 'isoformat', 'replace')),
    ],
)
def test_capabilities_are_discoverable(vector, capabilities):
    available = dir(vector)

    assert all(capability in available for capability in capabilities)


def test_categorical_uses_the_curated_string_capabilities():
    categorical = Vector(['alpha', None, 'Beta']).categorize()

    assert 'upper' in dir(categorical)
    assert list(categorical.upper()) == ['ALPHA', None, 'BETA']


@pytest.mark.parametrize(
    'vector,attribute',
    [
        (Vector([object()]), 'some_attr'),
        (Vector([1]), 'conjugate'),
        (Vector([1]), 'from_bytes'),
        (Vector(['a']), 'maketrans'),
        (Vector([date(2024, 1, 1)]), 'today'),
        (Vector([date(2024, 1, 1)]), 'nonexistent_method'),
    ],
)
def test_undeclared_scalar_attributes_are_not_vector_capabilities(
    vector,
    attribute,
):
    with pytest.raises(AttributeError, match=attribute):
        getattr(vector, attribute)


def test_homogeneous_rows_share_curated_dtype_capabilities():
    strings = Table({'left': ['hello'], 'right': ['world']})[0]
    dates = Table({
        'left': [date(2024, 1, 1)],
        'right': [date(2025, 1, 1)],
    })[0]

    assert list(strings.upper()) == ['HELLO', 'WORLD']
    assert list(dates.year) == [2024, 2025]
