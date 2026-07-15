"""
Repr must never crash — NaN and infinity are legal float values.

int(float('nan')) raises, so the float formatting path must short-circuit
non-finite values before the integer-ness check.
"""

from serif import Table, Vector


def test_vector_repr_with_nan():
    r = repr(Vector([1.0, float('nan')]))
    assert 'nan' in r
    assert '1.0' in r


def test_vector_repr_with_inf():
    r = repr(Vector([float('inf'), -float('inf'), 2.5]))
    assert 'inf' in r
    assert '-inf' in r


def test_table_repr_with_nan_column():
    r = repr(Table({'x': [1.0, float('nan')], 'y': [1, 2]}))
    assert 'nan' in r


def test_finite_float_formatting_unchanged():
    r = repr(Vector([2.0, 3.5]))
    assert '2.0' in r
    assert '3.5' in r
