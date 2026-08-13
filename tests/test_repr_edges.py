"""
Repr must never crash — NaN and infinity are legal float values.

int(float('nan')) raises, so the float formatting path must short-circuit
non-finite values before the integer-ness check.
"""

from serif import Table, Vector
from serif._vector.storage import ArrayStorage


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


def test_vector_repr_reads_only_displayed_head_and_tail(monkeypatch):
    vector = Vector(range(100))
    storage = vector._storage
    positions = []
    original_getitem = ArrayStorage.__getitem__

    def recording_getitem(self, index):
        if self is storage:
            positions.append(index)
        return original_getitem(self, index)

    def forbidden_full_read(*args, **kwargs):
        raise AssertionError('repr converted or iterated the complete column')

    monkeypatch.setattr(ArrayStorage, '__getitem__', recording_getitem)
    monkeypatch.setattr(ArrayStorage, '__iter__', forbidden_full_read)
    monkeypatch.setattr(ArrayStorage, 'to_tuple', forbidden_full_read)

    rendered = repr(vector)

    assert positions == [0, 1, 2, 3, 4, 5, 94, 95, 96, 97, 98, 99]
    assert '...' in rendered
