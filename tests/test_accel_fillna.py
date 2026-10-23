"""
Same-kind fillna writes the fill straight into the null lanes.

The numpy kernel copies the buffer and assigns the fill under the null
bitmap; the pure path rebuilds element by element. Both must return the
same storage type, values and schema, including coerced fills (an int into
a float column, a date into a datetime column) and the fills the kernel
declines (an int past int64, an aware datetime), which the pure path still
degrades or warns on as before.
"""

from datetime import date
from datetime import datetime
from datetime import timezone

import pytest

from serif import Schema
from serif import Vector
from serif._execution import DECLINED
from serif._vector._numpy import transforms as numpy_transforms


def _pure(fn, monkeypatch):
    with monkeypatch.context() as m:
        m.setattr(numpy_transforms, '_USE_NUMPY', False)
        return fn()


def _assert_identical(pure, fast):
    assert type(fast._storage) is type(pure._storage)
    assert fast.schema() == pure.schema()
    assert fast.vector_name == pure.vector_name
    assert list(fast) == list(pure)
    assert [type(x) for x in fast] == [type(x) for x in pure]


CASES = [
    ("int",          [1, None, 3, None],                 0),
    ("int_bool",     [1, None, 3],                       True),
    ("float",        [1.5, None, -0.0, None],            2.25),
    ("float_int",    [1.5, None],                        0),
    ("float_nan",    [1.5, None],                        float('nan')),
    ("bool",         [True, None, False, None],          False),
    ("date",         [date(2024, 1, 2), None, date(1969, 12, 31)],
                     date(1970, 1, 1)),
    ("datetime",     [datetime(2024, 1, 2, 3), None],    datetime(1, 1, 1)),
    ("datetime_date", [datetime(2024, 1, 2, 3), None],   date(2024, 5, 6)),
    ("dense",        [1, 2, 3],                          0),
    ("all_null_int", Vector([None, None], dtype=Schema(int, True)), 7),
]


def _make(values):
    return values if isinstance(values, Vector) else Vector(values, name='v')


@pytest.mark.parametrize("values,fill", [c[1:] for c in CASES], ids=[c[0] for c in CASES])
def test_fillna_matches_pure_path(values, fill, monkeypatch):
    fast = _make(values).fillna(fill)
    pure = _pure(lambda: _make(values).fillna(fill), monkeypatch)

    if isinstance(fill, float) and fill != fill:   # nan: compare by repr
        assert repr(list(fast)) == repr(list(pure))
        assert type(fast._storage) is type(pure._storage)
    else:
        _assert_identical(pure, fast)
    assert fast.schema().nullable is False


def test_padding_bits_in_the_mask_are_ignored():
    v = Vector([1, None, 3])
    v._storage._mask._buf[0] |= 0b11111000

    assert list(v.fillna(0)) == [1, 0, 3]


def test_int_fill_past_int64_degrades_exactly(monkeypatch):
    huge = 2**80

    fast = Vector([1, None]).fillna(huge)

    assert list(fast) == [1, huge]
    _assert_identical(_pure(lambda: Vector([1, None]).fillna(huge), monkeypatch), fast)


def test_aware_fill_still_falls_back_with_warning():
    aware = datetime(2024, 1, 1, tzinfo=timezone.utc)

    with pytest.warns(UserWarning, match='timezone-aware'):
        result = Vector([datetime(2024, 1, 2), None]).fillna(aware)

    assert list(result) == [datetime(2024, 1, 2), aware]


def test_kernel_engages_for_masked_fixed_width_storage(monkeypatch):
    pytest.importorskip("numpy")
    calls = []
    original = numpy_transforms.fill_nulls

    def spy(storage, fill):
        result = original(storage, fill)
        calls.append(result is not DECLINED)
        return result

    monkeypatch.setattr(numpy_transforms, 'fill_nulls', spy)
    Vector([1, None]).fillna(0)
    Vector([1.5, None]).fillna(0.0)
    Vector([True, None]).fillna(False)
    Vector([date(2024, 1, 1), None]).fillna(date(2024, 1, 2))
    Vector([datetime(2024, 1, 1), None]).fillna(datetime(2024, 1, 2))
    assert calls == [True] * 5

    calls.clear()
    Vector(['a', None]).fillna('b')        # strings keep the pure path
    assert calls == [False]
