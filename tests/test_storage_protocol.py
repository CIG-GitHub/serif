"""
Storage-protocol conformance suite.

Every behavior in this file must hold regardless of which storage backend a
Vector happens to be using (TupleStorage, ArrayStorage, StringStorage,
BoolStorage, _CategoryStorage). Backends are exercised only through the
public Vector API —
if a base-class method reaches into a backend's internals it doesn't own,
this suite is where it breaks.

Each case is (id, factory, expected_values). Factories build a fresh vector
per test so mutation tests can't leak between cases.
"""

from datetime import date

import pytest

from serif import Vector
from serif.errors import SerifValueError
from serif._vector.storage import ArrayStorage, TupleStorage, StringStorage, BoolStorage
from serif._vector.categorical import _Category, _CategoryStorage


CASES = [
    ("int_dense",   lambda: Vector([3, 1, 2]),                [3, 1, 2]),
    ("int_null",    lambda: Vector([3, None, 2]),             [3, None, 2]),
    ("float_dense", lambda: Vector([3.0, 1.5, 2.25]),         [3.0, 1.5, 2.25]),
    ("float_null",  lambda: Vector([3.0, None, 2.25]),        [3.0, None, 2.25]),
    ("str_dense",   lambda: Vector(["b", "a", "c"]),          ["b", "a", "c"]),
    ("str_null",    lambda: Vector(["b", None, "a"]),         ["b", None, "a"]),
    ("str_unicode", lambda: Vector(["héllo", "🐍snake", ""]), ["héllo", "🐍snake", ""]),
    ("bool_dense",  lambda: Vector([True, False, True]),      [True, False, True]),
    ("bool_null",   lambda: Vector([True, None, False]),      [True, None, False]),
    ("date_dense",
     lambda: Vector([date(2024, 1, 3), date(2024, 1, 1), date(2024, 1, 2)]),
     [date(2024, 1, 3), date(2024, 1, 1), date(2024, 1, 2)]),
    ("date_null",
     lambda: Vector([date(2024, 1, 3), None, date(2024, 1, 1)]),
     [date(2024, 1, 3), None, date(2024, 1, 1)]),
    ("cat",
     lambda: Vector(["b", "a", "b"]).categorize(["a", "b"]),
     ["b", "a", "b"]),
    ("cat_null",
     lambda: Vector(["b", None, "a"]).categorize(["a", "b"]),
     ["b", None, "a"]),
]

IDS = [c[0] for c in CASES]

# Cases whose sort semantics are plain value order (categoricals sort by
# category order and have their own suite in test_categorical.py).
SORTABLE = [c for c in CASES if not c[0].startswith("cat")]
SORTABLE_IDS = [c[0] for c in SORTABLE]

NUMERIC = [c for c in CASES if c[0].startswith(("int", "float"))]
NUMERIC_IDS = [c[0] for c in NUMERIC]

# A same-dtype replacement value for setitem tests, per case id.
SETITEM_VALUE = {
    "int_dense": 9, "int_null": 9,
    "float_dense": 9.5, "float_null": 9.5,
    "str_dense": "z", "str_null": "z", "str_unicode": "z",
    "bool_dense": False, "bool_null": False,
    "date_dense": date(2020, 1, 1), "date_null": date(2020, 1, 1),
    "cat": "a", "cat_null": "a",
}


def _params(cases, ids):
    return pytest.mark.parametrize("factory,values", [c[1:] for c in cases], ids=ids)


# ---------------------------------------------------------------------------
# Backend dispatch — pins which storage each dtype routes to on this branch.
# ---------------------------------------------------------------------------

EXPECTED_BACKEND = {
    "int_dense": ArrayStorage, "int_null": ArrayStorage,
    "float_dense": ArrayStorage, "float_null": ArrayStorage,
    "str_dense": StringStorage, "str_null": StringStorage,
    "str_unicode": StringStorage,
    "bool_dense": BoolStorage, "bool_null": BoolStorage,
    "date_dense": TupleStorage, "date_null": TupleStorage,
    "cat": _CategoryStorage, "cat_null": _CategoryStorage,
}


@pytest.mark.parametrize("case_id,factory", [(c[0], c[1]) for c in CASES], ids=IDS)
def test_expected_backend(case_id, factory):
    v = factory()
    assert isinstance(v._storage, EXPECTED_BACKEND[case_id])


# ---------------------------------------------------------------------------
# Read path
# ---------------------------------------------------------------------------

@_params(CASES, IDS)
def test_iteration_roundtrip(factory, values):
    assert list(factory()) == values


@_params(CASES, IDS)
def test_len(factory, values):
    assert len(factory()) == len(values)


@_params(CASES, IDS)
def test_getitem_int(factory, values):
    v = factory()
    assert v[0] == values[0]
    assert v[len(values) - 1] == values[-1]
    assert v[-1] == values[-1]
    with pytest.raises(IndexError):
        v[len(values)]


@_params(CASES, IDS)
def test_slice(factory, values):
    v = factory()
    assert list(v[1:]) == values[1:]
    assert list(v[::2]) == values[::2]
    assert list(v[::-1]) == values[::-1]
    assert list(v[10:]) == []


@_params(CASES, IDS)
def test_slice_preserves_kind(factory, values):
    v = factory()
    assert v[1:].schema().kind is v.schema().kind


@_params(CASES, IDS)
def test_boolean_mask(factory, values):
    v = factory()
    mask = [True, False, True][: len(values)]
    expected = [x for x, keep in zip(values, mask) if keep]
    assert list(v[mask]) == expected


@_params(CASES, IDS)
def test_first_last(factory, values):
    v = factory()
    assert v.first() == values[0]
    assert v.last() == values[-1]


# ---------------------------------------------------------------------------
# Null handling
# ---------------------------------------------------------------------------

@_params(CASES, IDS)
def test_isna(factory, values):
    assert list(factory().isna()) == [x is None for x in values]


@_params(CASES, IDS)
def test_dropna_values(factory, values):
    result = factory().dropna()
    assert list(result) == [x for x in values if x is not None]


@_params(CASES, IDS)
def test_dropna_clears_nullable(factory, values):
    assert factory().dropna().schema().nullable is False


def test_dropna_on_untyped_empty_vector():
    v = Vector()
    assert list(v.dropna()) == []


@_params(CASES, IDS)
def test_fillna_values(factory, values):
    v = factory()
    kind = v.schema().kind
    fill = {int: 0, float: 0.0, str: "x", bool: False, date: date(1970, 1, 1)}[kind]
    assert list(v.fillna(fill)) == [fill if x is None else x for x in values]


# ---------------------------------------------------------------------------
# Sorting
# ---------------------------------------------------------------------------

@_params(SORTABLE, SORTABLE_IDS)
def test_sort_ascending_na_last(factory, values):
    non_null = sorted(x for x in values if x is not None)
    nones = [None] * (len(values) - len(non_null))
    assert list(factory().sort_by()) == non_null + nones


@_params(SORTABLE, SORTABLE_IDS)
def test_sort_descending_na_last(factory, values):
    non_null = sorted((x for x in values if x is not None), reverse=True)
    nones = [None] * (len(values) - len(non_null))
    assert list(factory().sort_by(reverse=True)) == non_null + nones


@_params(SORTABLE, SORTABLE_IDS)
def test_sort_ascending_na_first(factory, values):
    non_null = sorted(x for x in values if x is not None)
    nones = [None] * (len(values) - len(non_null))
    assert list(factory().sort_by(na_last=False)) == nones + non_null


@_params(SORTABLE, SORTABLE_IDS)
def test_sort_preserves_kind_and_source(factory, values):
    v = factory()
    result = v.sort_by()
    assert result.schema().kind is v.schema().kind
    assert list(v) == values  # source unchanged


# ---------------------------------------------------------------------------
# Uniqueness / copying / conversion
# ---------------------------------------------------------------------------

@_params(CASES, IDS)
def test_unique(factory, values):
    seen, expected = set(), []
    for x in values:
        if x not in seen:
            seen.add(x)
            expected.append(x)
    assert list(factory().unique()) == expected


@pytest.mark.parametrize("case_id,factory,values", CASES, ids=IDS)
def test_copy_is_independent(case_id, factory, values):
    v = factory()
    c = v.copy()
    c[0] = SETITEM_VALUE[case_id]
    assert list(v) == values


@_params(CASES, IDS)
def test_to_object(factory, values):
    obj = factory().to_object()
    assert list(obj) == values
    assert obj.schema().kind is object


# ---------------------------------------------------------------------------
# Concatenation
# ---------------------------------------------------------------------------

@_params(CASES, IDS)
def test_concat_lshift(factory, values):
    assert list(factory() << factory()) == values + values


# ---------------------------------------------------------------------------
# Mutation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("case_id,factory,values", CASES, ids=IDS)
def test_setitem_single_index(case_id, factory, values):
    v = factory()
    v[0] = SETITEM_VALUE[case_id]
    assert v[0] == SETITEM_VALUE[case_id]
    assert list(v)[1:] == values[1:]


@pytest.mark.parametrize("case_id,factory,values", CASES, ids=IDS)
def test_setitem_boolean_mask(case_id, factory, values):
    v = factory()
    mask = [True] + [False] * (len(values) - 1)
    v[mask] = SETITEM_VALUE[case_id]
    assert v[0] == SETITEM_VALUE[case_id]
    assert list(v)[1:] == values[1:]


@pytest.mark.parametrize("case_id,factory,values", CASES, ids=IDS)
def test_setitem_then_full_readback(case_id, factory, values):
    """A mutated vector must stay coherent: iteration, comparison, and
    equality must all reflect the assignment (guards against a backend
    swapping data out from under a stale companion structure)."""
    v = factory()
    v[0] = SETITEM_VALUE[case_id]
    expected = [SETITEM_VALUE[case_id]] + values[1:]
    assert list(v) == expected
    # Null doctrine: null positions compare to None, not False.
    assert list(v == v.copy()) == [True if x is not None else None for x in expected]


def test_categorical_setitem_rejects_unknown_value():
    v = Vector(["b", "a", "b"]).categorize(["a", "b"])
    with pytest.raises(SerifValueError):
        v[0] = "zebra"


def test_categorical_setitem_none_marks_nullable():
    v = Vector(["b", "a", "b"]).categorize(["a", "b"])
    v[0] = None
    assert list(v) == [None, "a", "b"]
    assert v.schema().nullable is True


# ---------------------------------------------------------------------------
# Unary operations
# ---------------------------------------------------------------------------

@_params(NUMERIC, NUMERIC_IDS)
def test_unary_neg(factory, values):
    assert list(-factory()) == [None if x is None else -x for x in values]


@_params(NUMERIC, NUMERIC_IDS)
def test_unary_abs(factory, values):
    assert list(abs(factory())) == [None if x is None else abs(x) for x in values]


def test_unary_neg_on_strings_raises_typeerror():
    # Python semantics: -'a' is a TypeError. It must not surface as an
    # AttributeError from a backend internals mismatch.
    with pytest.raises(TypeError):
        -Vector(["a", "b"])


def test_invert_on_bool():
    assert list(~Vector([True, False, True])) == [False, True, False]
