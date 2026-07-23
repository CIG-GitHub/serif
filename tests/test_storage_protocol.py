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

from array import array
from datetime import date
from decimal import Decimal

import pytest

from serif import Vector
from serif.errors import SerifValueError
from serif._vector.storage import ArrayStorage
from serif._vector.storage import BoolStorage
from serif._vector.storage import DecimalStorage
from serif._vector.storage import StringStorage
from serif._vector.storage import TupleStorage
from serif._vector.storage import concatenate_storages
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


@pytest.mark.parametrize(
    "values,typecode,raw_values,mask_bytes",
    [
        ([1, None, -3, None, 5], 'q', [1, 0, -3, 0, 5], b'\x15'),
        ([1.5, None, -2.25], 'd', [1.5, 0.0, -2.25], b'\x05'),
    ],
    ids=['int', 'float'],
)
def test_array_storage_builds_generator_directly(
        values, typecode, raw_values, mask_bytes):
    storage = ArrayStorage.from_iterable(
        (value for value in values),
        typecode,
        nullable=True,
    )

    assert list(storage) == values
    assert storage._data.tolist() == raw_values
    assert bytes(storage._mask._buf) == mask_bytes


def test_array_storage_all_valid_generator_omits_mask():
    storage = ArrayStorage.from_iterable(
        (value for value in [1, 2, 3]),
        'q',
        nullable=False,
    )

    assert storage._data.tolist() == [1, 2, 3]
    assert storage._mask is None


def test_array_storage_preserves_native_value_errors_and_vector_fallback():
    with pytest.raises(OverflowError):
        ArrayStorage.from_iterable([2**63], 'q', nullable=False)
    with pytest.raises(TypeError):
        ArrayStorage.from_iterable([object()], 'q', nullable=False)

    vector = Vector([2**63])
    assert type(vector._storage) is TupleStorage
    assert list(vector) == [2**63]


def test_decimal_storage_builds_generator_directly():
    values = [Decimal('1.25'), None, Decimal('-2.50')]
    storage = DecimalStorage.from_iterable(
        (value for value in values),
        scale=2,
        precision=4,
        nullable=True,
    )

    assert list(storage) == values
    assert len(storage._buf) == 3 * 16
    assert storage._buf[16:32] == b'\x00' * 16
    assert bytes(storage._mask._buf) == b'\x05'


def test_decimal_storage_preserves_half_even_rounding_and_dense_mask():
    storage = DecimalStorage.from_iterable(
        (value for value in [Decimal('1.245'), Decimal('1.255')]),
        scale=2,
        precision=4,
        nullable=False,
    )

    assert list(storage) == [Decimal('1.24'), Decimal('1.26')]
    assert storage._mask is None


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
def test_is_na(factory, values):
    assert list(factory().is_na()) == [x is None for x in values]


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


# ---------------------------------------------------------------------------
# Copy-on-write tripwires
#
# copy() SHARES the frozen storage object (O(1) snapshot); independence is
# guaranteed by the rebuild-on-write doctrine (storage.py) — every public
# mutation path rebinds a NEW storage — not by physical duplication. These
# tests trip the moment either half of that contract regresses: the identity
# pin catches copy() sliding back to a walk, the isolation tests catch any
# future in-place buffer mutation corrupting an alias.
# ---------------------------------------------------------------------------

# Categoricals excluded from the identity pin only: their copy() shallow-
# copies the _CategoryStorage wrapper (codes/categories still shared).
NON_CAT = [c for c in CASES if not c[0].startswith("cat")]
NON_CAT_IDS = [c[0] for c in NON_CAT]


@_params(NON_CAT, NON_CAT_IDS)
def test_copy_shares_storage(factory, values):
    v = factory()
    assert v.copy()._storage is v._storage


@pytest.mark.parametrize("case_id,factory,values", CASES, ids=IDS)
def test_mutating_source_leaves_copy_untouched(case_id, factory, values):
    # Mirror of test_copy_is_independent: writes must not cross in EITHER
    # direction through the shared storage.
    v = factory()
    c = v.copy()
    v[0] = SETITEM_VALUE[case_id]
    assert list(c) == values


@pytest.mark.parametrize("case_id,factory,values", CASES, ids=IDS)
def test_mask_setitem_on_copy_leaves_source_untouched(case_id, factory, values):
    v = factory()
    c = v.copy()
    c[[True] + [False] * (len(values) - 1)] = SETITEM_VALUE[case_id]
    assert list(v) == values


def test_promotion_on_copy_leaves_source_dtype():
    # Kind promotion rebinds storage AND dtype on the mutated side only.
    v = Vector([1, 2, 3])
    c = v.copy()
    c[0] = 1.5
    assert c.schema().kind is float
    assert v.schema().kind is int
    assert list(v) == [1, 2, 3]


def test_table_snapshot_isolated_from_source_writes():
    from serif import Table
    v = Vector([1, 2, 3], name="a")
    t = Table([v])
    v[0] = 99
    assert list(t.a) == [1, 2, 3]


def test_source_isolated_from_table_column_writes():
    from serif import Table
    v = Vector([1, 2, 3], name="a")
    t = Table([v])
    t[0, 'a'] = 99
    assert list(v) == [1, 2, 3]
    assert list(t.a) == [99, 2, 3]


def test_dict_construction_snapshots_vectors():
    # The dict path snapshots an existing vector via copy() (storage
    # share), not a re-inferring rebuild: schema is preserved and writes
    # stay isolated in both directions, same as the list path.
    from serif import Table
    v = Vector([1, None, 3], name="orig")
    t = Table({"a": v})
    assert t.a.schema().kind is int
    assert t.a.schema().nullable is True
    assert t.column_names() == ["a"]        # dict key wins over 'orig'
    assert v.vector_name == "orig"          # source name untouched
    v[0] = 99
    assert list(t.a) == [1, None, 3]
    t[2, 'a'] = 7
    assert list(v) == [99, None, 3]


def test_dict_construction_preserves_declared_nullable():
    # A column declared nullable stays nullable even with no nulls present
    # — the dict path must not silently re-infer the schema (the list path
    # never did).
    from serif import Table
    from serif._vector.dtype import Schema
    v = Vector([1, 2, 3], dtype=Schema(int, True))
    assert Table({"a": v}).a.schema().nullable is True


def test_dict_construction_keeps_categorical():
    from serif import Table
    from serif._vector.categorical import _Category
    cat = Vector(["b", "a", "b"]).categorize(["a", "b"])
    t = Table({"c": cat})
    assert isinstance(t.c, _Category)
    assert list(t.c) == ["b", "a", "b"]


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


def _mask_signature(mask):
    if mask is None:
        return None
    return bytes(mask._buf), len(mask)


def _storage_signature(storage):
    if isinstance(storage, ArrayStorage):
        return (
            ArrayStorage,
            storage._data.typecode,
            storage._data.tobytes(),
            _mask_signature(storage._mask),
        )
    if isinstance(storage, BoolStorage):
        return BoolStorage, bytes(storage._data), _mask_signature(storage._mask)
    if isinstance(storage, StringStorage):
        return (
            StringStorage,
            storage._buf,
            tuple(storage._offsets),
            _mask_signature(storage._mask),
        )
    if isinstance(storage, DecimalStorage):
        return (
            DecimalStorage,
            bytes(storage._buf),
            storage._scale,
            storage._precision,
            _mask_signature(storage._mask),
        )
    return TupleStorage, storage._data


PHYSICAL_CONCAT_CASES = [
    (
        "array",
        lambda: (
            ArrayStorage.from_iterable([1, None], 'q', nullable=True),
            ArrayStorage.from_iterable([3, 4], 'q', nullable=False),
        ),
        lambda: ArrayStorage.from_iterable(
            [1, None, 3, 4], 'q', nullable=True),
    ),
    (
        "bool",
        lambda: (
            BoolStorage.from_iterable([True, None]),
            BoolStorage.from_iterable([False, True]),
        ),
        lambda: BoolStorage.from_iterable([True, None, False, True]),
    ),
    (
        "string",
        lambda: (
            StringStorage.from_iterable(["a", None]),
            StringStorage.from_iterable(["", "🐍"]),
            StringStorage.from_iterable(["tail"]),
        ),
        lambda: StringStorage.from_iterable(
            ["a", None, "", "🐍", "tail"]),
    ),
    (
        "decimal",
        lambda: (
            DecimalStorage.from_iterable(
                [Decimal('1.25'), None], 2, 4, nullable=True),
            DecimalStorage.from_iterable(
                [Decimal('-2.50')], 2, 4, nullable=False),
        ),
        lambda: DecimalStorage.from_iterable(
            [Decimal('1.25'), None, Decimal('-2.50')],
            2,
            4,
            nullable=True,
        ),
    ),
    (
        "tuple",
        lambda: (
            TupleStorage.from_iterable([date(2024, 1, 1), None]),
            TupleStorage.from_iterable([date(2024, 1, 3)]),
        ),
        lambda: TupleStorage.from_iterable(
            [date(2024, 1, 1), None, date(2024, 1, 3)]),
    ),
]


@pytest.mark.parametrize(
    "parts_factory,expected_factory",
    [case[1:] for case in PHYSICAL_CONCAT_CASES],
    ids=[case[0] for case in PHYSICAL_CONCAT_CASES],
)
def test_physical_storage_concatenation(parts_factory, expected_factory):
    parts = parts_factory()
    before = [_storage_signature(part) for part in parts]

    result = concatenate_storages(parts)

    assert _storage_signature(result) == _storage_signature(expected_factory())
    assert [_storage_signature(part) for part in parts] == before
    assert all(result is not part for part in parts)


def test_physical_storage_concatenation_keeps_dense_mask_absent():
    result = concatenate_storages((
        ArrayStorage(array('q', [1, 2])),
        ArrayStorage(array('q', [3, 4])),
    ))
    assert result._mask is None


def test_physical_storage_concatenation_rejects_invalid_sequences():
    with pytest.raises(ValueError, match='empty'):
        concatenate_storages(())
    with pytest.raises(TypeError, match='physical type'):
        concatenate_storages((
            ArrayStorage(array('q', [1])),
            TupleStorage((2,)),
        ))


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
