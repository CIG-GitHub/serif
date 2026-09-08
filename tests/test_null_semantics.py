"""
The null doctrine (docs/null-semantics.md):

  Element-wise: unknown in, unknown out. Kleene logic for & and |.
  Numeric aggregates skip nulls; empty remainder yields an identity or None.
  all()/any() fold with Kleene logic; only actually empty input returns the
  identity with SerifEmptyReductionWarning unless on_empty= is passed.

Plus the dtype dispatch for &/|/^: Kleene logical on bool vectors,
Python bitwise on int vectors.
"""

import operator
import warnings
from datetime import date

import pytest

from serif import Vector
from serif import Table
from serif import Schema
from serif import SerifEmptyReductionWarning
from serif import SerifTypeError


# ---------------------------------------------------------------------------
# Comparisons: unknown in, unknown out
# ---------------------------------------------------------------------------

def test_comparison_propagates_null():
    assert list(Vector([1, None, 8]) > 6) == [False, None, True]


def test_comparison_result_is_nullable_bool():
    s = (Vector([1, None, 8]) > 6).schema()
    assert s.kind is bool
    assert s.nullable is True


def test_dense_comparison_stays_non_nullable():
    s = (Vector([1, 2]) > 1).schema()
    assert s.nullable is False


def test_equality_between_nullable_vectors():
    a = Vector([1, None, 3])
    b = Vector([1, 2, None])
    assert list(a == b) == [True, None, None]
    assert list(a != b) == [False, None, None]


def test_null_equals_null_is_null():
    # SQL corollary: identity of position is not equality of value.
    a = Vector([None, 1])
    b = Vector([None, 1])
    assert list(a == b) == [None, True]
    assert list(a != b) == [None, False]


def test_not_equal_propagates_null():
    assert list(Vector([1, None]) != 1) == [False, None]


@pytest.mark.parametrize("comparison, alternative", [
    (operator.eq, 'v.is_na()'),
    (operator.ne, '~v.is_na()'),
])
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("data, dtype", [
    ([1, None, 3], Schema(int, True)),
    ([1, 3], int),
    ([None, None], Schema(int, True)),
    ([], int),
    ([], None),
    ([None], None),
    (['a', None], Schema(str, True)),
    ([True, None], Schema(bool, True)),
    ([date(2024, 1, 2), None], Schema(date, True)),
    ([1, 'a', None], Schema(object, True)),
])
def test_compare_to_none_scalar_raises(
    comparison, alternative, reverse, data, dtype,
):
    v = Vector(data, dtype=dtype)
    with pytest.raises(SerifTypeError, match='scalar None') as error:
        comparison(None, v) if reverse else comparison(v, None)
    assert alternative in str(error.value)


def test_is_na_mask_is_total():
    v = Vector([1, None, 3])
    result = v.is_na()
    assert list(result) == [False, True, False]
    assert list(~result) == [True, False, True]
    schema = result.schema()
    assert schema.kind is bool
    assert schema.nullable is False


@pytest.mark.parametrize("comparison, expected", [
    (operator.eq, [True, None, False]),
    (operator.ne, [False, None, True]),
])
def test_plucked_null_scalar_raises(comparison, expected):
    # Extracting a null must not turn value equality into missingness.
    v = Vector([1, None, 3])
    w = Vector([1, None, 4])
    assert list(comparison(v, w)) == expected
    assert w[1] is None
    with pytest.raises(SerifTypeError, match='is_na'):
        comparison(v, w[1])


@pytest.mark.parametrize("comparison", [operator.eq, operator.ne])
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("data", [['b', None], ['b'], [None], []])
def test_categorical_compare_to_none_raises(comparison, reverse, data):
    c = Vector(data, dtype=Schema(str, True)).categorize(['a', 'b'])
    with pytest.raises(SerifTypeError, match='is_na'):
        comparison(None, c) if reverse else comparison(c, None)


def test_categorical_missingness_and_vector_comparison():
    c = Vector(['b', None]).categorize(['a', 'b'])
    other = Vector(['a', None]).categorize(['a', 'b'])
    assert list(c.is_na()) == [False, True]
    assert list(~c.is_na()) == [True, False]
    assert list(c == other) == [False, None]
    assert list(c != other) == [True, None]


@pytest.mark.parametrize("comparison", [operator.eq, operator.ne])
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("data", [{'x': [1, None]}, {'x': []}, {}])
def test_table_compare_to_none_raises(comparison, reverse, data):
    t = Table(data)
    with pytest.raises(SerifTypeError, match='is_na'):
        comparison(None, t) if reverse else comparison(t, None)


def test_comparison_count_counts_known():
    # Doctrine synergy: count() skips the unknowns the comparison reported.
    assert (Vector([1, None, 8]) > 6).count() == 2


def test_date_comparison_propagates_null():
    v = Vector([date(2024, 1, 2), None])
    assert list(v > date(2024, 1, 1)) == [True, None]


def test_categorical_comparison_propagates_null():
    c = Vector(['b', None]).categorize(['a', 'b'])
    assert list(c == 'b') == [True, None]


def test_categorical_unknown_value_equality_propagates_null():
    c = Vector(['b', None]).categorize(['a', 'b'])
    assert list(c == 'zebra') == [False, None]
    assert list(c != 'zebra') == [True, None]


# ---------------------------------------------------------------------------
# Kleene logic for & | ^ on bool vectors
# ---------------------------------------------------------------------------

A = [True, True, True, False, False, False, None, None, None]
B = [True, False, None, True, False, None, True, False, None]


def test_kleene_and_full_table():
    assert list(Vector(A) & Vector(B)) == [
        True, False, None,
        False, False, False,
        None, False, None,
    ]


def test_kleene_or_full_table():
    assert list(Vector(A) | Vector(B)) == [
        True, True, True,
        True, False, None,
        True, None, None,
    ]


def test_xor_propagates_null():
    assert list(Vector(A) ^ Vector(B)) == [
        False, True, None,
        True, False, None,
        None, None, None,
    ]


def test_kleene_and_scalar_false_settles():
    assert list(Vector([True, None]) & False) == [False, False]


def test_kleene_or_scalar_true_settles():
    assert list(Vector([False, None]) | True) == [True, True]


def test_invert_propagates_null():
    result = ~Vector([True, None, False])
    assert list(result) == [False, None, True]
    assert result.schema().nullable is True


def test_invert_dense_bool_unchanged():
    assert list(~Vector([True, False])) == [False, True]


# ---------------------------------------------------------------------------
# & | ^ on int vectors are Python bitwise
# ---------------------------------------------------------------------------

def test_int_and_is_bitwise():
    result = Vector([3, 6]) & 1
    assert list(result) == [1, 0]
    assert result.schema().kind is int


def test_int_or_is_bitwise():
    assert list(Vector([1]) | 2) == [3]


def test_int_xor_is_bitwise():
    assert list(Vector([3]) ^ 1) == [2]


def test_int_bitwise_scalar_left():
    assert list(1 & Vector([3, 6])) == [1, 0]


def test_int_bitwise_propagates_null():
    assert list(Vector([3, None]) & 1) == [1, None]


# ---------------------------------------------------------------------------
# Filtering: null mask entries exclude (SQL WHERE)
# ---------------------------------------------------------------------------

def test_filter_excludes_null_rows():
    v = Vector([1, None, 8])
    assert list(v[v > 6]) == [8]


def test_complement_filter_also_excludes_null_rows():
    # The honesty property: neither half claims the unknown rows.
    v = Vector([1, None, 8])
    assert list(v[~(v > 6)]) == [1]


def test_is_na_claims_the_unknowns():
    v = Vector([1, None, 8])
    assert list(v[v.is_na()]) == [None]


def test_table_filter_with_nullable_mask():
    from serif import Table
    t = Table({'x': [1, None, 8], 'y': ['a', 'b', 'c']})
    result = t[t.x > 6]
    assert list(result.y) == ['c']


def test_masked_assignment_skips_null_entries():
    v = Vector([1, None, 8])
    v[v > 6] = 99
    assert list(v) == [1, None, 99]


# ---------------------------------------------------------------------------
# Aggregates: skip nulls; empty remainder → identity or None
# ---------------------------------------------------------------------------

def test_aggregates_skip_nulls():
    v = Vector([1, None, 3])
    assert v.sum() == 4
    assert v.count() == 2
    assert v.mean() == 2
    assert v.max() == 3
    assert v.min() == 1


@pytest.mark.parametrize("agg,expected", [
    ('sum', 0), ('count', 0),
    ('max', None), ('min', None), ('mean', None), ('stdev', None),
])
def test_all_null_aggregate_identity_rule(agg, expected):
    result = getattr(Vector([None, None]), agg)()
    if expected is None:
        assert result is None
    else:
        assert result == expected


def test_float_sum_identity_preserves_dtype():
    empty = Vector([1.0])[:0]
    all_null = Vector([1.0, None])[1:]

    assert empty.sum() == 0.0
    assert type(empty.sum()) is float
    assert all_null.sum() == 0.0
    assert type(all_null.sum()) is float


@pytest.mark.parametrize("data, all_result, any_result", [
    ([True, True], True, True),
    ([True, False], False, True),
    ([True, None], None, True),
    ([False, True], False, True),
    ([False, False], False, False),
    ([False, None], False, None),
    ([None, True], None, True),
    ([None, False], False, None),
    ([None, None], None, None),
    ([None], None, None),
    ([None, True, False], False, True),
])
def test_verdicts_fold_kleene_logic(data, all_result, any_result):
    with warnings.catch_warnings():
        warnings.simplefilter('error', SerifEmptyReductionWarning)
        v = Vector(data)
        assert v.all() is all_result
        assert v.any() is any_result


@pytest.mark.parametrize("data, all_result, any_result", [
    ([1, 2], True, True),
    ([0, 0], False, False),
    ([1, None], None, True),
    ([None, 0], False, None),
    (['', None], False, None),
    ([None, 'yes'], None, True),
])
def test_verdicts_preserve_known_value_truthiness(data, all_result, any_result):
    v = Vector(data)
    assert v.all() is all_result
    assert v.any() is any_result


def test_dropna_explicitly_reduces_only_known_values():
    assert Vector([True, None]).dropna().all() is True
    assert Vector([False, None]).dropna().any() is False


@pytest.mark.parametrize("data, verdict, enters", [
    ([6, 7], True, True),
    ([6, 4], False, False),
    ([6, None], None, False),
    ([None, None], None, False),
])
def test_comparison_all_requires_positive_truth_in_if(data, verdict, enters):
    result = (Vector(data) > 5).all()
    assert result is verdict
    entered = False
    if result:
        entered = True
    assert entered is enters


# ---------------------------------------------------------------------------
# Empty verdict reductions return the identity and warn
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("method, identity", [('all', True), ('any', False)])
def test_verdict_over_empty_input_warns_identity(method, identity):
    # Python semantics: all([]) is True, any([]) is False — plus a warning,
    # because a verdict from no evidence might not be what you meant.
    with pytest.warns(SerifEmptyReductionWarning):
        assert getattr(Vector([]), method)() is identity


@pytest.mark.parametrize("method", ['all', 'any'])
@pytest.mark.parametrize("verdict", [True, False])
def test_on_empty_value_is_the_verdict_and_silences(method, verdict):
    with warnings.catch_warnings():
        warnings.simplefilter("error", SerifEmptyReductionWarning)
        assert getattr(Vector([]), method)(on_empty=verdict) is verdict


def test_explicit_on_empty_none_still_warns():
    # Explicit None retains the default empty-input policy.
    with pytest.warns(SerifEmptyReductionWarning):
        assert Vector([]).all(on_empty=None) is True


@pytest.mark.parametrize("on_empty", [None, True, False])
def test_on_empty_does_not_override_nonempty_results(on_empty):
    with warnings.catch_warnings():
        warnings.simplefilter('error', SerifEmptyReductionWarning)
        assert Vector([True, None]).all(on_empty=on_empty) is None
        assert Vector([False, None]).any(on_empty=on_empty) is None
        assert Vector([None]).all(on_empty=on_empty) is None
        assert Vector([None]).any(on_empty=on_empty) is None
        assert Vector([False, None]).all(on_empty=on_empty) is False
        assert Vector([True, None]).any(on_empty=on_empty) is True


def test_decisive_value_settles_among_nulls():
    with warnings.catch_warnings():
        warnings.simplefilter("error", SerifEmptyReductionWarning)
        assert Vector([None, False, None]).all() is False
        assert Vector([None, True, None]).any() is True


@pytest.mark.parametrize("bad", ['yes', 1, 0, 1.0])
def test_on_empty_rejects_non_bool(bad):
    # Identity check, not truthiness: on_empty=1 is a bug, not a True.
    with pytest.raises(SerifTypeError, match='on_empty'):
        Vector([True]).all(on_empty=bad)
    with pytest.raises(SerifTypeError, match='on_empty'):
        Vector([True]).any(on_empty=bad)


def test_no_verdict_warning_teaches():
    with pytest.warns(SerifEmptyReductionWarning, match='on_empty'):
        Vector([]).all()
    with pytest.warns(SerifEmptyReductionWarning, match='empty vector'):
        Vector([]).any()


def test_warning_escalates_to_error_via_filter():
    # The old raise is one filter away for anyone who wants it back.
    with warnings.catch_warnings():
        warnings.simplefilter("error", SerifEmptyReductionWarning)
        with pytest.raises(SerifEmptyReductionWarning):
            Vector([]).any()
