"""
The null doctrine (docs/null-semantics.md):

  Element-wise: unknown in, unknown out. Kleene logic for & and |.
  Aggregate: skip nulls; empty remainder yields the identity element
  (sum 0, count 0, all True, any False) or None (max, min, mean, stdev);
  the verdict reductions all()/any() warn SerifEmptyReductionWarning
  unless on_empty= is passed.

Plus the dtype dispatch for &/|/^: Kleene logical on bool vectors,
Python bitwise on int vectors.
"""

import warnings
from datetime import date

import pytest

from serif import Vector
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


def test_null_equals_null_is_null():
    # SQL corollary: identity of position is not equality of value.
    a = Vector([None, 1])
    b = Vector([None, 1])
    assert list(a == b) == [None, True]


def test_not_equal_propagates_null():
    assert list(Vector([1, None]) != 1) == [False, None]


def test_compare_to_none_scalar_is_missingness_test_and_warns():
    # Doctrine rule three: the scalar None names absence itself, so
    # `v == None` is `v.is_na()` — Python's answer (None == None is True).
    v = Vector([1, None, 3])
    with pytest.warns(UserWarning, match='is_na'):
        result = v == None  # noqa: E711 — the point of the test
    assert list(result) == [False, True, False]
    with pytest.warns(UserWarning, match='is_na'):
        inverted = v != None  # noqa: E711
    assert list(inverted) == [True, False, True]


def test_none_comparison_mask_is_total():
    # Explicit absence collapses the unknowns: the result is plain bool,
    # never bool? — you addressed the missing values, so none survive.
    v = Vector([1, None, 3])
    with pytest.warns(UserWarning):
        result = v == None  # noqa: E711
    schema = result.schema()
    assert schema.kind is bool
    assert schema.nullable is False


def test_plucked_null_scalar_crosses_the_line():
    # The named sharp edge (docs/null-semantics.md): v == w propagates
    # null (two data unknowns), but plucking that null out makes it the
    # absence symbol — v == w[1] is a missingness test.
    v = Vector([1, None, 3])
    w = Vector([1, None, 4])
    assert list(v == w) == [True, None, False]
    assert w[1] is None
    with pytest.warns(UserWarning, match='is_na'):
        assert list(v == w[1]) == [False, True, False]


def test_categorical_compare_to_none_is_missingness_test():
    c = Vector(['b', None]).categorize(['a', 'b'])
    with pytest.warns(UserWarning, match='is_na'):
        assert list(c == None) == [False, True]  # noqa: E711


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


def test_all_skips_nulls():
    assert Vector([True, None]).all() is True


def test_any_skips_nulls():
    assert Vector([False, None]).any() is False


# ---------------------------------------------------------------------------
# Verdict reductions: all()/any() return the identity and warn
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("method, identity", [('all', True), ('any', False)])
@pytest.mark.parametrize("data", [[], [None], [None, None]])
def test_verdict_over_zero_valid_values_warns_identity(method, data, identity):
    # Python semantics: all([]) is True, any([]) is False — plus a warning,
    # because a verdict from no evidence might not be what you meant.
    with pytest.warns(SerifEmptyReductionWarning):
        assert getattr(Vector(data), method)() is identity


@pytest.mark.parametrize("method", ['all', 'any'])
@pytest.mark.parametrize("data", [[], [None, None]])
@pytest.mark.parametrize("verdict", [True, False])
def test_on_empty_value_is_the_verdict_and_silences(method, data, verdict):
    with warnings.catch_warnings():
        warnings.simplefilter("error", SerifEmptyReductionWarning)
        assert getattr(Vector(data), method)(on_empty=verdict) is verdict


def test_explicit_on_empty_none_still_warns():
    # None is "no verdict chosen", stated or omitted — there is no
    # null-verdict option (in an `if`, None is indistinguishable from False).
    with pytest.warns(SerifEmptyReductionWarning):
        assert Vector([None]).all(on_empty=None) is True


def test_on_empty_ignored_when_evidence_exists():
    assert Vector([True, None]).all(on_empty=False) is True
    assert Vector([False, None]).any(on_empty=True) is False


def test_verdict_with_any_valid_value_never_warns():
    # A single valid value settles it — no warning even among nulls.
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
        Vector([None, None]).all()
    with pytest.warns(SerifEmptyReductionWarning, match='empty vector'):
        Vector([]).any()


def test_warning_escalates_to_error_via_filter():
    # The old raise is one filter away for anyone who wants it back.
    with warnings.catch_warnings():
        warnings.simplefilter("error", SerifEmptyReductionWarning)
        with pytest.raises(SerifEmptyReductionWarning):
            Vector([None, None]).any()
