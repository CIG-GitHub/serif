"""
Conformance tests for the OPTIONAL numpy-accelerated reductions.

Two tiers of guarantee (agreed doctrine):

  * BIT-IDENTICAL to the pure path: integer sum/mean (exact bigint sum,
    division in Python on both paths), min/max of any kind, every empty
    and all-null case, and every returned TYPE (python in → python out —
    np.float64 is a float subclass, so types are asserted with `is`).

  * FSUM-ANCHORED for float sum/mean/stdev: the two paths may differ in
    the last ULPs, so both are anchored against math.fsum (the exactly
    rounded sum) instead of against each other. Note the direction: the
    PURE sum path uses math.fsum on every supported Python version and is
    often exactly rounded — the fast path trades those last ULPs
    (numpy pairwise, O(log n)·ULP) for speed.

Never silently wrong: integer sums recover the exact bigint total from
numpy's wrapping (modular) sum whenever the values' spread is narrower
than 2**64/n — totals far beyond int64 stay exact AND accelerated — and
DECLINE to the pure path in the ambiguous band past that. NaN-poisoned
min/max decline (Python's min/max are order-dependent under nan — the
pure path is the spec, so the accelerator steps aside).

Skipped entirely when numpy isn't installed.
"""

import math
from datetime import date

import pytest

np = pytest.importorskip("numpy")

from serif import Vector
import serif._accel as accel


def _pure(fn):
    saved = accel._USE_NUMPY
    accel._USE_NUMPY = False
    try:
        return fn()
    finally:
        accel._USE_NUMPY = saved


VECTORS = [
    ("int_dense",  lambda: Vector([3, -1, 4, 1, 5, -9, 2, 6])),
    ("int_null",   lambda: Vector([3, None, 4, None, 5])),
    ("int_single", lambda: Vector([7])),
    ("float_dense", lambda: Vector([1.5, -2.25, 3.75, 0.5])),
    ("float_null",  lambda: Vector([1.5, None, -0.5, None])),
    ("float_inf",   lambda: Vector([1.0, float('inf'), 2.0])),
    ("bool_col",    lambda: Vector([True, False, True])),        # declines → pure
]


@pytest.mark.parametrize("vf", [v[1] for v in VECTORS], ids=[v[0] for v in VECTORS])
@pytest.mark.parametrize("op", ["sum", "min", "max", "mean"])
def test_conformance_exact(vf, op):
    # min/max are exact for every kind; sum/mean are exact for ints (and
    # for these float fixtures, whose values and sums are all exactly
    # representable — chosen so even the ULP-exempt ops must agree).
    fast = getattr(vf(), op)()
    pure = _pure(lambda: getattr(vf(), op)())
    if pure is None:
        assert fast is None
    else:
        assert fast == pure
        assert type(fast) is type(pure)


def test_date_minmax_declines_to_pure():
    # Dates decline (TupleStorage): min/max still work through the pure
    # path; sum/mean raise there for dates, which is the pure behavior too.
    v = Vector([date(2026, 1, 2), date(2026, 1, 1)])
    assert v.max() == date(2026, 1, 2) == _pure(lambda: v.max())
    assert v.min() == date(2026, 1, 1) == _pure(lambda: v.min())


@pytest.mark.parametrize("population", [False, True])
def test_stdev_conformance(population):
    v = Vector([2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0])  # classic: stdev=2.138…
    fast = v.stdev(population)
    pure = _pure(lambda: v.stdev(population))
    assert math.isclose(fast, pure, rel_tol=1e-12)
    assert type(fast) is float
    # ddof sanity: population divisor n beats sample divisor n-1
    assert v.stdev(population=True) < v.stdev(population=False)


# ---------------------------------------------------------------------------
# Empty / all-null semantics are pinned exactly
# ---------------------------------------------------------------------------

def test_empty_and_all_null_cases():
    all_null = Vector([None, None, 3]).copy()
    all_null = Vector([3, None])[Vector([False, True])]   # typed, one null
    for v in (all_null,):
        assert v.sum() == 0 and type(v.sum()) is int      # sum of no values = int 0
        assert v.max() is None and v.min() is None
        assert v.mean() is None
        assert v.stdev() is None
    empty = Vector([1.5, 2.5])[Vector([False, False])]    # typed float, length 0
    assert empty.sum() == 0 and type(empty.sum()) is int
    assert empty.max() is None and empty.mean() is None


# ---------------------------------------------------------------------------
# Never silently wrong
# ---------------------------------------------------------------------------

def test_int_sum_past_int64_stays_exact_and_accelerated():
    # 3 · 2**62 wraps in a naive np.sum reading — but the spread is ZERO,
    # so the residue pins the exact bigint total and the fast path now
    # ANSWERS this instead of declining (it used to fall to pure).
    from serif._accel import reduce as reduce_mod, DECLINED
    v = Vector([2**62, 2**62, 2**62])
    expected = 3 * 2**62                       # > int64 max
    assert reduce_mod.sum_(v._storage) is not DECLINED   # engaged, not pure
    assert v.sum() == expected == _pure(lambda: v.sum())
    assert type(v.sum()) is int
    assert math.isclose(v.mean(), expected / 3)


def test_full_range_spread_declines():
    # Spread 2**63 + 5 over two lanes: n·span ≥ 2**64, the residue is
    # ambiguous, and pure is the spec.
    from serif._accel import reduce as reduce_mod, DECLINED
    v = Vector([-2**63, 5])
    assert reduce_mod.sum_(v._storage) is DECLINED
    assert v.sum() == -2**63 + 5 == _pure(lambda: v.sum())


# ---------------------------------------------------------------------------
# The residue engine: exact bigint totals from a wrapping sum
# ---------------------------------------------------------------------------

def _engaged_sum(v):
    """The fast path's answer, asserting it actually engaged."""
    from serif._accel import reduce as reduce_mod, DECLINED
    result = reduce_mod.sum_(v._storage)
    assert result is not DECLINED
    return result


def test_timestamp_shape_sums_exactly():
    # The motivating shape: nanosecond epochs. Every value ~2**60.5, so
    # the OLD n·max|v| guard declined any column longer than ~5 rows and
    # serif fell to pure summation. The spread is a day — microscopic
    # next to 2**64/n — so the residue engine answers exactly.
    import random
    rng = random.Random(7)
    base = 1_700_000_000_000_000_000
    vals = [base + rng.randrange(86_400_000_000_000) for _ in range(5_000)]
    v = Vector(vals)
    expected = sum(vals)                       # ≈ 8.5e21, way past int64
    assert _engaged_sum(v) == expected
    assert v.sum() == expected == _pure(lambda: v.sum())
    assert v.mean() == _pure(lambda: v.mean())   # bigint ÷ n: bit-identical
    # Nulls ride the usual compress; the residue math never sees them.
    v2 = Vector(vals[:100] + [None] * 5 + vals[100:200])
    assert v2.sum() == sum(vals[:200]) == _pure(lambda: v2.sum())


def test_outlier_spread_still_declines():
    # One huge value stretches the spread window itself past 2**64/n —
    # the residue stays ambiguous, and pure is the spec. (This shape
    # needs an ESTIMATE-based window — e.g. a float64 sum with a proven
    # error bound — a separate game with separate epistemics.)
    from serif._accel import reduce as reduce_mod, DECLINED
    vals = [2**62] + list(range(-500, 500))
    v = Vector(vals)
    assert reduce_mod.sum_(v._storage) is DECLINED
    assert v.sum() == sum(vals) == _pure(lambda: v.sum())


def test_negative_clustered_sums_exactly():
    vals = [-2**62 - k for k in range(1_000)]
    v = Vector(vals)
    assert _engaged_sum(v) == sum(vals)        # ≈ -4.6e21, exact
    assert v.sum() == sum(vals) == _pure(lambda: v.sum())


def test_boundary_total_past_int64_zero_spread():
    v = Vector([2**63 - 1, 2**63 - 1])
    assert _engaged_sum(v) == 2**64 - 2
    assert v.sum() == 2**64 - 2 == _pure(lambda: v.sum())


def test_randomized_wide_spread_cross_check():
    # Spread ~2**41 over 10k lanes: n·span ≈ 2**54.3, comfortably inside
    # the window; intermediates wrap freely and must not matter.
    import random
    rng = random.Random(23)
    for base in (0, 2**62, -2**62):
        vals = [base + rng.randrange(-2**40, 2**40) for _ in range(10_000)]
        v = Vector(vals)
        assert _engaged_sum(v) == sum(vals)
        assert v.sum() == sum(vals) == _pure(lambda: v.sum())


def test_stdev_now_engages_on_big_values():
    # Previously the big-int decline dragged stdev to pure with it; the
    # residue total lets the float pipeline run. Same float64 lane math
    # both paths (fsum-anchored tier), so they agree tightly.
    import random
    rng = random.Random(3)
    vals = [2**60 + rng.randrange(10**6) for _ in range(2_000)]
    v = Vector(vals)
    fast = v.stdev()
    pure = _pure(lambda: v.stdev())
    assert math.isclose(fast, pure, rel_tol=1e-12)
    assert type(fast) is float


def test_nan_minmax_matches_pure_exactly():
    # np.max would say nan; Python's max is order-dependent under nan.
    # The accelerator declines, so both calls run the same pure code.
    for vals in ([1.0, float('nan'), 3.0], [float('nan'), 1.0]):
        v = Vector(vals)
        fast, pure = v.max(), _pure(lambda: v.max())
        assert (fast == pure) or (math.isnan(fast) and math.isnan(pure))


def test_nan_sum_propagates_on_both_paths():
    v = Vector([1.0, float('nan'), 3.0])
    assert math.isnan(v.sum()) and math.isnan(_pure(lambda: v.sum()))


# ---------------------------------------------------------------------------
# Float doctrine: both paths anchored against the exactly rounded sum
# ---------------------------------------------------------------------------

def test_float_sum_fsum_anchored():
    import random
    rng = random.Random(11)
    vals = [rng.uniform(-1e6, 1e6) for _ in range(10_000)]
    v = Vector(vals)
    truth = math.fsum(vals)
    scale = math.fsum(abs(x) for x in vals)
    bound = len(vals) * 2**-52 * scale         # left-fold worst case; pairwise is tighter
    assert abs(v.sum() - truth) <= bound
    assert abs(_pure(lambda: v.sum()) - truth) <= bound
    assert type(v.sum()) is float


def test_absorption_case_both_paths_bounded():
    # The absorption pathology: a naive left-fold loses every 1.0 into
    # 1e16 (error -1000). Neither path is naive: the pure math.fsum is
    # exactly rounded here, while numpy's pairwise sum places
    # the small terms among themselves first (error ~ULPs). This pins the
    # documented trade — pure may be MORE accurate; fast stays within a
    # pairwise-scale bound of the exactly rounded answer.
    vals = [1e16] + [1.0] * 1000
    v = Vector(vals)
    truth = math.fsum(vals)
    assert _pure(lambda: v.sum()) == truth          # compensated: exact here
    assert abs(v.sum() - truth) <= 64 * math.ulp(truth)
    naive_error = 1000.0                            # what a left-fold would lose
    assert abs(v.sum() - truth) < naive_error / 10


# ---------------------------------------------------------------------------
# The fast path actually engages (guards against silent decline rot)
# ---------------------------------------------------------------------------

def test_fast_path_engages_and_declines_where_designed(monkeypatch):
    from serif._accel import reduce as reduce_mod

    engaged = []
    orig = reduce_mod.sum_

    def spy(storage):
        result = orig(storage)
        engaged.append(result is not accel.DECLINED)
        return result

    monkeypatch.setattr(reduce_mod, 'sum_', spy)
    Vector([1, 2, 3]).sum()                    # int: engages
    Vector([1.5, None]).sum()                  # nullable float: engages
    Vector([2**62] * 3).sum()                  # past int64, ZERO spread:
    #                                            the residue engine answers
    Vector([-2**63, 5]).sum()                  # full-range spread: declines
    Vector([True, False]).sum()                # BoolStorage: declines
    assert engaged == [True, True, True, False, False]
