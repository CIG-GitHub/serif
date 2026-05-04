"""
Tests for typing, dtype inference, promotion and nullable behavior in Vector.
"""

from datetime import date, datetime

import pytest

from serif import Vector
from serif.typing import DataType, infer_dtype


class TestInferDtype:
    """Inference from raw Python values -> DataType."""

    @pytest.mark.parametrize(
        "values, expected_kind, expected_nullable",
        [
            ([1, 2, 3], int, False),
            ([1.5, 2.5], float, False),
            (["a", "b", "c"], str, False),
            ([True, False], bool, False),
            ([1 + 2j, 3 + 4j], complex, False),
            ([date(2020, 1, 1), date(2020, 1, 2)], date, False),
            ([datetime(2020, 1, 1), datetime(2020, 1, 2)], datetime, False),
        ],
    )
    def test_infer_pure_kinds(self, values, expected_kind, expected_nullable):
        dt = infer_dtype(values)
        assert isinstance(dt, DataType)
        assert dt.kind is expected_kind
        assert dt.nullable is expected_nullable

    @pytest.mark.parametrize(
        "values, expected_kind",
        [
            ([1, 2.5, 3], float),
            ([1, 2.5, 3 + 4j], complex),
            ([True, 1, 2], int),        # bool + int => int
            ([True, 1.0], float),       # bool + float => float
        ],
    )
    def test_infer_mixed_numeric_promotes(self, values, expected_kind):
        dt = infer_dtype(values)
        assert dt.kind is expected_kind
        assert not dt.nullable

    def test_infer_mixed_temporal_promotes_to_datetime(self):
        values = [date(2020, 1, 1), datetime(2020, 1, 2)]
        dt = infer_dtype(values)
        assert dt.kind is datetime
        assert not dt.nullable

    def test_infer_nullable_when_none_present(self):
        dt = infer_dtype([1, None, 3])
        assert dt.kind is int
        assert dt.nullable

        dt = infer_dtype(["a", None, "c"])
        assert dt.kind is str
        assert dt.nullable

        dt = infer_dtype([date(2020, 1, 1), None])
        assert dt.kind is date
        assert dt.nullable

    def test_infer_all_none_gives_object_nullable(self):
        dt = infer_dtype([None, None, None])
        assert dt.kind is object
        assert dt.nullable

    def test_infer_mixed_incompatible_falls_back_to_object(self):
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            dt = infer_dtype([1, "a", 3])
        assert dt.kind is object
        assert dt.nullable is False  # no None in input


class TestVectorCreationWithDtype:
    """Creating vectors with explicit DataType or Python type."""

    def test_create_typed_int(self):
        v = Vector([1, 2, 3], dtype=DataType(int, nullable=False))
        s = v.schema()
        assert s.kind is int
        assert s.nullable is False
        assert list(v) == [1, 2, 3]

    def test_create_nullable_int(self):
        v = Vector([1, None, 3], dtype=DataType(int, nullable=True))
        s = v.schema()
        assert s.kind is int
        assert s.nullable is True
        assert list(v) == [1, None, 3]

    def test_create_typed_float(self):
        v = Vector([1.0, 2.0, 3.0], dtype=DataType(float, nullable=False))
        s = v.schema()
        assert s.kind is float
        assert s.nullable is False

    def test_create_typed_str(self):
        v = Vector(["a", "b", "c"], dtype=DataType(str, nullable=False))
        s = v.schema()
        assert s.kind is str
        assert s.nullable is False

    def test_python_type_shorthand(self):
        v = Vector([1, 2, 3], dtype=int)
        s = v.schema()
        assert s.kind is int
        assert s.nullable is False


class TestArithmeticPromotion:
    """Arithmetic ops should produce new vectors with promoted dtype."""

    def test_add_scalar_promotes_int_to_float(self):
        v = Vector([1, 2, 3])
        out = v + 0.5

        s = out.schema()
        assert s.kind is float
        assert s.nullable is False
        assert list(out) == [1.5, 2.5, 3.5]

    def test_mul_scalar_promotes_int_to_float(self):
        v = Vector([1, 2, 3])
        out = v * 0.5

        s = out.schema()
        assert s.kind is float
        assert s.nullable is False
        assert list(out) == [0.5, 1.0, 1.5]

    def test_div_scalar_promotes_int_to_float(self):
        v = Vector([4, 6, 8])
        out = v / 2

        s = out.schema()
        assert s.kind is float
        assert s.nullable is False
        assert list(out) == [2.0, 3.0, 4.0]

    def test_vector_vector_numeric_promotion(self):
        v_int = Vector([1, 2, 3])
        v_float = Vector([1.5, 2.5, 3.5])

        out = v_int + v_float
        s = out.schema()
        assert s.kind is float
        assert s.nullable is False
        assert list(out) == [2.5, 4.5, 6.5]

    def test_vector_vector_complex_promotion(self):
        v = Vector([1, 2.0, 3])
        w = Vector([1 + 2j, 0 + 0j, 5 + 0j])

        out = v + w
        s = out.schema()
        assert s.kind is complex
        assert s.nullable is False
        assert list(out) == [2 + 2j, 2.0 + 0j, 8 + 0j]

    def test_arith_with_none_preserves_mask(self):
        v = Vector([1, None, 3])
        out = v + 10

        s = out.schema()
        assert s.kind is int or s.kind is float  # up to implementation
        assert s.nullable is True
        assert list(out) == [11, None, 13]

    def test_comparison_with_none_produces_bool_mask(self):
        v = Vector([1, None, 3])
        mask = v > 1

        s = mask.schema()
        assert s.kind is bool
        assert s.nullable is False
        # Define tri-value comparison as: None compared to anything -> False
        assert list(mask) == [False, False, True]

    def test_mixed_incompatible_types_fall_back_to_object(self):
        v = Vector([1, 2, 3])
        w = Vector(["a", "b", "c"])

        out = v + w  # whatever behavior you choose, dtype should be object
        s = out.schema()
        assert s.kind is object


class TestDataTypePromotionInternal:
    """Direct tests for internal promotion behavior (_promote)."""

    def test_numeric_ladder_int_to_float(self):
        v = Vector([1, 2, 3])
        assert v.schema().kind is int

        v._promote(float)
        s = v.schema()
        assert s.kind is float
        assert list(v) == [1.0, 2.0, 3.0]

    def test_numeric_ladder_int_to_complex(self):
        v = Vector([1, 2, 3])
        v._promote(complex)
        s = v.schema()
        assert s.kind is complex
        assert list(v) == [1 + 0j, 2 + 0j, 3 + 0j]

    def test_numeric_ladder_float_to_complex(self):
        v = Vector([1.5, 2.5])
        v._promote(complex)
        s = v.schema()
        assert s.kind is complex
        assert list(v) == [1.5 + 0j, 2.5 + 0j]

    def test_numeric_backward_promotions_disallowed(self):
        v = Vector([1.5, 2.5])
        with pytest.raises(Exception):
            v._promote(int)

        w = Vector([1 + 2j, 3 + 4j])
        with pytest.raises(Exception):
            w._promote(float)

    def test_temporal_ladder_date_to_datetime(self):
        v = Vector([date(2020, 1, 1), date(2020, 1, 2)])
        # Promote whole vector to datetime
        v._promote(datetime)
        s = v.schema()
        assert s.kind is datetime
        assert s.nullable is False
        assert all(isinstance(x, datetime) for x in v)

    def test_promotion_preserves_nullable_flag(self):
        v = Vector([1, None, 3], dtype=DataType(int, nullable=True))
        assert v.schema().nullable is True

        v._promote(float)
        s = v.schema()
        assert s.kind is float
        assert s.nullable is True
        assert list(v) == [1.0, None, 3.0]


class TestSetitemPromotion:
    """Setting values should trigger promotion when needed."""

    def test_setitem_scalar_promotes_int_to_float(self):
        v = Vector([1, 2, 3])
        assert v.schema().kind is int

        v[1] = 2.5
        s = v.schema()
        assert s.kind is float
        assert list(v) == [1.0, 2.5, 3.0]

    def test_setitem_slice_promotes_int_to_float(self):
        v = Vector([1, 2, 3, 4])
        v[1:3] = [2.5, 3.5]

        s = v.schema()
        assert s.kind is float
        assert list(v) == [1.0, 2.5, 3.5, 4.0]

    def test_setitem_scalar_promotes_to_complex(self):
        v = Vector([1, 2, 3])
        v[0] = 1 + 2j

        s = v.schema()
        assert s.kind is complex
        assert list(v) == [1 + 2j, 2 + 0j, 3 + 0j]

    def test_setitem_boolean_mask_promotes(self):
        v = Vector([1, 2, 3, 4])
        mask = Vector([True, False, True, False])

        v[mask] = [1.5, 3.5]
        s = v.schema()
        assert s.kind is float
        assert list(v) == [1.5, 2.0, 3.5, 4.0]

    def test_setitem_invalid_promotion_raises(self):
        v = Vector([1, 2, 3])
        with pytest.raises(Exception):
            v[0] = "hello"  # int -> str should not be silently allowed


class TestNullableBehavior:
    """Masking and null-handling APIs: isna, fillna, dropna."""

    def test_isna_returns_boolean_mask(self):
        v = Vector([1, None, 3, None])
        m = v.isna()

        s = m.schema()
        assert s.kind is bool
        assert s.nullable is False
        assert list(m) == [False, True, False, True]

    def test_fillna_removes_nullable_flag(self):
        v = Vector([1, None, 3])
        assert v.schema().nullable is True

        filled = v.fillna(0)
        s = filled.schema()
        assert s.kind is int or s.kind is float
        assert s.nullable is False
        assert list(filled) == [1, 0, 3]

    def test_dropna_removes_nullable_flag(self):
        v = Vector([1, None, 3, None, 5])
        assert v.schema().nullable is True

        dropped = v.dropna()
        s = dropped.schema()
        assert s.nullable is False
        assert list(dropped) == [1, 3, 5]

    def test_arithmetic_preserves_nullable_flag(self):
        v = Vector([1, None, 3])
        out = v * 2

        s = out.schema()
        assert s.nullable is True
        assert list(out) == [2, None, 6]


class TestTypedSubclasses:
    """Typed subclasses: _Int, _Float, etc."""

    def test_int_vector_uses_Int_subclass(self):
        from serif.vector import _Int
        v = Vector([1, 2, 3])
        assert isinstance(v, _Int)
        assert v.schema().kind is int

    def test_float_vector_uses_Float_subclass(self):
        from serif.vector import _Float
        v = Vector([1.5, 2.5])
        assert isinstance(v, _Float)
        assert v.schema().kind is float

    def test_string_vector_uses_String_subclass(self):
        from serif.vector import _String
        v = Vector(["a", "b", "c"])
        assert isinstance(v, _String)
        assert v.schema().kind is str

    def test_date_vector_uses_Date_subclass(self):
        from serif.vector import _Date
        v = Vector([date(2020, 1, 1), date(2020, 1, 2)])
        assert isinstance(v, _Date)
        assert v.schema().kind is date

    def test_promotion_does_not_change_class_but_changes_schema(self):
        from serif.vector import _Int
        v = Vector([1, 2, 3])
        assert isinstance(v, _Int)
        assert v.schema().kind is int

        v._promote(float)
        # class stays the same, dtype changes
        assert isinstance(v, _Int)
        assert v.schema().kind is float




