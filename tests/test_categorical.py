"""Tests for _Category vector — ordered string categories."""
import pytest
from serif import Vector
from serif._vector.categorical import _Category
from serif.errors import SerifValueError
from serif.errors import SerifTypeError


SIZES = ['xs', 's', 'm', 'l', 'xl']


def make_cat(values=('m', 'xl', 's', 'xs', 'l'), categories=None):
    v = Vector(list(values))
    return v.categorize(categories or SIZES)


class TestCategoricalConstruction:
    def test_basic_construction(self):
        c = make_cat()
        assert isinstance(c, _Category)
        assert list(c) == ['m', 'xl', 's', 'xs', 'l']

    def test_categories_preserved(self):
        c = make_cat()
        assert c.categories == tuple(SIZES)

    def test_nullable_construction(self):
        v = Vector(['m', None, 'l'])
        c = v.categorize(SIZES)
        assert list(c) == ['m', None, 'l']
        assert c.schema().nullable is True

    def test_non_nullable_construction(self):
        c = make_cat()
        assert c.schema().nullable is False

    def test_value_outside_categories_raises(self):
        v = Vector(['m', 'xxl'])
        with pytest.raises(SerifValueError, match="not in the category list"):
            v.categorize(SIZES)

    def test_duplicate_categories_raises(self):
        v = Vector(['m'])
        with pytest.raises(SerifValueError, match="duplicates"):
            v.categorize(['m', 'm', 'l'])

    def test_non_string_category_raises(self):
        v = Vector(['m'])
        with pytest.raises(SerifTypeError):
            v.categorize(['m', 1, 'l'])

    def test_accepts_vector_as_categories(self):
        v = Vector(['m', 'l', 's'])
        cat_v = Vector(['s', 'm', 'l'])
        c = v.categorize(cat_v)
        assert c.categories == ('s', 'm', 'l')
        assert list(c) == ['m', 'l', 's']

    def test_unused_categories_allowed(self):
        # Categories may include values that don't appear in the data
        v = Vector(['s', 'm'])
        c = v.categorize(SIZES)  # 'xs', 'l', 'xl' unused
        assert c.categories == tuple(SIZES)
        assert list(c) == ['s', 'm']

    def test_duplicate_source_values_allowed(self):
        v = Vector(['m', 'm', 's', 'm'])
        c = v.categorize(SIZES)
        assert list(c) == ['m', 'm', 's', 'm']

    def test_empty_vector_empty_categories(self):
        v = Vector([], dtype=str)
        c = v.categorize([])
        assert len(c) == 0
        assert c.categories == ()


class TestCategoricalComparisons:
    def test_equality_scalar(self):
        c = make_cat(['s', 'm', 'l'])
        result = c == 'm'
        assert list(result) == [False, True, False]

    def test_inequality_scalar(self):
        c = make_cat(['s', 'm', 'l'])
        result = c != 'm'
        assert list(result) == [True, False, True]

    def test_less_than_scalar(self):
        # xs < s < m < l < xl
        c = make_cat(['xs', 's', 'm', 'l', 'xl'])
        result = c < 'l'
        assert list(result) == [True, True, True, False, False]

    def test_greater_than_scalar(self):
        c = make_cat(['xs', 's', 'm', 'l', 'xl'])
        result = c > 'm'
        assert list(result) == [False, False, False, True, True]

    def test_less_than_or_equal(self):
        c = make_cat(['s', 'm', 'l'])
        result = c <= 'm'
        assert list(result) == [True, True, False]

    def test_greater_than_or_equal(self):
        c = make_cat(['s', 'm', 'l'])
        result = c >= 'm'
        assert list(result) == [False, True, True]

    def test_null_always_false_in_comparison(self):
        v = Vector(['s', None, 'l'])
        c = v.categorize(SIZES)
        result = c == 's'
        assert list(result) == [True, False, False]

    def test_two_Categorys_same_categories(self):
        a = make_cat(['s', 'm', 'l'])
        b = make_cat(['m', 'm', 'm'])
        result = a < b
        assert list(result) == [True, False, False]

    def test_two_Categorys_different_categories_raises(self):
        a = Vector(['s', 'm']).categorize(['s', 'm', 'l'])
        b = Vector(['s', 'm']).categorize(['m', 's', 'l'])
        with pytest.raises(SerifValueError, match="different category lists"):
            _ = a < b

    def test_two_Categorys_different_categories_equality_by_label(self):
        # == and != compare by label value regardless of category list
        a = Vector(['s', 'm']).categorize(['s', 'm', 'l'])
        b = Vector(['s', 'm']).categorize(['m', 's', 'l'])
        assert list(a == b) == [True, True]
        assert list(a != b) == [False, False]

    def test_equality_unknown_scalar_all_false(self):
        # Value not in categories: equality returns all False, does not raise
        c = make_cat(['s', 'm', 'l'])
        result = c == 'xxl'
        assert list(result) == [False, False, False]

    def test_inequality_unknown_scalar_all_true(self):
        # 'xxl' is not in categories but is a real string — nothing equals it
        c = make_cat(['s', 'm', 'l'])
        result = c != 'xxl'
        assert list(result) == [True, True, True]

    def test_ordering_unknown_scalar_raises(self):
        c = make_cat(['s', 'm', 'l'])
        with pytest.raises(SerifValueError, match="not in the category list"):
            _ = c < 'xxl'

    def test_null_ne_is_false_sql_semantics(self):
        # NULL != 's' → False  (SQL NULL semantics: any comparison with NULL is False)
        v = Vector(['s', None, 'l'])
        c = v.categorize(SIZES)
        result = c != 's'
        assert list(result) == [False, False, True]

    def test_set_categories_raises(self):
        v = Vector(['s', 'm'])
        with pytest.raises(SerifTypeError, match="ordered"):
            v.categorize({'s', 'm', 'l'})

    def test_frozenset_categories_raises(self):
        v = Vector(['s', 'm'])
        with pytest.raises(SerifTypeError, match="ordered"):
            v.categorize(frozenset({'s', 'm', 'l'}))


class TestCategoricalSorting:
    def test_sort_ascending(self):
        c = make_cat(['l', 'xs', 'xl', 's', 'm'])
        sorted_c = c.sort_by()
        assert list(sorted_c) == ['xs', 's', 'm', 'l', 'xl']

    def test_sort_descending(self):
        c = make_cat(['l', 'xs', 'xl', 's', 'm'])
        sorted_c = c.sort_by(reverse=True)
        assert list(sorted_c) == ['xl', 'l', 'm', 's', 'xs']

    def test_sort_with_nulls_last(self):
        v = Vector(['l', None, 'xs'])
        c = v.categorize(SIZES)
        sorted_c = c.sort_by(na_last=True)
        assert list(sorted_c) == ['xs', 'l', None]

    def test_sort_with_nulls_first(self):
        v = Vector(['l', None, 'xs'])
        c = v.categorize(SIZES)
        sorted_c = c.sort_by(na_last=False)
        assert list(sorted_c) == [None, 'xs', 'l']

    def test_sort_preserves_categories(self):
        c = make_cat(['l', 's', 'm'])
        sorted_c = c.sort_by()
        assert sorted_c.categories == tuple(SIZES)


class TestCategoricalIndexing:
    def test_integer_index(self):
        c = make_cat(['s', 'm', 'l'])
        assert c[0] == 's'
        assert c[2] == 'l'

    def test_slice(self):
        c = make_cat(['xs', 's', 'm', 'l', 'xl'])
        sliced = c[1:3]
        assert isinstance(sliced, _Category)
        assert list(sliced) == ['s', 'm']
        assert sliced.categories == tuple(SIZES)

    def test_boolean_mask(self):
        c = make_cat(['xs', 's', 'm', 'l', 'xl'])
        mask = c > 's'
        filtered = c[mask]
        assert isinstance(filtered, _Category)
        assert list(filtered) == ['m', 'l', 'xl']


class TestCategoricalIsin:
    def test_isin(self):
        c = make_cat(['xs', 'm', 'xl'])
        result = c.isin(['xs', 'xl'])
        assert list(result) == [True, False, True]

    def test_isin_with_null(self):
        v = Vector(['xs', None, 'xl'])
        c = v.categorize(SIZES)
        result = c.isin(['xs', 'xl'])
        assert list(result) == [True, False, True]


class TestCategorizeNone:
    def test_none_infers_appearance_order(self):
        v = Vector(['m', 'l', 's', 'm', 'xs'])
        c = v.categorize(None)
        assert isinstance(c, _Category)
        assert c.categories == ('m', 'l', 's', 'xs')  # first-seen order
        assert list(c) == ['m', 'l', 's', 'm', 'xs']

    def test_none_excludes_null_from_categories(self):
        v = Vector(['m', None, 's', 'm'])
        c = v.categorize(None)
        assert None not in c.categories
        assert c.categories == ('m', 's')
        assert list(c) == ['m', None, 's', 'm']

    def test_none_on_single_value(self):
        v = Vector(['m', 'm', 'm'])
        c = v.categorize(None)
        assert c.categories == ('m',)

    def test_none_sorting_uses_appearance_order(self):
        # appearance order: 'l', 'xs', 'm'
        v = Vector(['l', 'xs', 'm', 'l'])
        c = v.categorize(None)
        sorted_c = c.sort_by()
        assert list(sorted_c) == ['l', 'l', 'xs', 'm']


class TestSetCategories:
    def test_reorder(self):
        c = Vector(['s', 'm', 'l']).categorize(['s', 'm', 'l'])
        c2 = c.set_categories(['l', 'm', 's'])
        assert list(c2) == ['s', 'm', 'l']
        assert c2.categories == ('l', 'm', 's')

    def test_reorder_changes_sort_order(self):
        c = Vector(['s', 'm', 'l']).categorize(['s', 'm', 'l'])
        c2 = c.set_categories(['l', 'm', 's'])
        assert list(c2.sort_by()) == ['l', 'm', 's']

    def test_add_category(self):
        c = Vector(['s', 'm']).categorize(['s', 'm'])
        c2 = c.set_categories(['xs', 's', 'm', 'l', 'xl'])
        assert list(c2) == ['s', 'm']
        assert c2.categories == ('xs', 's', 'm', 'l', 'xl')

    def test_remove_unused_category(self):
        c = Vector(['s', 'm']).categorize(['xs', 's', 'm', 'l'])
        c2 = c.set_categories(['s', 'm'])  # drop unused 'xs' and 'l'
        assert list(c2) == ['s', 'm']
        assert c2.categories == ('s', 'm')

    def test_remove_in_use_category_raises(self):
        c = Vector(['s', 'm', 'l']).categorize(['s', 'm', 'l'])
        with pytest.raises(SerifValueError, match="not in the category list"):
            c.set_categories(['s', 'm'])  # 'l' is in use

    def test_preserves_nulls(self):
        v = Vector(['s', None, 'm'])
        c = v.categorize(['s', 'm'])
        c2 = c.set_categories(['m', 's'])
        assert list(c2) == ['s', None, 'm']
        assert c2.schema().nullable is True

    def test_set_rejects_set_type(self):
        c = make_cat(['s', 'm'])
        with pytest.raises(SerifTypeError, match="ordered"):
            c.set_categories({'s', 'm', 'l'})


class TestCategoricalSchema:
    def test_schema_kind_is_str(self):
        c = make_cat()
        assert c.schema().kind is str

    def test_name_preserved(self):
        v = Vector(['s', 'm', 'l'], name='size')
        c = v.categorize(SIZES)
        assert c.name == 'size'
