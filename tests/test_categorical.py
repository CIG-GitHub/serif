"""Tests for _Category vector — ordered string categories."""
import pytest
from serif import Vector
from serif._vector.categorical import _Category
from serif._vector.categorical import _CategoryStorage
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

    def test_null_compares_to_null(self):
        # Unknown in, unknown out (docs/null-semantics.md).
        v = Vector(['s', None, 'l'])
        c = v.categorize(SIZES)
        result = c == 's'
        assert list(result) == [True, None, False]

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

    def test_null_ne_is_null_sql_semantics(self):
        # NULL != 's' → NULL (proper SQL three-valued logic: any comparison
        # with NULL is NULL, not False — docs/null-semantics.md)
        v = Vector(['s', None, 'l'])
        c = v.categorize(SIZES)
        result = c != 's'
        assert list(result) == [False, None, True]

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

    @pytest.mark.parametrize(
        "selector, expected_values, expected_codes",
        [
            (slice(1, None), [None, 's', 'l'], [None, 1, 3]),
            (
                [True, False, True, False],
                ['m', 's'],
                [2, 1],
            ),
            (
                Vector([True, None, False, True]),
                ['m', 'l'],
                [2, 3],
            ),
            ([3, 0, 3], ['l', 'm', 'l'], [3, 2, 3]),
            (Vector([3, 0, 3]), ['l', 'm', 'l'], [3, 2, 3]),
            ((slice(0, 2),), ['m', None], [2, None]),
        ],
        ids=[
            'slice',
            'boolean-list',
            'nullable-boolean-vector',
            'integer-list',
            'integer-vector',
            'tuple-slice',
        ],
    )
    def test_non_scalar_selection_stays_encoded(
        self,
        selector,
        expected_values,
        expected_codes,
        monkeypatch,
    ):
        c = Vector(
            ['m', None, 's', 'l'],
            name='size',
        ).categorize(SIZES)

        def unexpected_decode(storage):
            raise AssertionError('categorical selection decoded all labels')

        with monkeypatch.context() as context:
            context.setattr(
                _CategoryStorage,
                '__iter__',
                unexpected_decode,
            )
            selected = c[selector]

        assert isinstance(selected, _Category)
        assert selected.categories == tuple(SIZES)
        assert selected.vector_name == 'size'
        assert list(selected) == expected_values
        assert list(selected._code_storage) == expected_codes

    def test_tuple_scalar_index_still_returns_label(self):
        c = make_cat(['s', 'm', 'l'])
        assert c[(1,)] == 'm'

    def test_positional_selection_keeps_categorical_semantics(self):
        c = make_cat(['l', 'xs', 'm'])
        selected = c[[0, 1, 2]]

        assert list(selected.sort_by()) == ['xs', 'm', 'l']
        with pytest.raises(SerifValueError, match="not in the category list"):
            _ = selected < 'xxl'
        with pytest.raises(SerifValueError, match="not in the category list"):
            selected[0] = 'xxl'

    def test_mutation_rebuild_preserves_codes_and_is_atomic(self):
        c = make_cat(['m', 's', 'l'])
        c[[0, 2]] = ['l', None]

        assert list(c) == ['l', 's', None]
        assert list(c._code_storage) == [3, 1, None]
        assert c.categories == tuple(SIZES)
        assert c.schema().nullable is True

        with pytest.raises(SerifValueError, match="not in the category list"):
            c[[0, 1]] = ['s', 'xxl']

        assert list(c) == ['l', 's', None]
        assert list(c._code_storage) == [3, 1, None]


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


class TestCategoricalConcatenation:
    def test_in_domain_scalar_preserves_category(self):
        c = Vector(['low', 'medium', 'high']).categorize(
            ['low', 'medium', 'high', 'extreme']
        )

        result = c << 'extreme'

        assert isinstance(result, _Category)
        assert result.categories == ('low', 'medium', 'high', 'extreme')
        assert list(result) == ['low', 'medium', 'high', 'extreme']

    def test_none_preserves_category_and_widens_nullability(self):
        c = make_cat(['s', 'm'])

        result = c << None

        assert isinstance(result, _Category)
        assert result.categories == tuple(SIZES)
        assert result.schema().nullable is True
        assert list(result) == ['s', 'm', None]

    def test_out_of_domain_scalar_raises(self):
        c = make_cat(['s', 'm'])

        with pytest.raises(SerifValueError, match="not in the category list"):
            c << 'xxl'

    def test_matching_category_columns_preserve_domain(self):
        left = make_cat(['s', 'm'])
        right = make_cat(['l', None])

        result = left << right

        assert isinstance(result, _Category)
        assert result.categories == tuple(SIZES)
        assert result.schema().nullable is True
        assert list(result) == ['s', 'm', 'l', None]

    def test_different_category_columns_demote_to_string(self):
        left = Vector(['s', 'm']).categorize(['s', 'm', 'l'])
        right = Vector(['l', None]).categorize(['l', 'm', 's'])

        result = left << right

        assert not isinstance(result, _Category)
        assert result.schema().kind is str
        assert result.schema().nullable is True
        assert list(result) == ['s', 'm', 'l', None]

    @pytest.mark.parametrize('category_on_left', [True, False])
    def test_category_and_string_columns_produce_string(self, category_on_left):
        category = Vector(['s', 'm']).categorize(['s', 'm', 'l'])
        strings = Vector(['l'])
        left, right = (
            (category, strings)
            if category_on_left
            else (strings, category)
        )

        result = left << right

        assert not isinstance(result, _Category)
        assert result.schema().kind is str
        assert list(result) == list(left) + list(right)


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
        assert c.vector_name == 'size'
