"""Contracts shared by deterministic physical-backend dispatch."""

import builtins
import operator

import pytest

from serif import Table
from serif import Vector
from serif.errors import SerifValueError
import serif._accel as accel
import serif._execution as execution
from serif._table import grouping as table_grouping
from serif._table import joins as table_joins
from serif._table._arrow import grouping as arrow_grouping
from serif._table._arrow import joins as arrow_joins
from serif._table._numpy import grouping as numpy_grouping
from serif._table._numpy import joins as numpy_joins
from serif._table._python import grouping as python_grouping
from serif._table._python import joins as python_joins
from serif._vector import operators as vector_ops
from serif._vector import reductions as vector_reductions
from serif._vector import selection as vector_selection
from serif._vector._arrow import operators as arrow_ops
from serif._vector._numpy import operators as numpy_ops
from serif._vector._numpy import reductions as numpy_reductions
from serif._vector._numpy import selection as numpy_selection
from serif._vector._python import reductions as python_reductions
from serif._vector._python import selection as python_selection


def test_declined_has_one_identity_and_is_not_none():
    assert execution.DECLINED is accel.DECLINED
    assert execution.DECLINED is not None


def test_execution_contract_does_not_import_public_classes():
    assert 'Vector' not in vars(execution)
    assert 'Table' not in vars(execution)


def test_missing_numpy_is_an_unavailable_backend(monkeypatch):
    real_import = builtins.__import__

    def missing_numpy(name, *args, **kwargs):
        if name == 'numpy':
            raise ImportError('numpy unavailable for test')
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, '__import__', missing_numpy)
    assert execution._load_numpy() is None


def test_missing_arrow_is_an_unavailable_backend(monkeypatch):
    real_import = builtins.__import__

    def missing_arrow(name, *args, **kwargs):
        if name == 'pyarrow' or name.startswith('pyarrow.'):
            raise ImportError('pyarrow unavailable for test')
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, '__import__', missing_arrow)
    assert execution._load_arrow() == (None, None)


def test_optional_import_defects_are_not_disguised_as_unavailable(monkeypatch):
    real_import = builtins.__import__

    def broken_numpy(name, *args, **kwargs):
        if name == 'numpy':
            raise RuntimeError('backend import defect')
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, '__import__', broken_numpy)
    with pytest.raises(RuntimeError, match='backend import defect'):
        execution._load_numpy()


def test_comparison_dispatch_is_numpy_then_arrow(monkeypatch):
    calls = []
    result = object()

    def decline_numpy(*args):
        calls.append('numpy')
        return execution.DECLINED

    def accept_arrow(*args):
        calls.append('arrow')
        return result

    monkeypatch.setattr(numpy_ops, 'compare_storage', decline_numpy)
    monkeypatch.setattr(arrow_ops, 'compare_strings', accept_arrow)

    actual = vector_ops._dispatch_compare(object(), object(), operator.eq)
    assert actual is result
    assert calls == ['numpy', 'arrow']


def test_true_division_dispatch_is_arrow_then_numpy(monkeypatch):
    calls = []
    result = object()

    def accept_arrow(*args):
        calls.append('arrow')
        return result

    def unexpected_numpy(*args):
        calls.append('numpy')
        return execution.DECLINED

    monkeypatch.setattr(arrow_ops, 'div_floats', accept_arrow)
    monkeypatch.setattr(numpy_ops, 'binop_storage', unexpected_numpy)

    actual = vector_ops._dispatch_binary(
        object(),
        object(),
        operator.truediv,
        float,
    )
    assert actual is result
    assert calls == ['arrow']


def test_true_division_decline_advances_to_numpy(monkeypatch):
    calls = []
    result = object()

    def decline_arrow(*args):
        calls.append('arrow')
        return execution.DECLINED

    def accept_numpy(*args):
        calls.append('numpy')
        return result

    monkeypatch.setattr(arrow_ops, 'div_floats', decline_arrow)
    monkeypatch.setattr(numpy_ops, 'binop_storage', accept_numpy)

    actual = vector_ops._dispatch_binary(
        object(),
        object(),
        operator.truediv,
        float,
    )
    assert actual is result
    assert calls == ['arrow', 'numpy']


def test_arithmetic_dispatch_is_numpy_then_checked_arrow(monkeypatch):
    calls = []
    result = object()

    def decline_numpy(*args):
        calls.append('numpy')
        return execution.DECLINED

    def accept_arrow(*args):
        calls.append('arrow')
        return result

    monkeypatch.setattr(numpy_ops, 'binop_storage', decline_numpy)
    monkeypatch.setattr(arrow_ops, 'binop_ints', accept_arrow)

    actual = vector_ops._dispatch_binary(
        object(),
        object(),
        operator.add,
        int,
    )
    assert actual is result
    assert calls == ['numpy', 'arrow']


def test_only_declined_advances_dispatch(monkeypatch):
    calls = []

    def return_none(*args):
        calls.append('numpy')
        return None

    def unexpected_arrow(*args):
        calls.append('arrow')
        return execution.DECLINED

    monkeypatch.setattr(numpy_ops, 'compare_storage', return_none)
    monkeypatch.setattr(arrow_ops, 'compare_strings', unexpected_arrow)

    actual = vector_ops._dispatch_compare(object(), object(), operator.eq)
    assert actual is None
    assert calls == ['numpy']


def test_declined_backends_reach_mandatory_python_path(monkeypatch):
    calls = []

    monkeypatch.setattr(
        numpy_ops,
        'binop_storage',
        lambda *args: execution.DECLINED,
    )
    monkeypatch.setattr(
        arrow_ops,
        'binop_ints',
        lambda *args: execution.DECLINED,
    )

    def python_scalar(storage, other, op_func):
        calls.append('python')
        return (4, 5)

    monkeypatch.setattr(
        vector_ops._python_ops,
        'binary_scalar',
        python_scalar,
    )

    assert list(Vector([1, 2]) + 3) == [4, 5]
    assert calls == ['python']


def test_backend_defects_propagate_without_fallback(monkeypatch):
    calls = []

    def broken_numpy(*args):
        calls.append('numpy')
        raise RuntimeError('backend execution defect')

    def unexpected_arrow(*args):
        calls.append('arrow')
        return execution.DECLINED

    monkeypatch.setattr(numpy_ops, 'compare_storage', broken_numpy)
    monkeypatch.setattr(arrow_ops, 'compare_strings', unexpected_arrow)

    with pytest.raises(RuntimeError, match='backend execution defect'):
        Vector([1, 2]) == 1
    assert calls == ['numpy']


def test_invalid_division_raises_before_backend_dispatch(monkeypatch):
    def unexpected(*args):
        raise AssertionError('invalid operation reached a backend')

    monkeypatch.setattr(arrow_ops, 'div_floats', unexpected)
    monkeypatch.setattr(numpy_ops, 'binop_storage', unexpected)
    monkeypatch.setattr(arrow_ops, 'binop_ints', unexpected)

    with pytest.raises(ZeroDivisionError):
        Vector([1.0, 2.0]) / 0.0


def test_reduction_none_is_a_completed_backend_result(monkeypatch):
    calls = []

    def numpy_none(storage):
        calls.append('numpy')
        return None

    def unexpected_python(storage):
        calls.append('python')
        raise AssertionError('successful None fell through')

    monkeypatch.setattr(numpy_reductions, 'max_', numpy_none)
    monkeypatch.setattr(python_reductions, 'max_', unexpected_python)

    assert Vector([1, 2]).max() is None
    assert calls == ['numpy']


def test_reduction_decline_reaches_mandatory_python_path(monkeypatch):
    calls = []

    def decline_numpy(storage):
        calls.append('numpy')
        return execution.DECLINED

    def accept_python(storage):
        calls.append('python')
        return 1.5

    monkeypatch.setattr(numpy_reductions, 'mean', decline_numpy)
    monkeypatch.setattr(python_reductions, 'mean', accept_python)

    assert Vector([1, 2]).mean() == 1.5
    assert calls == ['numpy', 'python']


def test_reduction_backend_defects_propagate(monkeypatch):
    def broken_numpy(storage):
        raise RuntimeError('reduction backend defect')

    monkeypatch.setattr(numpy_reductions, 'sum_', broken_numpy)

    with pytest.raises(RuntimeError, match='reduction backend defect'):
        Vector([1, 2]).sum()


def test_python_only_reductions_skip_optional_dispatch(monkeypatch):
    def unexpected_numpy():
        raise AssertionError('count reached optional dispatch')

    monkeypatch.setattr(
        vector_reductions,
        '_numpy_reductions',
        unexpected_numpy,
    )
    vector = Vector([True, None, False])
    assert vector.first() is True
    assert vector.last() is False
    assert vector.all() is False
    assert vector.any() is True
    assert vector.count() == 2


def test_reduction_layers_do_not_own_public_classes():
    for module in (
        vector_reductions,
        numpy_reductions,
        python_reductions,
    ):
        assert 'Vector' not in vars(module)
        assert 'Table' not in vars(module)


def test_disabled_numpy_reductions_decline(monkeypatch):
    storage = Vector([1, 2])._storage
    monkeypatch.setattr(numpy_reductions, '_USE_NUMPY', False)

    assert numpy_reductions.max_(storage) is execution.DECLINED
    assert numpy_reductions.min_(storage) is execution.DECLINED
    assert numpy_reductions.sum_(storage) is execution.DECLINED
    assert numpy_reductions.mean(storage) is execution.DECLINED
    assert numpy_reductions.stdev(storage) is execution.DECLINED


def test_filter_dispatch_is_numpy_then_python(monkeypatch):
    calls = []
    result = object()

    def decline_numpy(storage, mask):
        calls.append('numpy')
        return execution.DECLINED

    def accept_python(storage, mask):
        calls.append('python')
        return result

    monkeypatch.setattr(
        numpy_selection,
        'filter_storage',
        decline_numpy,
    )
    monkeypatch.setattr(
        python_selection,
        'filter_storage',
        accept_python,
    )

    actual = vector_selection.filter_storage(object(), object())
    assert actual is result
    assert calls == ['numpy', 'python']


def test_take_backend_success_skips_python(monkeypatch):
    calls = []
    result = object()

    def accept_numpy(storage, indices):
        calls.append('numpy')
        return result

    def unexpected_python(storage, indices):
        calls.append('python')
        raise AssertionError('successful take fell through')

    monkeypatch.setattr(numpy_selection, 'take_storage', accept_numpy)
    monkeypatch.setattr(
        python_selection,
        'take_storage',
        unexpected_python,
    )

    actual = vector_selection.take_storage(object(), object())
    assert actual is result
    assert calls == ['numpy']


def test_zero_popcount_is_a_completed_backend_result(monkeypatch):
    calls = []

    def numpy_zero(storage):
        calls.append('numpy')
        return 0

    def unexpected_python(storage):
        calls.append('python')
        raise AssertionError('successful zero fell through')

    monkeypatch.setattr(
        numpy_selection,
        'popcount_storage',
        numpy_zero,
    )
    monkeypatch.setattr(
        python_selection,
        'popcount',
        unexpected_python,
    )

    assert vector_selection.popcount(object()) == 0
    assert calls == ['numpy']


def test_selection_backend_defects_propagate(monkeypatch):
    def broken_numpy(storage, indices):
        raise RuntimeError('selection backend defect')

    monkeypatch.setattr(numpy_selection, 'take_storage', broken_numpy)

    with pytest.raises(RuntimeError, match='selection backend defect'):
        vector_selection.take_storage(object(), object())


def test_invalid_mask_raises_before_filter_dispatch(monkeypatch):
    def unexpected_filter(storage, mask):
        raise AssertionError('invalid mask reached dispatch')

    monkeypatch.setattr(
        vector_selection,
        'filter_storage',
        unexpected_filter,
    )

    with pytest.raises(SerifValueError, match='length mismatch'):
        Vector([1, 2])[Vector([True])]


def test_padded_take_decline_remains_caller_visible(monkeypatch):
    monkeypatch.setattr(
        numpy_selection,
        'take_pad_storage',
        lambda storage, indices: execution.DECLINED,
    )

    assert vector_selection.take_pad_storage(
        object(),
        object(),
    ) is execution.DECLINED
    assert vector_selection.take_pad_values(
        Vector([10, 20])._storage,
        [1, -1, 0],
    ) == [20, None, 10]


def test_selection_layers_do_not_own_public_classes():
    for module in (
        vector_selection,
        numpy_selection,
        python_selection,
    ):
        assert 'Vector' not in vars(module)
        assert 'Table' not in vars(module)


def test_disabled_numpy_selection_declines(monkeypatch):
    storage = Vector([1, 2])._storage
    mask = Vector([True, False])._storage
    monkeypatch.setattr(numpy_selection, '_USE_NUMPY', False)

    assert numpy_selection.filter_storage(
        storage,
        mask,
    ) is execution.DECLINED
    assert numpy_selection.take_storage(
        storage,
        [0],
    ) is execution.DECLINED
    assert numpy_selection.take_pad_storage(
        storage,
        [-1],
    ) is execution.DECLINED
    assert numpy_selection.popcount_storage(mask) is execution.DECLINED


def test_grouping_dispatch_is_numpy_then_arrow(monkeypatch):
    calls = []
    result = object()

    def decline_numpy(storage):
        calls.append('numpy')
        return execution.DECLINED

    def accept_arrow(storage):
        calls.append('arrow')
        return result

    monkeypatch.setattr(numpy_grouping, 'group_indices', decline_numpy)
    monkeypatch.setattr(arrow_grouping, 'group_strings', accept_arrow)

    assert table_grouping._dispatch_single_key(object()) is result
    assert calls == ['numpy', 'arrow']


def test_grouping_none_is_a_completed_backend_result(monkeypatch):
    calls = []

    def numpy_none(storage):
        calls.append('numpy')
        return None

    def unexpected_arrow(storage):
        calls.append('arrow')
        raise AssertionError('successful None fell through')

    monkeypatch.setattr(numpy_grouping, 'group_indices', numpy_none)
    monkeypatch.setattr(arrow_grouping, 'group_strings', unexpected_arrow)

    assert table_grouping._dispatch_single_key(object()) is None
    assert calls == ['numpy']


def test_grouping_declines_reach_mandatory_python_path(monkeypatch):
    calls = []
    original = python_grouping.bucket_rows

    monkeypatch.setattr(
        numpy_grouping,
        'group_indices',
        lambda storage: execution.DECLINED,
    )
    monkeypatch.setattr(
        arrow_grouping,
        'group_strings',
        lambda storage: execution.DECLINED,
    )

    def spy_python(*args, **kwargs):
        calls.append('python')
        return original(*args, **kwargs)

    monkeypatch.setattr(python_grouping, 'bucket_rows', spy_python)

    result = Table({'g': [2, 1, 2]}).aggregate(groupby='g')
    assert list(result.g) == [2, 1]
    assert calls == ['python']


def test_grouping_backend_defects_propagate(monkeypatch):
    calls = []

    def broken_numpy(storage):
        calls.append('numpy')
        raise RuntimeError('grouping backend defect')

    def unexpected_arrow(storage):
        calls.append('arrow')
        return execution.DECLINED

    monkeypatch.setattr(numpy_grouping, 'group_indices', broken_numpy)
    monkeypatch.setattr(arrow_grouping, 'group_strings', unexpected_arrow)

    with pytest.raises(RuntimeError, match='grouping backend defect'):
        Table({'g': [1, 2]}).aggregate(groupby='g')
    assert calls == ['numpy']


def test_arrow_grouping_defects_propagate_without_python_fallback(monkeypatch):
    calls = []

    def decline_numpy(storage):
        calls.append('numpy')
        return execution.DECLINED

    def broken_arrow(storage):
        calls.append('arrow')
        raise RuntimeError('Arrow grouping backend defect')

    def unexpected_python(*args, **kwargs):
        calls.append('python')
        raise AssertionError('backend defect reached Python fallback')

    monkeypatch.setattr(numpy_grouping, 'group_indices', decline_numpy)
    monkeypatch.setattr(arrow_grouping, 'group_strings', broken_arrow)
    monkeypatch.setattr(python_grouping, 'bucket_rows', unexpected_python)

    with pytest.raises(RuntimeError, match='Arrow grouping backend defect'):
        Table({'g': ['a', 'b']}).aggregate(groupby='g')
    assert calls == ['numpy', 'arrow']


def test_invalid_group_key_length_raises_before_dispatch(monkeypatch):
    def unexpected_optional(storage):
        raise AssertionError('invalid group key reached dispatch')

    monkeypatch.setattr(numpy_grouping, 'group_indices', unexpected_optional)
    monkeypatch.setattr(arrow_grouping, 'group_strings', unexpected_optional)

    table = Table({'x': [1, 2, 3]})
    with pytest.raises(SerifValueError, match='has length 2'):
        table.aggregate(groupby=Vector([1, 2]))


def test_multi_key_grouping_uses_canonical_python_path(monkeypatch):
    def unexpected_optional(storage):
        raise AssertionError('multi-key grouping reached optional dispatch')

    monkeypatch.setattr(numpy_grouping, 'group_indices', unexpected_optional)
    monkeypatch.setattr(arrow_grouping, 'group_strings', unexpected_optional)

    result = Table({
        'a': [1, 1, 2],
        'b': ['x', 'y', 'x'],
    }).aggregate(groupby=['a', 'b'])
    assert list(zip(result.a, result.b)) == [
        (1, 'x'),
        (1, 'y'),
        (2, 'x'),
    ]


def test_window_row_keys_use_canonical_python_grouping(monkeypatch):
    def unexpected_optional(storage):
        raise AssertionError('window row keys reached optional dispatch')

    monkeypatch.setattr(numpy_grouping, 'group_indices', unexpected_optional)
    monkeypatch.setattr(arrow_grouping, 'group_strings', unexpected_optional)

    result = Table({'g': [1, 2, 1], 'x': [3, 4, 5]}).window(
        groupby='g',
        aggregations={'total': lambda group: group.x.sum()},
    )
    assert list(result.total) == [8, 4, 8]


def test_grouping_physical_layers_do_not_own_public_classes():
    for module in (
        numpy_grouping,
        arrow_grouping,
        python_grouping,
    ):
        assert 'Vector' not in vars(module)
        assert 'Table' not in vars(module)


def test_disabled_grouping_backends_decline(monkeypatch):
    int_storage = Vector([1, 2])._storage
    string_storage = Vector(['a', 'b'])._storage

    monkeypatch.setattr(numpy_grouping, '_USE_NUMPY', False)
    assert numpy_grouping.group_indices(int_storage) is execution.DECLINED
    assert arrow_grouping.group_strings(
        string_storage
    ) is execution.DECLINED

    monkeypatch.undo()
    monkeypatch.setattr(arrow_grouping, '_USE_ARROW', False)
    assert arrow_grouping.group_strings(
        string_storage
    ) is execution.DECLINED


def test_join_dispatch_uses_effective_cascade(monkeypatch):
    calls = []
    result = object()

    def decline(name):
        def implementation(*args):
            calls.append(name)
            return execution.DECLINED
        return implementation

    def accept_strings(*args):
        calls.append('arrow_sorted')
        return result

    monkeypatch.setattr(
        numpy_joins,
        'probe_int64_dense',
        decline('numpy_dense'),
    )
    monkeypatch.setattr(
        arrow_joins,
        'probe_strings_hash',
        decline('arrow_hash'),
    )
    monkeypatch.setattr(
        numpy_joins,
        'probe_int64',
        decline('numpy_sorted'),
    )
    monkeypatch.setattr(arrow_joins, 'probe_strings', accept_strings)

    actual = table_joins._dispatch_single_key_join(
        object(), object(), False, True, True, False
    )
    assert actual is result
    assert calls == [
        'numpy_dense',
        'arrow_hash',
        'numpy_sorted',
        'arrow_sorted',
    ]


def test_join_diagnostic_is_a_completed_backend_outcome(monkeypatch):
    calls = []
    diagnostic = ('right_dup', (2,), 2)

    def dense_diagnostic(*args):
        calls.append('numpy_dense')
        return diagnostic

    def unexpected(*args):
        calls.append('unexpected')
        return execution.DECLINED

    monkeypatch.setattr(
        numpy_joins,
        'probe_int64_dense',
        dense_diagnostic,
    )
    monkeypatch.setattr(arrow_joins, 'probe_strings_hash', unexpected)
    monkeypatch.setattr(numpy_joins, 'probe_int64', unexpected)
    monkeypatch.setattr(arrow_joins, 'probe_strings', unexpected)

    actual = table_joins._dispatch_single_key_join(
        object(), object(), False, True, False, False
    )
    assert actual is diagnostic
    assert calls == ['numpy_dense']


def test_join_none_is_not_decline(monkeypatch):
    calls = []

    def dense_none(*args):
        calls.append('numpy_dense')
        return None

    def unexpected(*args):
        calls.append('unexpected')
        return execution.DECLINED

    monkeypatch.setattr(numpy_joins, 'probe_int64_dense', dense_none)
    monkeypatch.setattr(arrow_joins, 'probe_strings_hash', unexpected)
    monkeypatch.setattr(numpy_joins, 'probe_int64', unexpected)
    monkeypatch.setattr(arrow_joins, 'probe_strings', unexpected)

    actual = table_joins._dispatch_single_key_join(
        object(), object(), False, True, False, False
    )
    assert actual is None
    assert calls == ['numpy_dense']


def test_join_declines_reach_mandatory_python_path(monkeypatch):
    calls = []
    original = python_joins.probe

    for module, name in (
        (numpy_joins, 'probe_int64_dense'),
        (arrow_joins, 'probe_strings_hash'),
        (numpy_joins, 'probe_int64'),
        (arrow_joins, 'probe_strings'),
    ):
        monkeypatch.setattr(
            module,
            name,
            lambda *args: execution.DECLINED,
        )

    def spy_python(*args, **kwargs):
        calls.append('python')
        return original(*args, **kwargs)

    monkeypatch.setattr(python_joins, 'probe', spy_python)

    left = Table({'key': [1, 2], 'x': [10, 20]})
    right = Table({'key': [2], 'y': [30]})
    result = left.left_join(right, 'key', 'key')
    assert list(result.y) == [None, 30]
    assert calls == ['python']


def test_join_backend_defects_propagate_without_fallback(monkeypatch):
    calls = []

    def broken_dense(*args):
        calls.append('numpy_dense')
        raise RuntimeError('join backend defect')

    def unexpected(*args):
        calls.append('unexpected')
        return execution.DECLINED

    monkeypatch.setattr(numpy_joins, 'probe_int64_dense', broken_dense)
    monkeypatch.setattr(arrow_joins, 'probe_strings_hash', unexpected)
    monkeypatch.setattr(numpy_joins, 'probe_int64', unexpected)
    monkeypatch.setattr(arrow_joins, 'probe_strings', unexpected)
    monkeypatch.setattr(python_joins, 'probe', unexpected)

    left = Table({'key': [1]})
    right = Table({'key': [1]})
    with pytest.raises(RuntimeError, match='join backend defect'):
        left.inner_join(right, 'key', 'key')
    assert calls == ['numpy_dense']


def test_arrow_join_backend_defects_propagate(monkeypatch):
    calls = []

    def decline_dense(*args):
        calls.append('numpy_dense')
        return execution.DECLINED

    def broken_arrow(*args):
        calls.append('arrow_hash')
        raise RuntimeError('Arrow join backend defect')

    def unexpected(*args):
        calls.append('unexpected')
        return execution.DECLINED

    monkeypatch.setattr(numpy_joins, 'probe_int64_dense', decline_dense)
    monkeypatch.setattr(arrow_joins, 'probe_strings_hash', broken_arrow)
    monkeypatch.setattr(numpy_joins, 'probe_int64', unexpected)
    monkeypatch.setattr(arrow_joins, 'probe_strings', unexpected)
    monkeypatch.setattr(python_joins, 'probe', unexpected)

    left = Table({'key': ['a']})
    right = Table({'key': ['a']})
    with pytest.raises(RuntimeError, match='Arrow join backend defect'):
        left.inner_join(right, 'key', 'key')
    assert calls == ['numpy_dense', 'arrow_hash']


def test_invalid_join_keys_raise_before_dispatch(monkeypatch):
    def unexpected(*args):
        raise AssertionError('invalid join keys reached dispatch')

    monkeypatch.setattr(table_joins, '_dispatch_single_key_join', unexpected)

    left = Table({'key': [1, 2, 3]})
    right = Table({'key': [1, 2]})
    with pytest.raises(SerifValueError, match='Left join key.*length 2'):
        left.inner_join(right, Vector([1, 2]), 'key')


def test_multi_key_join_skips_optional_dispatch(monkeypatch):
    def unexpected(*args):
        raise AssertionError('multi-key join reached optional dispatch')

    monkeypatch.setattr(table_joins, '_dispatch_single_key_join', unexpected)

    left = Table({'a': [1, 1], 'b': ['x', 'y']})
    right = Table({'a': [1], 'b': ['y']})
    result = left.inner_join(right, ['a', 'b'], ['a', 'b'])
    assert list(result.a) == [1]
    assert list(result.b) == ['y']


def test_join_physical_layers_do_not_own_public_classes():
    for module in (
        numpy_joins,
        arrow_joins,
        python_joins,
    ):
        assert 'Vector' not in vars(module)
        assert 'Table' not in vars(module)


def test_disabled_join_backends_decline(monkeypatch):
    left_int = Vector([1, 2])._storage
    right_int = Vector([2])._storage
    left_string = Vector(['a', 'b'])._storage
    right_string = Vector(['b'])._storage

    monkeypatch.setattr(numpy_joins, '_USE_NUMPY', False)
    assert numpy_joins.probe_int64_dense(
        left_int, right_int, False, True, False, False
    ) is execution.DECLINED
    assert numpy_joins.probe_int64(
        left_int, right_int, False, True, False, False
    ) is execution.DECLINED
    assert arrow_joins.probe_strings_hash(
        left_string, right_string, False, True, False, False
    ) is execution.DECLINED
    assert arrow_joins.probe_strings(
        left_string, right_string, False, True, False, False
    ) is execution.DECLINED

    monkeypatch.undo()
    monkeypatch.setattr(arrow_joins, '_USE_ARROW', False)
    assert arrow_joins.probe_strings_hash(
        left_string, right_string, False, True, False, False
    ) is execution.DECLINED
    assert arrow_joins.probe_strings(
        left_string, right_string, False, True, False, False
    ) is execution.DECLINED
