"""Contracts shared by deterministic physical-backend dispatch."""

import builtins
import operator

import pytest

from serif import Vector
import serif._accel as accel
import serif._execution as execution
from serif._vector import operators as vector_ops
from serif._vector import reductions as vector_reductions
from serif._vector._arrow import operators as arrow_ops
from serif._vector._numpy import operators as numpy_ops
from serif._vector._numpy import reductions as numpy_reductions
from serif._vector._python import reductions as python_reductions


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
