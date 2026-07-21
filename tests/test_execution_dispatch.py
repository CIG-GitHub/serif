"""Contracts shared by deterministic physical-backend dispatch."""

import builtins
import operator

import pytest

from serif import Vector
import serif._accel as accel
import serif._execution as execution
from serif._vector import operators as vector_ops
from serif._vector._arrow import operators as arrow_ops
from serif._vector._numpy import operators as numpy_ops


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
