"""Contracts shared by deterministic physical-backend dispatch."""

import builtins

import pytest

import serif._accel as accel
import serif._execution as execution


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
