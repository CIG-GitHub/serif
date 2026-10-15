import pytest


@pytest.fixture(autouse=True)
def _keep_rust_out_of_other_backend_tests(request, monkeypatch):
    """NumPy and Arrow conformance files pin THEIR backend, and Rust runs
    first wherever it is built. Switch it off there so those files keep
    testing what they name; every other file runs with whatever is built."""
    name = request.node.path.name
    if not name.startswith('test_accel_') or name.startswith('test_accel_rust'):
        return
    from serif._table._rust import joins as rust_joins
    from serif._vector._rust import operators as rust_ops
    monkeypatch.setattr(rust_ops, '_USE_RUST', False)
    monkeypatch.setattr(rust_joins, '_USE_RUST', False)
