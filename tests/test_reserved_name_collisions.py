"""
Reserved-method / column-name collisions.

Every public method or property on Vector/Table is a reserved name. A column
named after one is shadowed on plain dot-access (Python resolves the method
before Table.__getattr__ ever runs), but it MUST remain reachable via:

  1. bracket access by the exact original name  -> t['first']
  2. the sanitized dot-name, which _sanitize_user_name auto-suffixes with '_'
     on a reserved-name collision                -> t.first_

The reserved set is enumerated dynamically (dir(Vector) + dir(Table)), so this
automatically covers any method added later without a maintained list.
"""
import pytest
from serif import Table
from serif.naming import _get_reserved_names, _sanitize_user_name


RESERVED = sorted(_get_reserved_names())


def test_reserved_set_is_nonempty():
    # Guard against the enumeration silently returning nothing.
    assert len(RESERVED) > 5
    assert 'sum' in RESERVED
    assert 'first' in RESERVED
    assert 'last' in RESERVED


@pytest.mark.parametrize("name", RESERVED)
def test_column_named_after_reserved_method_stays_reachable(name):
    t = Table({name: [1, 2, 3]})

    # (1) Bracket access always escapes the collision.
    assert list(t[name]) == [1, 2, 3]

    # (2) Sanitized dot-access: collision appends '_', so it stays reachable.
    sanitized = _sanitize_user_name(name)
    assert sanitized.endswith('_'), f"{name!r} should sanitize with a trailing _"
    assert list(getattr(t, sanitized)) == [1, 2, 3]


def test_first_last_collision_resolved():
    # Regression for the first/last methods added for block aggregations:
    # they must not make columns of those names unreachable.
    t = Table({'first': [10, 20], 'last': [30, 40]})

    assert list(t['first']) == [10, 20]
    assert list(t['last']) == [30, 40]
    assert list(t.first_) == [10, 20]
    assert list(t.last_) == [30, 40]

    # Plain dot-access resolves to the method, not the column.
    assert callable(t.first)
    assert callable(t.last)
