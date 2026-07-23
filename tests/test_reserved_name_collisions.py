"""
Reserved-name / column-name collisions.

Every Python hard keyword and public method or property on Vector/Table is a
reserved name. A keyword cannot be parsed after a dot; a class attribute is
resolved before Table.__getattr__ runs. In either case the column MUST remain
reachable via:

  1. bracket access by the exact original name  -> t['first']
  2. the sanitized dot-name, which _sanitize_user_name auto-suffixes with '_'
     on a reserved-name collision                -> t.first_

The reserved set is enumerated dynamically (keyword.kwlist + dir(Vector) +
dir(Table)), so new runtime keywords and methods require no maintained list.
"""
import keyword
import warnings

import pytest
from serif import Table
from serif.naming import _get_reserved_names, _sanitize_user_name


RESERVED = sorted(_get_reserved_names())
UNREACHABLE_KEYWORDS = sorted(
    name for name in keyword.kwlist if keyword.iskeyword(name.lower())
)


def test_reserved_set_is_nonempty():
    # Guard against the enumeration silently returning nothing.
    assert len(RESERVED) > 5
    assert 'sum' in RESERVED
    assert 'first' in RESERVED
    assert 'last' in RESERVED


def test_unreachable_python_keywords_are_reserved():
    assert set(UNREACHABLE_KEYWORDS) <= set(RESERVED)


@pytest.mark.parametrize("name", RESERVED)
def test_column_named_after_reserved_name_stays_reachable(name):
    # Naming a column after a reserved name warns at construction — the
    # column has moved to `.<name>_` — but it must stay reachable both ways.
    with pytest.warns(UserWarning, match="reserved"):
        t = Table({name: [1, 2, 3]})

    # (1) Bracket access always escapes the collision.
    assert list(t[name]) == [1, 2, 3]

    # (2) Sanitized dot-access: collision appends '_', so it stays reachable.
    sanitized = _sanitize_user_name(name)
    assert sanitized.endswith('_'), f"{name!r} should sanitize with a trailing _"
    assert list(getattr(t, sanitized)) == [1, 2, 3]


@pytest.mark.parametrize("name", UNREACHABLE_KEYWORDS)
def test_keyword_column_preserves_programmatic_legacy_access(name):
    with pytest.warns(UserWarning, match="Python keyword"):
        t = Table({name: [1, 2, 3]})

    assert list(getattr(t, name)) == [1, 2, 3]
    assert name in dir(t)
    assert f"{name}_" in dir(t)


def test_keyword_warning_explains_valid_python_spelling():
    with pytest.warns(UserWarning) as rec:
        Table({'class': [1, 2, 3]})
    msg = str(rec[0].message)
    assert "Python keyword 'class'" in msg
    assert "invalid Python syntax" in msg
    assert ".class_" in msg
    assert "t['class']" in msg


def test_reserved_collision_warns_with_actionable_message():
    # The warning fires at name time and names the column, the collision, and
    # both escape routes (.sum_ and t['sum']).
    with pytest.warns(UserWarning) as rec:
        Table({'sum': [1, 2, 3]})
    msgs = [str(w.message) for w in rec if 'reserved' in str(w.message)]
    assert len(msgs) == 1
    msg = msgs[0]
    assert "'sum'" in msg
    assert ".sum_" in msg
    assert "t['sum']" in msg


def test_reserved_collision_warns_once_per_table():
    with pytest.warns(UserWarning, match="reserved") as rec:
        t = Table({'sum': [1, 2, 3]})
    assert sum('reserved' in str(w.message) for w in rec) == 1
    # Rebuilding the column map must NOT re-warn (deduped per table).
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        t._build_column_map()


def test_non_colliding_name_does_not_warn():
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        Table({'amount': [1, 2, 3]})
