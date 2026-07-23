"""Column name sanitization and uniquification utilities."""

from __future__ import annotations
import keyword
import re


def _get_reserved_names():
    """Get Python keywords and public Vector/Table methods and properties.

    Python hard keywords are included only when their normalized lowercase
    spelling is itself a keyword. Thus ``class`` is reserved because
    ``t.class`` cannot be parsed, while ``False`` normalizes to the reachable
    attribute ``false``. Public class names are computed from dir() rather
    than hardcoded so the set tracks the classes as they evolve, then cached
    for the life of the process — anything that adds methods after first use
    must invalidate _get_reserved_names._cache.
    """
    if not hasattr(_get_reserved_names, '_cache'):
        from .vector import Vector
        from .table import Table
        
        reserved = {
            name.lower()
            for name in keyword.kwlist
            if keyword.iskeyword(name.lower())
        }
        
        # Collect all public attributes from both classes
        for cls in (Vector, Table):
            for name in dir(cls):
                # Skip private/dunder attributes
                if name.startswith('_'):
                    continue
                # Add public methods and properties
                attr = getattr(cls, name, None)
                if callable(attr) or isinstance(attr, property):
                    reserved.add(name.lower())
        
        _get_reserved_names._cache = reserved
    
    return _get_reserved_names._cache


def _normalize_name(name) -> str | None:
    """Sanitize a column name to a valid Python identifier, WITHOUT the
    reserved-collision suffix.

    Rules:
    - Convert to lowercase
    - Replace runs of non-alphanumeric chars (except _) with single _
    - Strip leading/trailing underscores
    - Prefix with 'c' if starts with digit
    - Append '_' if it looks like the indexed-accessor pattern (name__digits)
    - Return None if empty after sanitization

    This is the shared pipeline; _sanitize_user_name adds the reserved-name
    suffix on top, and _reserved_collision checks membership against it — so
    the three stay in lockstep with no duplicated normalization.
    """
    if not isinstance(name, str):
        name = str(name)

    # Lowercase
    name = name.lower()

    # Replace runs of invalid characters with _
    sanitized = re.sub(r'[^a-z0-9_]+', '_', name)

    # Strip leading/trailing _
    sanitized = sanitized.strip('_')

    # Empty → None
    if sanitized == "":
        return None

    # Starts with digit → prefix c
    if sanitized[0].isdigit():
        sanitized = "c" + sanitized

    # If name looks like indexed accessor pattern (name__digits), append _ to disambiguate
    if re.match(r'^.+__\d+$', sanitized):
        sanitized = sanitized + '_'

    return sanitized


def _sanitize_user_name(name) -> str | None:
    """Sanitize a column name to a valid Python identifier, appending '_' when
    it would collide with a Python keyword or reserved method/attribute name.
    Returns None if empty after sanitization.
    """
    sanitized = _normalize_name(name)
    if sanitized is None:
        return None

    # Conflicts with reserved name → append _
    if sanitized in _get_reserved_names():
        sanitized = sanitized + '_'

    return sanitized


def _reserved_collision(name) -> str | None:
    """Return the reserved name a column name collides with, or None.

    A collision is either a Python hard keyword, which cannot be parsed after
    a dot, or a public method/property, which wins normal attribute lookup.
    In both cases the column remains reachable as ``t.<name>_`` or by its
    exact original bracket key.
    """
    sanitized = _normalize_name(name)
    if sanitized is not None and sanitized in _get_reserved_names():
        return sanitized
    return None


def _is_python_keyword(name: str) -> bool:
    """Return whether a normalized name is unreachable after a Python dot."""
    return keyword.iskeyword(name)


def _disambiguate(base: str, idx: int) -> str:
    """
    Canonical suffix for duplicate sanitized column names: the column at
    position idx gets '<base>__<idx>' (single separator when base already
    ends with '_'). This is THE one rule — Table._build_column_map,
    Table.__getitem__, and display._compute_headers must all agree, or a
    name shown in one place won't resolve in another.
    """
    sep = "" if base.endswith("_") else "_"
    return f"{base}{sep}_{idx}"

