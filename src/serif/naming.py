"""Column name sanitization and uniquification utilities."""

from __future__ import annotations
import re


def _get_reserved_names():
	"""Get all public methods and properties from Vector and Table classes.
	
	This is computed dynamically to support future plugin extensions.
	Results are cached for performance.
	"""
	if not hasattr(_get_reserved_names, '_cache'):
		from .vector import Vector
		from .table import Table
		
		reserved = set()
		
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


def _sanitize_user_name(name) -> str | None:
	"""Sanitize column name to valid Python identifier.
	
	Rules:
	- Convert to lowercase
	- Replace runs of non-alphanumeric chars (except _) with single _
	- Strip leading/trailing underscores
	- Prefix with 'c' if starts with digit
	- Append '_' if conflicts with reserved method names
	- Return None if empty after sanitization
	"""
	if not isinstance(name, str):
		name = str(name)
	
	# Lowercase
	name = name.lower()
	
	# Replace runs of invalid characters (including _) with single _
	# This reserves __ for indexed accessors
	sanitized = re.sub(r'[^a-z0-9_]+', '_', name)
	
	# Strip leading/trailing _
	sanitized = sanitized.strip('_')
	
	# Empty → None
	if sanitized == "":
		return None
	
	# Starts with digit → prefix c
	if sanitized[0].isdigit():
		sanitized = "c" + sanitized
	
	# Conflicts with reserved name → append _
	if sanitized in _get_reserved_names():
		sanitized = sanitized + '_'
	
	return sanitized

