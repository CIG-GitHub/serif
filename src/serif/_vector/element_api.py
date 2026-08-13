"""Curated per-dtype element capabilities."""


_CAPABILITIES = {}


def _vector_class():
    # Local import avoids a cycle while typed Vector subclasses are imported.
    from ..vector import Vector
    return Vector


def _elementwise_method(scalar_kind, method_name, result_kind):
    """Build and register one explicit per-element method."""
    def proxy(self, *args, **kwargs):
        Vector = _vector_class()
        values = (
            (
                getattr(element, method_name)(*args, **kwargs)
                if element is not None
                else None
            )
            for element in self._storage
        )
        return Vector._from_iterable_known_kind(values, result_kind)

    proxy.__name__ = method_name
    proxy.__doc__ = (
        f"Element-wise {method_name}() on each value (None passes through)."
    )
    _CAPABILITIES[(scalar_kind, method_name)] = proxy
    return proxy


def _elementwise_property(scalar_kind, attribute_name, result_kind):
    """Build and register one explicit fixed-result property."""
    def attribute(self):
        Vector = _vector_class()
        return Vector._from_iterable_known_kind(
            (
                getattr(element, attribute_name)
                if element is not None
                else None
                for element in self._storage
            ),
            result_kind,
        )

    attribute.__name__ = attribute_name
    attribute.__doc__ = (
        f"Element-wise {attribute_name} on each value (None passes through)."
    )
    descriptor = property(attribute)
    _CAPABILITIES[(scalar_kind, attribute_name)] = descriptor
    return descriptor


def resolve_capability(vector, name):
    """Bind one registered capability for a Vector's semantic dtype."""
    schema = object.__getattribute__(vector, 'schema')()
    descriptor = (
        _CAPABILITIES.get((schema.kind, name))
        if schema is not None
        else None
    )
    if descriptor is None:
        raise AttributeError(
            f"{type(vector).__name__!s} object has no attribute '{name}'"
        )
    return descriptor.__get__(vector, type(vector))


def capability_names(vector):
    """Return the registered capability names for a Vector's semantic dtype."""
    schema = object.__getattribute__(vector, 'schema')()
    if schema is None:
        return set()
    return {
        name
        for kind, name in _CAPABILITIES
        if kind is schema.kind
    }
