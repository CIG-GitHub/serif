"""Explicit and dynamic per-dtype element APIs."""


def _vector_class():
    # Local import avoids a cycle while Vector still imports this module to
    # delegate __getattr__.
    from ..vector import Vector
    return Vector


class MethodProxy:
    """Proxy that defers a method call to each element in a Vector."""

    def __init__(self, vector, method_name):
        self._vector = vector
        self._method_name = method_name

    def __call__(self, *args, **kwargs):
        method = self._method_name
        results = []
        for element in self._vector._storage:
            if element is None:
                results.append(None)
            else:
                results.append(getattr(element, method)(*args, **kwargs))
        return _vector_class()(results)


def _elementwise_proxy(method_name, result_kind=None):
    """Build an explicit per-element method for a typed Vector subclass."""
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
        if result_kind is not None:
            return Vector._from_iterable_known_kind(values, result_kind)
        return Vector(tuple(values))

    proxy.__name__ = method_name
    proxy.__doc__ = (
        f"Element-wise {method_name}() on each value (None passes through)."
    )
    return proxy


def _elementwise_attribute(attribute_name, result_kind):
    """Build an explicit fixed-result property for a typed Vector subclass."""
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
    return property(attribute)


def resolve(vector, name):
    """Resolve an attribute against the Vector's scalar dtype."""
    schema = object.__getattribute__(vector, 'schema')()
    if schema is None:
        raise AttributeError(f"Empty Vector has no attribute '{name}'")
    dtype_kind = schema.kind
    if dtype_kind is object:
        raise AttributeError(f"Vector[object] has no attribute '{name}'")

    class_attribute = getattr(dtype_kind, name, None)
    if class_attribute is None:
        raise AttributeError(
            f"'{dtype_kind.__name__}' object has no attribute '{name}'"
        )

    if callable(class_attribute):
        return MethodProxy(vector, name)

    Vector = _vector_class()
    return Vector(tuple(
        getattr(element, name) if element is not None else None
        for element in vector._storage
    ))
