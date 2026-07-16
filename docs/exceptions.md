# Exception Handling

Serif raises specific exception types for clear error handling.

## Exception Types

### SerifKeyError
Subclass of `KeyError`. Raised when:
- Column not found in table
- Key missing in dictionary operations

```python
from serif import SerifKeyError

try:
    column = table['missing_column']
except SerifKeyError:
    print("Column not found")
```

### SerifValueError
Subclass of `ValueError`. Raised when:
- Invalid values provided
- Mismatched lengths in operations
- Invalid join key configurations

```python
from serif import SerifValueError

try:
    table.inner_join(other, left_on=['a', 'b'], right_on=['x'])
except SerifValueError:
    print("Mismatched join key lengths")
```

### SerifTypeError
Subclass of `TypeError`. Raised when:
- Invalid types provided to type-safe vectors
- Type mismatches in operations

```python
from serif import SerifTypeError

try:
    result = typesafe_int_vector + "string"
except SerifTypeError:
    print("Type mismatch")
```

### SerifIndexError
Subclass of `IndexError`. Raised when:
- Out-of-bounds indexing
- Invalid slice operations

```python
from serif import SerifIndexError

try:
    value = vector[1000]  # index out of range
except SerifIndexError:
    print("Index out of bounds")
```

### SerifEmptyReductionError
Subclass of `SerifValueError`. Raised when `all()` or `any()` reduce over
zero valid values (an empty vector, or one whose values are all null) —
a verdict needs evidence. Pass `on_empty=` to supply the empty-case
verdict. In `aggregate()`/`window()`, the raise carries the group key so
you can tell a data problem from a legitimately sparse group.

```python
from serif import SerifEmptyReductionError

flags = Vector([None, None])
flags.any()                 # raises SerifEmptyReductionError
flags.any(on_empty=False)   # False — opted into deliberately
```

See docs/null-semantics.md for the full doctrine.

## Broad Exception Catching

All custom exceptions inherit from `SerifError`:

```python
from serif import SerifError

try:
    # ... operations ...
except SerifError:
    # Catch all serif-specific errors
    pass
```

## Attribute Access

`table.missing_column` raises `AttributeError` (Pythonic behavior). 

Use `table['col']` for dictionary-style access or check existence with `'col' in table.column_names()`.


