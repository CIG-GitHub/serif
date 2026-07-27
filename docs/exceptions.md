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

### SerifEmptyReductionWarning
Subclass of `UserWarning` — a warning, not an exception. Warned when
`all()` or `any()` reduce over zero valid values (an empty vector, or one
whose values are all null): the identity is returned (`all` → `True`,
`any` → `False`, as Python's `all([])`/`any([])` answer), and this warning
flags that the verdict came from no evidence. Pass `on_empty=` to state
the empty-case verdict and silence it. In `aggregate()`/`window()`, one
warning per output column names the empty groups so you can tell a data
problem from a legitimately sparse group.

```python
from serif import SerifEmptyReductionWarning

flags = Vector([None, None])
flags.any()                 # False, warns SerifEmptyReductionWarning
flags.any(on_empty=False)   # False — opted into deliberately; silent

import warnings
warnings.simplefilter('error', SerifEmptyReductionWarning)  # the old raise, if you want it
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


