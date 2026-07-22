"""Legacy optional Arrow grouped aggregation implementation.

Vector operator kernels and storage bridges live under
``serif._vector._arrow``. This module keeps the established ``None`` decline
contract until grouped aggregation migrates.
"""

from .._execution import DECLINED
from .._execution import _load_arrow
from .._vector._arrow import storage as _arrow_storage
from .._vector.storage import ArrayStorage
from .._vector.storage import StringStorage


_pa, _pc = _load_arrow()
_USE_ARROW = _pa is not None

_U64 = 2**64


def _legacy_result(result):
    return None if result is DECLINED else result


def string_array(storage):
    """Return an Arrow string view, or legacy ``None`` decline."""
    if not _USE_ARROW:
        return None
    return _legacy_result(_arrow_storage.string_array(storage))


def numeric_array(storage):
    """Return an Arrow numeric view, or legacy ``None`` decline."""
    if not _USE_ARROW:
        return None
    return _legacy_result(_arrow_storage.numeric_array(storage))


def int64_array(storage):
    """Return an Arrow int64 view, or legacy ``None`` decline."""
    if not _USE_ARROW:
        return None
    return _legacy_result(_arrow_storage.int64_array(storage))


def grouped_sums(key_storage, value_storages):
    """Hash-group one key and sum supported numeric value columns."""
    if not _USE_ARROW:
        return None

    if (
        isinstance(key_storage, ArrayStorage)
        and key_storage._data.typecode == 'q'
        and key_storage._mask is None
    ):
        key_array = int64_array(key_storage)
    elif (
        isinstance(key_storage, StringStorage)
        and key_storage._mask is None
    ):
        key_array = string_array(key_storage)
    else:
        return None
    if key_array is None:
        return None

    value_arrays = []
    for storage in value_storages:
        array = numeric_array(storage)
        if array is None:
            return None
        value_arrays.append(array)

    key_name = '__serif_group_key'
    value_names = [
        f'__serif_value_{index}'
        for index in range(len(value_arrays))
    ]
    table = _pa.Table.from_arrays(
        [key_array, *value_arrays],
        names=[key_name, *value_names],
    )
    specs = []
    for name in value_names:
        specs.extend([
            (name, 'sum'),
            (name, 'count'),
            (name, 'min'),
            (name, 'max'),
        ])
    try:
        grouped = table.group_by(
            key_name,
            use_threads=False,
        ).aggregate(specs)
    except (_pa.ArrowInvalid, _pa.ArrowNotImplementedError):
        return None

    keys = grouped[key_name].to_pylist()
    outputs = []
    for storage, name in zip(value_storages, value_names):
        wrapped = grouped[f'{name}_sum'].to_pylist()
        counts = grouped[f'{name}_count'].to_pylist()
        minimums = grouped[f'{name}_min'].to_pylist()
        maximums = grouped[f'{name}_max'].to_pylist()

        if storage._data.typecode == 'q':
            values = []
            for residue, count, minimum, maximum in zip(
                wrapped,
                counts,
                minimums,
                maximums,
            ):
                count = int(count)
                if count == 0:
                    values.append(0)
                    continue
                minimum = int(minimum)
                maximum = int(maximum)
                if count * (maximum - minimum) >= _U64:
                    return None
                residue = int(residue)
                spread_sum = (residue - count * minimum) % _U64
                values.append(count * minimum + spread_sum)
            outputs.append(values)
        else:
            outputs.append([
                0 if count == 0 else float(value)
                for value, count in zip(wrapped, counts)
            ])

    return keys, outputs
