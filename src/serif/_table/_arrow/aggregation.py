"""Optional Arrow physical implementation for grouped bound sums."""

from ..._execution import DECLINED
from ..._vector._arrow import storage as _arrow_storage
from ..._vector.storage import ArrayStorage
from ..._vector.storage import StringStorage
from . import _pa
from . import _USE_ARROW


_U64 = 2**64


def grouped_sums(key_storage, value_storages):
    """Hash-group one key and sum supported numeric value columns."""
    if not _USE_ARROW:
        return DECLINED

    if (
        isinstance(key_storage, ArrayStorage)
        and key_storage._data.typecode == 'q'
        and key_storage._mask is None
    ):
        key_array = _arrow_storage.int64_array(key_storage)
    elif (
        isinstance(key_storage, StringStorage)
        and key_storage._mask is None
    ):
        key_array = _arrow_storage.string_array(key_storage)
    else:
        return DECLINED
    if key_array is DECLINED:
        return DECLINED

    value_arrays = []
    for storage in value_storages:
        array = _arrow_storage.numeric_array(storage)
        if array is DECLINED:
            return DECLINED
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
    specifications = []
    for name in value_names:
        specifications.extend([
            (name, 'sum'),
            (name, 'count'),
            (name, 'min'),
            (name, 'max'),
        ])
    try:
        grouped = table.group_by(
            key_name,
            use_threads=False,
        ).aggregate(specifications)
    except (_pa.ArrowInvalid, _pa.ArrowNotImplementedError):
        return DECLINED

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
                    return DECLINED
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

