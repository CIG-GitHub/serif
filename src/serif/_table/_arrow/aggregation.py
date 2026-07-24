"""Optional Arrow physical implementation for grouped bound sums."""

from ..._execution import DECLINED
from ..._vector._arrow import storage as _arrow_storage
from ..._vector.storage import ArrayStorage
from ..._vector.storage import StringStorage
from . import _pa
from . import _pc
from . import _USE_ARROW


_U64 = 2**64


def _contiguous_array(chunked):
    """Return one offset-zero Arrow array from a grouped result column."""
    if chunked.num_chunks == 0:
        return _pa.array([], type=chunked.type)
    if chunked.num_chunks == 1:
        return chunked.chunk(0)
    return _pa.concat_arrays(chunked.chunks)


def grouped_sums(key_source, value_sources):
    """Hash-group one key and sum supported numeric value columns."""
    if not _USE_ARROW:
        return DECLINED

    key_schema, key_storage = key_source
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
    for _, storage in value_sources:
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

    key_array = _contiguous_array(grouped[key_name])
    if isinstance(key_storage, ArrayStorage):
        key_result = _arrow_storage.numeric_storage(key_array)
    else:
        key_result = _arrow_storage.string_storage(key_array)
    if key_result is DECLINED:
        return DECLINED

    outputs = []
    for (source_schema, storage), name in zip(value_sources, value_names):
        summed = _contiguous_array(grouped[f'{name}_sum'])
        if storage._data.typecode == 'd':
            normalized = _pc.fill_null(
                summed,
                _pa.scalar(0.0, type=summed.type),
            )
            result_storage = _arrow_storage.numeric_storage(normalized)
            if result_storage is DECLINED:
                return DECLINED
            outputs.append((source_schema, result_storage))
            continue

        wrapped = summed.to_pylist()
        counts = grouped[f'{name}_count'].to_pylist()
        minimums = grouped[f'{name}_min'].to_pylist()
        maximums = grouped[f'{name}_max'].to_pylist()

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
        outputs.append((source_schema, values))

    return (key_schema, key_result), outputs

