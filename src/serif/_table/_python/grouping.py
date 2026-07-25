"""Mandatory pure-Python physical implementation for row bucketing."""


def bucket_rows(storages, nrows, *, track_row_keys=False):
    """Return canonical first-appearance buckets and optional row keys."""
    partition_index = {}
    row_keys = [None] * nrows if track_row_keys else None

    for row_index in range(nrows):
        key = tuple(storage[row_index] for storage in storages)
        if row_keys is not None:
            row_keys[row_index] = key
        bucket = partition_index.get(key)
        if bucket is None:
            partition_index[key] = [row_index]
        else:
            bucket.append(row_index)

    return partition_index, row_keys

