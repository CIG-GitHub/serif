"""Mandatory pure-Python physical implementation for row bucketing."""


def bucket_rows(storages, nrows, *, track_row_keys=False):
    """Return first-appearance buckets; null components are bucket values."""
    partition_index = {}
    row_keys = [None] * nrows if track_row_keys else None

    for row_index in range(nrows):
        # Grouping classifies rows rather than asserting equality. Unlike a
        # join key, None is therefore an ordinary coordinate in the bucket.
        key = tuple(storage[row_index] for storage in storages)
        if row_keys is not None:
            row_keys[row_index] = key
        bucket = partition_index.get(key)
        if bucket is None:
            partition_index[key] = [row_index]
        else:
            bucket.append(row_index)

    return partition_index, row_keys

