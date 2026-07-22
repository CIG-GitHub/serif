"""Mandatory pure-Python physical implementation for row bucketing."""


def bucket_rows(storages, nrows, *, track_row_keys=False):
    """Return canonical first-appearance buckets and optional row keys."""
    partition_index = {}
    key_count = len(storages)
    key_data = [storage.to_tuple() for storage in storages]
    row_keys = [None] * nrows if track_row_keys else None

    for row_index in range(nrows):
        key = tuple(key_data[index][row_index] for index in range(key_count))
        if row_keys is not None:
            row_keys[row_index] = key
        bucket = partition_index.get(key)
        if bucket is None:
            partition_index[key] = [row_index]
        else:
            bucket.append(row_index)

    return partition_index, row_keys

