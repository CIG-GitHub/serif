"""Mandatory pure-Python physical implementation for Table joins."""


def probe(
    left_storages,
    right_storages,
    left_nrows,
    right_nrows,
    expect_left_unique,
    expect_right_unique,
    keep_unmatched_left,
    keep_unmatched_right,
    *,
    validate_left=None,
    validate_right=None,
):
    """Hash-index the right side and probe in left-row order."""
    left_key_data = [storage.to_tuple() for storage in left_storages]
    right_key_data = [storage.to_tuple() for storage in right_storages]

    first_duplicate_key = None
    right_index = {}
    right_index_get = right_index.get
    for row_index in range(right_nrows):
        key = tuple(data[row_index] for data in right_key_data)
        if validate_right is not None:
            validate_right(key, row_index)
        bucket = right_index_get(key)
        if bucket is None:
            right_index[key] = [row_index]
        else:
            bucket.append(row_index)
            if expect_right_unique and first_duplicate_key is None:
                first_duplicate_key = key

    if expect_right_unique and first_duplicate_key is not None:
        return (
            'right_dup',
            first_duplicate_key,
            len(right_index[first_duplicate_key]),
        )

    left_keys_seen = set() if expect_left_unique else None
    matched_right_rows = set() if keep_unmatched_right else None
    left_take = []
    right_take = []
    pad = -1

    for left_index in range(left_nrows):
        key = tuple(data[left_index] for data in left_key_data)
        if validate_left is not None:
            validate_left(key, left_index)

        if left_keys_seen is not None:
            if key in left_keys_seen:
                return 'left_dup', key, None
            left_keys_seen.add(key)

        matches = right_index_get(key)
        if matches is not None:
            for right_index_value in matches:
                if matched_right_rows is not None:
                    matched_right_rows.add(right_index_value)
                left_take.append(left_index)
                right_take.append(right_index_value)
        elif keep_unmatched_left:
            left_take.append(left_index)
            right_take.append(pad)

    if keep_unmatched_right:
        for right_index_value in range(right_nrows):
            if right_index_value not in matched_right_rows:
                left_take.append(pad)
                right_take.append(right_index_value)

    return 'ok', left_take, right_take

