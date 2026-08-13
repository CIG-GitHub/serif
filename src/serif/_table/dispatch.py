"""Optional accelerator routing for Table physical operations."""

from .._execution import first_supported


def group_single_key(storage):
    """Try useful optional single-key bucket implementations in order."""
    def numpy_grouping():
        from ._numpy import grouping

        return grouping.group_indices(storage)

    def arrow_grouping():
        from ._arrow import grouping

        return grouping.group_strings(storage)

    return first_supported(
        numpy_grouping,
        arrow_grouping,
    )


def join_single_key(
    left_storage,
    right_storage,
    expect_left_unique,
    expect_right_unique,
    keep_unmatched_left,
    keep_unmatched_right,
):
    """Try useful single-key join implementations in priority order."""
    arguments = (
        left_storage,
        right_storage,
        expect_left_unique,
        expect_right_unique,
        keep_unmatched_left,
        keep_unmatched_right,
    )

    def numpy_dense():
        from ._numpy import joins

        return joins.probe_int64_dense(*arguments)

    def arrow_hash():
        from ._arrow import joins

        return joins.probe_strings_hash(*arguments)

    def numpy_sorted():
        from ._numpy import joins

        return joins.probe_int64(*arguments)

    def arrow_sorted():
        from ._arrow import joins

        return joins.probe_strings(*arguments)

    return first_supported(
        numpy_dense,
        arrow_hash,
        numpy_sorted,
        arrow_sorted,
    )


def grouped_sums(key_source, value_sources):
    from ._arrow import aggregation

    return aggregation.grouped_sums(key_source, value_sources)
