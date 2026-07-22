"""Canonical pure-Python physical implementations for row selection."""


def filter_storage(storage, mask):
    indices = [
        index
        for index, selected in enumerate(mask)
        if selected
    ]
    return storage.take(indices)


def take_storage(storage, indices):
    return storage.take(indices)


def take_pad_values(storage, indices):
    return [
        None if index < 0 else storage[index]
        for index in indices
    ]


def popcount(mask_storage):
    return sum(1 for selected in mask_storage if selected)
