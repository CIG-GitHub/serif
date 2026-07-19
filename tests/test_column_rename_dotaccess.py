import pytest

from serif import SerifTypeError, Table


def test_table_owned_column_vector_name_is_frozen():
    t = Table({'old_name': [1, 2, 3], 'other': [4, 5, 6]})
    col = t.old_name

    with pytest.raises(SerifTypeError, match=r"t = t\.rename"):
        col.vector_name = 'new_name'

    assert t.column_names() == ['old_name', 'other']
    assert list(t.old_name) == [1, 2, 3]


def test_table_owned_column_alias_is_frozen():
    t = Table({'old_name': [1, 2, 3]})

    with pytest.raises(SerifTypeError, match="metadata is frozen"):
        t.old_name.alias('new_name')

    assert t.column_names() == ['old_name']


def test_copied_column_metadata_is_independent_and_mutable():
    t = Table({'old_name': [1, 2, 3]})
    col = t.old_name.copy().alias('new_name')

    assert col.vector_name == 'new_name'
    assert t.column_names() == ['old_name']


def test_rename_remains_owner_addressed_and_non_mutating():
    t = Table({'lowercase': [1, 2, 3]})
    renamed = t.rename({'lowercase': 'UPPERCASE'})

    assert t.column_names() == ['lowercase']
    assert renamed.column_names() == ['UPPERCASE']
    assert list(renamed.UPPERCASE) == [1, 2, 3]
    assert list(renamed.uppercase) == [1, 2, 3]
