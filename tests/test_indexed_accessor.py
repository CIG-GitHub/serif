"""
Tests for indexed column accessor pattern (e.g., t.total__5).
"""

import pytest
from serif import Table, Vector


def test_indexed_accessor_basic():
    """Test basic indexed accessor retrieval."""
    t = Table({
        'total': [1, 2, 3],
        'count': [4, 5, 6],
        'revenue': [7, 8, 9]
    })
    
    # Access by index with validation
    assert t.total__0 is t.total
    assert t.count__1 is t.count
    assert t.revenue__2 is t.revenue
    
    # All should return the same vector
    assert all(t.total__0._underlying[i] == t.total._underlying[i] for i in range(3))


def test_indexed_accessor_duplicate_names():
    """Test indexed accessor with duplicate column names."""
    t = Table({
        'total': [1, 2, 3],
        'count': [4, 5, 6],
        'total__2': [7, 8, 9]  # Second 'total' column
    })
    
    # First 'total' is at index 0
    assert t.total is t.total__0
    
    # Second 'total' is at index 2 (uniquified to 'total__2' internally)
    # The sanitized name for column 2 should still be 'total'
    col2 = t._underlying[2]
    assert col2._name == 'total__2'


def test_indexed_accessor_wrong_name():
    """Test that indexed accessor validates column name."""
    t = Table({
        'total': [1, 2, 3],
        'count': [4, 5, 6],
        'revenue': [7, 8, 9]
    })
    
    # Trying to access column 1 as 'total' should fail
    with pytest.raises(AttributeError, match="Column 1 is 'count'.*not 'total'"):
        _ = t.total__1
    
    # Trying to access column 0 as 'count' should fail
    with pytest.raises(AttributeError, match="Column 0 is 'total'.*not 'count'"):
        _ = t.count__0


def test_indexed_accessor_out_of_range():
    """Test that out-of-range indices raise errors."""
    t = Table({
        'total': [1, 2, 3],
        'count': [4, 5, 6]
    })
    
    # Index too high
    with pytest.raises(AttributeError, match="Column index 5 out of range"):
        _ = t.total__5
    
    # Negative index (should be treated as literal attribute, not found)
    with pytest.raises(AttributeError):
        _ = t.total__-1


def test_indexed_accessor_setattr():
    """Test setting columns via indexed accessor."""
    t = Table({
        'total': [1, 2, 3],
        'count': [4, 5, 6],
        'revenue': [7, 8, 9]
    })
    
    # Set column 1 with validation
    new_col = Vector([10, 20, 30])
    t.count__1 = new_col
    
    assert all(t.count._underlying[i] == new_col._underlying[i] for i in range(3))
    assert t.count._name == 'count'  # Name preserved


def test_indexed_accessor_setattr_wrong_name():
    """Test that setattr validates column name."""
    t = Table({
        'total': [1, 2, 3],
        'count': [4, 5, 6]
    })
    
    # Trying to set column 0 as 'count' should fail
    with pytest.raises(AttributeError, match="Column 0 is 'total'.*not 'count'"):
        t.count__0 = Vector([10, 20, 30])


def test_indexed_accessor_setattr_wrong_length():
    """Test that setattr validates vector length."""
    t = Table({
        'total': [1, 2, 3],
        'count': [4, 5, 6]
    })
    
    # Wrong length
    with pytest.raises(ValueError, match="length 2 != table length 3"):
        t.total__0 = Vector([10, 20])


def test_indexed_accessor_empty_base():
    """Test that __N without base name raises error."""
    t = Table({
        'total': [1, 2, 3],
        'count': [4, 5, 6]
    })
    
    # '__5' should error with clear message
    with pytest.raises(AttributeError, match="Invalid indexed accessor '__5': missing base name"):
        _ = t.__5


def test_indexed_accessor_non_numeric_suffix():
    """Test that non-numeric suffixes are treated as regular attributes."""
    t = Table({
        'total': [1, 2, 3],
        'total__abc': [4, 5, 6]  # Column literally named 'total__abc'
    })
    
    # Should access the column named 'total__abc', not fail as indexed accessor
    # Note: This would sanitize to 'total__abc' if that's a valid column name
    col = t.total__abc
    assert col._name == 'total__abc'


def test_indexed_accessor_multi_digit():
    """Test that multi-digit indices work correctly."""
    # Create table with many columns
    cols = {f'col{i}': [i*10, i*20, i*30] for i in range(15)}
    t = Table(cols)
    
    # Access column 10 and 14
    assert t.col10__10._name == 'col10'
    assert t.col14__14._name == 'col14'


def test_regular_access_still_works():
    """Test that regular column access (without index) still works."""
    t = Table({
        'total': [1, 2, 3],
        'count': [4, 5, 6]
    })
    
    # Regular access should return first match
    assert t.total is t._underlying[0]
    assert t.count is t._underlying[1]


def test_indexed_accessor_with_special_chars():
    """Test indexed accessor with column names that sanitize."""
    t = Table({
        'Total Amount': [1, 2, 3],  # Sanitizes to 'total_amount'
        'Count!': [4, 5, 6],        # Sanitizes to 'count'
        'Sum#': [7, 8, 9]           # Sanitizes to 'sum'
    })
    
    # Access using sanitized names
    assert t.total_amount__0._name == 'Total Amount'
    assert t.count__1._name == 'Count!'
    assert t.sum__2._name == 'Sum#'
