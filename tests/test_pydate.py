"""_Date specific tests - date method proxying and operations"""
import pytest
from datetime import date, datetime
from serif import Vector, _Date


class TestDateCreation:
    """Test _Date automatic creation"""
    
    def test_auto_creates_Date(self):
        v = Vector([date(2025, 1, 1), date(2025, 1, 2)])
        assert isinstance(v, _Date)
        assert v.schema().kind == date


class TestDateMethods:
    """Test date method proxying"""
    
    def test_year(self):
        v = Vector([date(2025, 1, 15), date(2024, 12, 31)])
        result = v.year
        assert isinstance(result, Vector)
        assert list(result) == [2025, 2024]
    
    def test_month(self):
        v = Vector([date(2025, 1, 15), date(2025, 12, 31)])
        result = v.month
        assert list(result) == [1, 12]
    
    def test_day(self):
        v = Vector([date(2025, 1, 15), date(2025, 1, 31)])
        result = v.day
        assert list(result) == [15, 31]
    
    def test_weekday(self):
        v = Vector([date(2025, 1, 1), date(2025, 1, 2)])
        result = v.weekday()
        assert isinstance(result, Vector)
        # Just check it returns something valid
        assert all(0 <= x <= 6 for x in result)
    
    def test_isoweekday(self):
        v = Vector([date(2025, 1, 1), date(2025, 1, 2)])
        result = v.isoweekday()
        assert isinstance(result, Vector)
        assert all(1 <= x <= 7 for x in result)
    
    def test_isoformat(self):
        v = Vector([date(2025, 1, 15), date(2024, 12, 31)])
        result = v.isoformat()
        assert isinstance(result, Vector)
        assert list(result) == ['2025-01-15', '2024-12-31']
    
    def test_strftime(self):
        v = Vector([date(2025, 1, 15), date(2025, 12, 31)])
        result = v.strftime('%Y-%m-%d')
        assert list(result) == ['2025-01-15', '2025-12-31']
    
    def test_replace(self):
        v = Vector([date(2025, 1, 15), date(2025, 12, 31)])
        result = v.replace(year=2026)
        assert isinstance(result, Vector)
        assert list(result) == [date(2026, 1, 15), date(2026, 12, 31)]


class TestDateComparisons:
    """Test date comparison operations"""
    
    def test_equality_with_date(self):
        v = Vector([date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 3)])
        result = v == date(2025, 1, 2)
        assert list(result) == [False, True, False]
    
    def test_greater_than(self):
        v = Vector([date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 3)])
        result = v > date(2025, 1, 2)
        assert list(result) == [False, False, True]
    
    def test_less_than(self):
        v = Vector([date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 3)])
        result = v < date(2025, 1, 2)
        assert list(result) == [True, False, False]
    
    def test_comparison_with_string_isoformat(self):
        v = Vector([date(2025, 1, 1), date(2025, 1, 2)])
        result = v == '2025-01-02'
        assert list(result) == [False, True]


class TestDateArithmetic:
    """Test date arithmetic operations"""
    
    def test_add_int_days(self):
        v = Vector([date(2025, 1, 1), date(2025, 1, 15)])
        result = v + 10
        assert isinstance(result, Vector)
        assert list(result) == [date(2025, 1, 11), date(2025, 1, 25)]
    
    def test_add_int_vector(self):
        v = Vector([date(2025, 1, 1), date(2025, 1, 15)])
        days = Vector([5, 10])
        result = v + days
        assert list(result) == [date(2025, 1, 6), date(2025, 1, 25)]


class TestDateUtilities:
    """Test date utility methods"""
    
    def test_toordinal(self):
        v = Vector([date(2025, 1, 1), date(2025, 1, 2)])
        result = v.toordinal()
        assert isinstance(result, Vector)
        # Ordinals should be sequential
        assert result[1] == result[0] + 1
    
    def test_isocalendar(self):
        v = Vector([date(2025, 1, 1)])
        result = v.isocalendar()
        assert isinstance(result, Vector)
        # Returns IsoCalendarDate tuples
        assert hasattr(result[0], 'year')




