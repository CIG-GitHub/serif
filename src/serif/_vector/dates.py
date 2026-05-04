from .base import Vector
from datetime import date
from datetime import datetime
from datetime import timedelta
from collections.abc import Iterable
from .dtype import DataType

class _Date(Vector):
	def __init__(self, initial=(), dtype=None, name=None, as_row=False, **kwargs):
		# dtype already set by __new__
		super().__init__(initial, dtype=dtype, name=name, as_row=as_row)

	def _elementwise_compare(self, other, op):
		other = self._check_duplicate(other)
		if isinstance(other, Vector):
			# Raise mismatched lengths
			if len(self) != len(other):
				raise ValueError(f"Length mismatch: {len(self)} != {len(other)}")
			if other.schema().kind == str:
				return Vector(tuple(bool(op(x, date.fromisoformat(y))) for x, y in zip(self, other, strict=True)), dtype=DataType(bool))
			if other.schema().kind == datetime:
				return Vector(tuple(bool(op(datetime.combine(x, datetime.time(0, 0)), y)) for x, y in zip(self, other, strict=True)), dtype=DataType(bool))
		elif isinstance(other, Iterable) and not isinstance(other, (str, bytes, bytearray)):
			# Raise mismatched lengths
			if len(self) != len(other):
				raise ValueError(f"Length mismatch: {len(self)} != {len(other)}")
			# If it's not a Vector or Constant, don't apply date compare logic
			return Vector(tuple(bool(op(x, y)) for x, y in zip(self, other, strict=True)), dtype=DataType(bool))
		elif isinstance(other, str):
			return Vector(tuple(bool(op(x, date.fromisoformat(other))) for x in self), dtype=DataType(bool))
		elif isinstance(other, datetime):
			return Vector(tuple(bool(op(datetime.combine(x, datetime.time(0, 0)), other)) for x in self), dtype=DataType(bool))
		# finally, 
		return super()._elementwise_compare(other, op)


	def ctime(self, *args, **kwargs):
		return Vector(tuple((s.ctime(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def fromisocalendar(self, *args, **kwargs):
		return Vector(tuple((s.fromisocalendar(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def fromisoformat(self, *args, **kwargs):
		return Vector(tuple((s.fromisoformat(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def fromordinal(self, *args, **kwargs):
		return Vector(tuple((s.fromordinal(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def fromtimestamp(self, *args, **kwargs):
		return Vector(tuple((s.fromtimestamp(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def isocalendar(self, *args, **kwargs):
		return Vector(tuple((s.isocalendar(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def isoformat(self, *args, **kwargs):
		return Vector(tuple((s.isoformat(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def isoweekday(self, *args, **kwargs):
		return Vector(tuple((s.isoweekday(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def replace(self, *args, **kwargs):
		return Vector(tuple((s.replace(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def strftime(self, *args, **kwargs):
		return Vector(tuple((s.strftime(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def timetuple(self, *args, **kwargs):
		return Vector(tuple((s.timetuple(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def today(self, *args, **kwargs):
		return Vector(tuple((s.today(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def toordinal(self, *args, **kwargs):
		return Vector(tuple((s.toordinal(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def weekday(self, *args, **kwargs):
		return Vector(tuple((s.weekday(*args, **kwargs) if s is not None else None) for s in self._underlying))

	def __add__(self, other):
		""" adding integers is adding days """
		if isinstance(other, Vector) and other.schema().kind == int:
			if len(self) != len(other):
				raise ValueError(f"Length mismatch: {len(self)} != {len(other)}")
			return Vector(tuple(
				(date.fromordinal(s.toordinal() + y) if s is not None and y is not None else None)
				for s, y in zip(self._underlying, other, strict=True)
			))

		if isinstance(other, int):
			return Vector(tuple((date.fromordinal(s.toordinal() + other) if s is not None else None) for s in self._underlying))
		return super().add(other)

	def eomonth(self):
		out = []
		for d in self._underlying:
			if d is None:
				out.append(None)
				continue

			# move to first of next month
			first_next = (d.replace(day=28) + timedelta(days=4)).replace(day=1)

			# back up one day -> last day of original month
			last = first_next - timedelta(days=1)

			out.append(last)

		return Vector(tuple(out))




