from re import compile as _regcomp, IGNORECASE as _IGNORECASE
from time import localtime as _localtime, mktime as _mktime
from calendar import timegm as _timegm
from fractions import Fraction as _Fraction

try:
	from time import time_ns
except ImportError:
	from time import time
	def time_ns():
		return int(time() * 1000000000.0)

_parse_interval_re = _regcomp(r'\s*(\d+)\s*([smhdwlqy]|[mun]s)\s*', _IGNORECASE)

_parse_interval_units = dict(
	ns = 1,
	us = 1000,
	ms = 1000000,
	s = 1000000000,
	m = 60000000000,
	h = 3600000000000,
	d = 86400000000000,
	w = 86400000000000 * 7,
	l = 3652425 * 86400 * 100000 // 12,
	q = 3652425 * 86400 * 100000 // 4,
	y = 3652425 * 86400 * 100000,
)

_parse_interval_timestruct_adjustment = dict(
	d = (2, 1),
	w = (2, 7),
	l = (1, 1),
	q = (1, 3),
	y = (0, 1),
)

def parse_interval(s, future = None, relative_to = None):
	match = _parse_interval_re.match(s)
	if match is None:
		raise Exception("unable to parse date expression '%s'" % (s,))

	result = 0
	then = None

	localtime = _localtime
	mktime = _mktime

	if future is not None and relative_to is None:
		relative_to = time_ns()

	while match is not None:
		number = int(match.group(1))
		unit = match.group(2).lower()

		if future is None:
			result += number * _parse_interval_units[unit]
		else:
			adjustment = _parse_interval_timestruct_adjustment.get(unit)
			if adjustment is not None:
				if then is None:
					relative_to_seconds = relative_to // 1000000000
					relative_to_local = localtime(relative_to_seconds)
					then = list(relative_to_local)
				what, howmuch = adjustment
				if future:
					then[what] += howmuch * number
				else:
					then[what] -= howmuch * number
			else:
				result += number * _parse_interval_units[unit]

		match = _parse_interval_re.match(s, match.end())

	if then is not None:
		then_seconds = int(mktime(tuple(then)))
		if future:
			then_interval = then_seconds - relative_to_seconds
		else:
			then_interval = relative_to_seconds - then_seconds
		result += then_interval * 1000000000

	return result

def _day_interval(a, b, number, offset):
	a_struct = _localtime(a // 1000000000)
	a_day = int(_timegm((
		a_struct.tm_year,
		a_struct.tm_mon,
		a_struct.tm_mday,
		0, 0, 0, 0, 0, -1,
	)) // 86400) - offset
	a_day_remainder = a_day % number
	a_day //= number
	a_day_start = int(_mktime((
		a_struct.tm_year,
		a_struct.tm_mon,
		a_struct.tm_mday - a_day_remainder,
		0, 0, 0, 0, 0, -1,
	)) * 1000000000)
	a_day_end = int(_mktime((
		a_struct.tm_year,
		a_struct.tm_mon,
		a_struct.tm_mday - a_day_remainder + number,
		0, 0, 0, 0, 0, -1,
	)) * 1000000000)
	a_day_ratio = _Fraction(a - a_day_start, a_day_end - a_day_start)

	b_struct = _localtime(b // 1000000000)
	b_day = (int(_timegm((
		b_struct.tm_year,
		b_struct.tm_mon,
		b_struct.tm_mday,
		0, 0, 0, 0, 0, -1,
	)) // 86400) - offset)
	b_day_remainder = b_day % number
	b_day //= number
	if a_day == b_day:
		b_day_start = a_day_start
		b_day_end = a_day_end
	else:
		b_day_start = int(_mktime((
			b_struct.tm_year,
			b_struct.tm_mon,
			b_struct.tm_mday - b_day_remainder,
			0, 0, 0, 0, 0, -1,
		)) * 1000000000)
		b_day_end = int(_mktime((
			b_struct.tm_year,
			b_struct.tm_mon,
			b_struct.tm_mday - b_day_remainder + number,
			0, 0, 0, 0, 0, -1,
		)) * 1000000000)
	b_day_ratio = _Fraction(b - b_day_start, b_day_end - b_day_start)

	return float(_Fraction(b_day - a_day, 1) + b_day_ratio - a_day_ratio)

def day_interval(a, b):
	return _day_interval(a, b, 1, 0)

def week_interval(a, b):
	return _day_interval(a, b, 7, 5)

def _month_interval(a, b, number):
	a_struct = _localtime(a // 1000000000)
	a_yearmonth = a_struct.tm_year * 12 + a_struct.tm_mon
	a_yearmonth_remainder = a_yearmonth % number
	a_yearmonth //= number
	a_month_start = int(_mktime((
		a_struct.tm_year,
		a_struct.tm_mon - a_yearmonth_remainder,
		1, 0, 0, 0, 0, 0, -1,
	)) * 1000000000)
	a_month_end = int(_mktime((
		a_struct.tm_year,
		a_struct.tm_mon - a_yearmonth_remainder + number,
		1, 0, 0, 0, 0, 0, -1,
	)) * 1000000000)
	a_month_ratio = _Fraction(a - a_month_start, a_month_end - a_month_start)

	b_struct = _localtime(b // 1000000000)
	b_yearmonth = b_struct.tm_year * 12 + b_struct.tm_mon
	b_yearmonth_remainder = b_yearmonth % number
	b_yearmonth //= number
	if a_yearmonth == b_yearmonth:
		b_month_start = a_month_start
		b_month_end = a_month_end
	else:
		b_month_start = int(_mktime((
			b_struct.tm_year,
			b_struct.tm_mon - b_yearmonth_remainder,
			1, 0, 0, 0, 0, 0, -1,
		)) * 1000000000)
		b_month_end = int(_mktime((
			b_struct.tm_year,
			b_struct.tm_mon - b_yearmonth_remainder + number,
			1, 0, 0, 0, 0, 0, -1,
		)) * 1000000000)
	b_month_ratio = _Fraction(b - b_month_start, b_month_end - b_month_start)

	return float(_Fraction(b_yearmonth - a_yearmonth, 1) + b_month_ratio - a_month_ratio)

def month_interval(a, b):
	return _month_interval(a, b, 1)

def quarter_interval(a, b):
	return _month_interval(a, b, 3)

def year_interval(a, b):
	a_struct = _localtime(a // 1000000000)
	a_year = a_struct.tm_year
	a_year_start = int(_mktime((
		a_struct.tm_year,
		1, 1, 0, 0, 0, 0, 0, -1,
	)) * 1000000000)
	a_year_end = int(_mktime((
		a_struct.tm_year + 1,
		1, 1, 0, 0, 0, 0, 0, -1,
	)) * 1000000000)
	a_year_ratio = _Fraction(a - a_year_start, a_year_end - a_year_start)

	b_struct = _localtime(b // 1000000000)
	b_year = b_struct.tm_year
	if a_year == b_year:
		b_year_start = a_year_start
		b_year_end = a_year_end
	else:
		b_year_start = int(_mktime((
			b_struct.tm_year,
			1, 1, 0, 0, 0, 0, 0, -1,
		)) * 1000000000)
		b_year_end = int(_mktime((
			b_struct.tm_year + 1,
			1, 1, 0, 0, 0, 0, 0, -1,
		)) * 1000000000)
	b_year_ratio = _Fraction(b - b_year_start, b_year_end - b_year_start)

	return float(_Fraction(b_year - a_year, 1) + b_year_ratio - a_year_ratio)
