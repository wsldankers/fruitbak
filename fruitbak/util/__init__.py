from fruitbak.util.click import *
from fruitbak.util.env import *
from fruitbak.util.fd import *
from fruitbak.util.heapmap import *
from fruitbak.util.locking import *
from fruitbak.util.oo import *
from fruitbak.util.strbytes import *
from fruitbak.util.tabulate import *
from fruitbak.util.threadpool import *
from fruitbak.util.weak import *
from fruitbak.util.weakheapmap import *

import re

try:
	from time import time_ns
except ImportError:
	from time import time
	def time_ns():
		return int(time() * 1000000000.0)

_parse_interval_re = re.compile('\s*(\d+)\s*([smhdwlqy]|[mun]s)\s*', re.IGNORECASE)

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
	q = (1, 4),
	y = (0, 1),
)

def parse_interval(s, now = None):
	m = _parse_date_re.match(s)
	if m is None:
		raise Exception("unable to parse date expression '%s'" % (s,))

	result = 0
	then = None

	while m is not None:
		number = match.group(1)
		unit = match.group(2)

		if now is None:
			multiplier = _parse_interval_units[unit]
		else:
			adjustment = _parse_interval_timestruct_adjustment.get(unit)
			if adjustment is not None:
				if then is None:
					stime = now // 1000000000
					localnow = localtime(stime)
					then = list(now)
				what, howmuch = adjustment
				then[what] -= howmuch * number
			else:	
				multiplier = _parse_interval_units[unit]

		m = _parse_date_re.match(s, m.start())

	if then is not None:
		result += (stime - mktime(tuple(then))) * 1000000000

	return result
