from unittest import TestCase, main
from fruitbak.util import *
from sys import stderr
from gc import collect as gc

class dummy:
	def __init__(self, c):
		self._c = c
	def __id__(self):
		return id(self._c)
	def __hash__(self):
		return hash(self._c)
	def __eq__(self, other):
		return self._c == other._c
	def __repr__(self):
		return 'dummy(' + repr(self._c) + ')'

class TestWeakHeapMap(TestCase):
	def test_order(self):
		data = [(dummy(chr(ord('z') - x)), 0) for x in range(26)]
		h = MinWeakHeapMap(data)
		for x in range(26):
			self.assertEqual(h.popkey(), dummy(chr(ord('z') - x)))
		data = [(dummy(chr(ord('z') - x)), -x) for x in range(26)]
		h = MinWeakHeapMap(data)
		for x in range(26):
			self.assertEqual(h.popkey(), dummy(chr(ord('a') + x)))
		h = MinWeakHeapMap(data)
		data.pop()
		gc()
		self.assertEqual(len(h), 25)
		del data
		gc()
		self.assertEqual(len(h), 0)

if __name__ == '__main__':
	main()
