from unittest import TestCase, main
from fruitbak.util import *
from sys import stderr

class TestHeapMap(TestCase):
	def test_order(self):
		h = MinHeapMap((chr(ord('z') - x), 0) for x in range(26))
		for x in range(26):
			self.assertEqual(h.popkey(), chr(ord('z') - x))
		h = MinHeapMap((chr(ord('z') - x), -x) for x in range(26))
		for x in range(26):
			self.assertEqual(h.popkey(), chr(ord('a') + x))

if __name__ == '__main__':
	raise Exception("wtf")
	main()
