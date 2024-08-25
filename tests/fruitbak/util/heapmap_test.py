from unittest import TestCase, main

from fruitbak.util import *


class TestHeapMap(TestCase):
    def test_order(self):
        h = MinHeapMap((chr(ord('z') - x), 0) for x in range(26))
        for x in range(26):
            self.assertEqual(h.popkey(), chr(ord('z') - x))
        h = MinHeapMap((chr(ord('z') - x), -x) for x in range(26))
        for x in range(26):
            self.assertEqual(h.popkey(), chr(ord('a') + x))
        h = MinHeapMap((chr(ord('a') + x), 0) for x in range(26))
        h.move_to_end('n')
        for x in range(25):
            y = x + 1 if x > 12 else x
            self.assertEqual(h.popkey(), chr(ord('a') + y))
        self.assertEqual(h.popkey(), 'n')


if __name__ == '__main__':
    raise Exception("wtf")
    main()
