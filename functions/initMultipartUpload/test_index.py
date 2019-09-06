# -*- coding: utf-8 -*-

import logging
import os
import string
import unittest
from .index import calc_groups


class TestIndex(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestIndex, self).__init__(*args, **kwargs)

    def test_calc_groups(self):
        cases = [
            # total_size, part_size, max_total_part_size
            [[100, 10, 40], [10, 3, 4]], 
            [[100, 10, 50], [10, 2, 5]],
            [[101, 10, 40], [11, 3, 4]],
            [[101, 10, 50], [11, 3, 5]],
            [[99, 10, 40], [10, 3, 4]],
            [[99, 10, 50], [10, 2, 5]],
            [[100, 15, 40], [7, 4, 2]],
            [[100, 15, 50], [7, 3, 3]],
        ]
        for c in cases:
            input = c[0]
            expected = c[1]
            t, g, p = calc_groups(input[0], input[1], input[2])
            self.assertEqual(t, expected[0], input)
            self.assertEqual(g, expected[1], input)
            self.assertEqual(p, expected[2], input)

if __name__ == '__main__':
    unittest.main()