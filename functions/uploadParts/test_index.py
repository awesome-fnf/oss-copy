# -*- coding: utf-8 -*-

import logging
import os
import string
import unittest
from index import gen_parts

try:
    import Queue as queue
except ImportError:
    import queue


class TestIndex(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestIndex, self).__init__(*args, **kwargs)

    def test_gen_parts(self):
        # part_size, total_size, group_id, num_of_parts_per_group, total_num_of_parts
        cases = [
            [(10, 100, 0, 4, 10), [(1, (0, 9)), (2, (10, 19)), (3, (20, 29)), (4, (30, 39))]],
            [(10, 100, 1, 4, 10), [(5, (40, 49)), (6, (50, 59)), (7, (60, 69)), (8, (70, 79))]],
            [(10, 100, 2, 4, 10), [(9, (80, 89)), (10, (90, 99))]],
            [(11, 100, 0, 4, 10), [(1, (0, 10)), (2, (11, 21)), (3, (22, 32)), (4, (33, 43))]],
            [(11, 100, 1, 4, 10), [(5, (44, 54)), (6, (55, 65)), (7, (66, 76)), (8, (77, 87))]],
            [(11, 100, 2, 4, 10), [(9, (88, 98)), (10, (99, 99))]],
        ]
        for c in cases:
            q = queue.Queue()
            part_size, total_size, group_id, num_of_parts_per_group, total_num_of_parts = c[0]
            expected = c[1]
            gen_parts(q, part_size, total_size, group_id, num_of_parts_per_group, total_num_of_parts)
            self.assertEqual(expected, list(q.queue))

if __name__ == '__main__':
    unittest.main()