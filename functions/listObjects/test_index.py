# -*- coding: utf-8 -*-

import logging
import os
import string
from unittest import TestCase, mock, main
from oss2.models import ListObjectsResult, SimplifiedObjectInfo
from index import handler


class TestIndex(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestIndex, self).__init__(*args, **kwargs)

    def test_handler_exceeds_group_limit(self):
        with mock.patch('index.oss2') as mock_oss:
            with mock.patch.dict('os.environ', {'SRC_OSS_ENDPOINT': 'ep'}):
                resp = mock.Mock()
                lor = ListObjectsResult(resp)
                lor.is_truncated = False
                lor.marker = ""
                lor.object_list = [
                    SimplifiedObjectInfo("5", 1, "", "", 5, ""),
                    SimplifiedObjectInfo("10", 1, "", "", 10, ""),
                    SimplifiedObjectInfo("5", 1, "", "", 5, ""),
                    SimplifiedObjectInfo("15", 1, "", "", 15, ""),
                    SimplifiedObjectInfo("8", 1, "", "", 8, ""),
                    SimplifiedObjectInfo("4", 1, "", "", 4, "")
                ]
                mock_oss.Bucket.return_value.list_objects.return_value = lor
                event = """
{
    "bucket": "hangzhouhangzhou",
    "prefix": "",
    "marker": "",
    "delimiter": "/",
    "small_threshold": 5,
    "large_threshold": 10,
    "max_group_size": 5
}
"""
                context = mock.Mock()
                result = handler(event, context)
                self.assertEqual(result, {
                    "has_more": True,
                    "marker": "4",
                    "small": [["5", "5"]],
                    "large": [["10", 10], ["8", 8]],
                    "xlarge": [["15", 15]]
                })

    def test_handler(self):
        with mock.patch('index.oss2') as mock_oss:
            with mock.patch.dict('os.environ', {'SRC_OSS_ENDPOINT': 'ep'}):
                resp = mock.Mock()
                lor = ListObjectsResult(resp)
                lor.is_truncated = False
                lor.marker = ""
                lor.object_list = [
                    SimplifiedObjectInfo("10", 1, "", "", 10, ""),
                    SimplifiedObjectInfo("15", 1, "", "", 15, ""),
                    SimplifiedObjectInfo("8", 1, "", "", 8, ""),
                    SimplifiedObjectInfo("5", 1, "", "", 5, ""),
                    SimplifiedObjectInfo("5", 1, "", "", 5, ""),
                    SimplifiedObjectInfo("4", 1, "", "", 4, ""),
                ]
                mock_oss.Bucket.return_value.list_objects.return_value = lor
                event = """
{
    "bucket": "hangzhouhangzhou",
    "prefix": "",
    "marker": "",
    "delimiter": "/",
    "small_threshold": 5,
    "large_threshold": 10,
    "max_group_size": 5
}
"""
                context = mock.Mock()
                result = handler(event, context)
                self.assertEqual(result, {
                    "has_more": True,
                    "marker": "4",
                    "small": [["5", "5"]],
                    "large": [["10", 10], ["8", 8]],
                    "xlarge": [["15", 15]]
                })

    def test_handler_list_object_twice(self):
        with mock.patch('index.oss2') as mock_oss:
            with mock.patch.dict('os.environ', {'SRC_OSS_ENDPOINT': 'ep'}):
                resp = mock.Mock()
                lor1 = ListObjectsResult(resp)
                lor1.is_truncated = True
                lor1.marker = ""
                lor1.object_list = [
                    SimplifiedObjectInfo("5", 1, "", "", 5, ""),
                    SimplifiedObjectInfo("10", 1, "", "", 10, ""),
                    SimplifiedObjectInfo("5", 1, "", "", 5, ""),
                    SimplifiedObjectInfo("15", 1, "", "", 15, ""),
                    SimplifiedObjectInfo("8", 1, "", "", 8, ""),
                    SimplifiedObjectInfo("4", 1, "", "", 4, "")
                ]
                lor2 = ListObjectsResult(resp)
                lor2.is_truncated = False
                lor2.marker = ""
                lor2.object_list = [
                    SimplifiedObjectInfo("5", 1, "", "", 5, ""),
                    SimplifiedObjectInfo("10", 1, "", "", 10, ""),
                    SimplifiedObjectInfo("5", 1, "", "", 5, ""),
                    SimplifiedObjectInfo("15", 1, "", "", 15, ""),
                    SimplifiedObjectInfo("8", 1, "", "", 8, ""),
                    SimplifiedObjectInfo("4", 1, "", "", 4, "")
                ]
                mock_oss.Bucket.return_value.list_objects.side_effect = [lor1, lor2]
                event = """
{
    "bucket": "hangzhouhangzhou",
    "prefix": "",
    "marker": "",
    "delimiter": "/",
    "small_threshold": 5,
    "large_threshold": 10,
    "max_group_size": 20
}
"""
                context = mock.Mock()
                result = handler(event, context)
                self.assertEqual(result, {
                    "has_more": False,
                    "marker": "",
                    "small": [["5", "5"], ["4", "5"], ["5", "4"]],
                    "large": [["10", 10], ["8", 8], ["10", 10], ["8", 8]],
                    "xlarge": [["15", 15], ["15", 15]]
                })
if __name__ == '__main__':
    main()