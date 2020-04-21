# -*- coding: utf-8 -*-
import logging
import os
import oss2
from oss2.models import PartInfo
import json

from oss_client import get_oss_client


# event format
# {
#   "dest_bucket": "",
#   "key": "",
#   "upload_id": "",
#   "parts":[
#     {"part_no": 1, "etag": ""},
#     {"part_no": 2, "etag": ""}
#   ]
# }

clients = {}

def handler(event, context):
  logger = logging.getLogger()
  evt = json.loads(event)
  logger.info("Handling event: %s", evt)
  src_endpoint = 'https://oss-%s-internal.aliyuncs.com' % context.region
  dest_bucket = evt["dest_bucket"]
  dest_client = clients.get(dest_bucket)
  if dest_client is None:
    dest_client = get_oss_client(context, evt.get("dest_oss_endpoint") or os.environ.get('DEST_OSS_ENDPOINT') or src_endpoint, dest_bucket, evt.get("dest_access_role"))
    clients[dest_bucket] = dest_client

  parts = []
  for part in evt["parts"]:
    parts.append(PartInfo(part["part_no"], part["etag"]))

  dest_client.complete_multipart_upload(evt["key"], evt["upload_id"], parts)

  return {}