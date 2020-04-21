# -*- coding: utf-8 -*-
import logging
import os
import oss2
import json

from oss_client import get_oss_client

# event format
# {
#   "bucket": "",
#   "dest_bucket": "",
#   "key": "",
#   "upload_id": "",
#   "part_no": 1,
#   "part_size": 1024,
#   "total_size": 1025
# }
clients = {}

def handler(event, context):
  logger = logging.getLogger()
  evt = json.loads(event)
  logger.info("Handling event: %s", evt)
  src_endpoint = 'https://oss-%s-internal.aliyuncs.com' % context.region
  src_bucket = evt["src_bucket"]
  src_client = clients.get(src_bucket)
  if src_client is None:
    src_client = get_oss_client(context, src_endpoint, src_bucket)
    clients[src_bucket] = src_client
  dest_bucket = evt["dest_bucket"]
  dest_client = clients.get(dest_bucket)
  if dest_client is None:
    dest_client = get_oss_client(context, evt.get("dest_oss_endpoint") or os.environ.get('DEST_OSS_ENDPOINT') or src_endpoint, dest_bucket, evt.get("dest_access_role"))
    clients[dest_bucket] = dest_client

  # Download part of the file and upload as part
  part_no = evt["part_no"]
  part_size = evt["part_size"]
  byte_range = ((part_no-1)*part_size, min(part_no*part_size, evt["total_size"])-1)
  # src_client.get_object_to_file(evt["key"], "/tmp/testpart%d" % part_no, byte_range=byte_range)
  object_stream = src_client.get_object(evt["key"], byte_range=byte_range)
  res = dest_client.upload_part(evt["key"], evt["upload_id"], part_no, object_stream)
  return {"part_no": part_no, "etag": res.etag}
