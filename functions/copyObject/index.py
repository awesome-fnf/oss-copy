# -*- coding: utf-8 -*-
import logging
import os
import oss2
import json
import time

from oss_client import get_oss_client


# Copy multiple objects specified by keys from src_bucket to dest_bucket.

# event format
# {
#   "src_bucket": "",
#   "dest_bucket": "",
#   "key": "a"
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

  copy(src_client, dest_client, evt["key"])

  return {}


def copy(src_client, dest_client, key):
  logger = logging.getLogger()
  logger.info("Starting to copy %s", key)
  start_time = time.time()
  object_stream = src_client.get_object(key)
  res = dest_client.put_object(key, object_stream)
  end_time = time.time()
  logger.info('Copied %s in %s secs', key, end_time-start_time)
