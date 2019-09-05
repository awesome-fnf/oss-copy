# -*- coding: utf-8 -*-
import logging
import os
import oss2
from task_queue import TaskQueue
import json
import time


# Copy multiple objects specified by keys from src_bucket to dest_bucket.

# event format
# {
#   "src_bucket": "",
#   "dest_bucket": "",
#   "keys": ["a","b"]
# }

def handler(event, context):
  logger = logging.getLogger()
  evt = json.loads(event)
  logger.info("Handling event: %s", evt)
  src_client = get_oss_client(context, os.environ['SRC_OSS_ENDPOINT'], evt["src_bucket"])
  dest_client = get_oss_client(context, evt.get("dest_oss_endpoint") or os.environ['DEST_OSS_ENDPOINT'], evt["dest_bucket"])

  copy(src_client, dest_client, evt["keys"])

  return {}


def copy(src_client, dest_client, keys):
  logger = logging.getLogger()
  logger.info("Starting to copy %d objects", len(keys))
  start_time = time.time()

  def producer(queue):
    for key in keys:
      queue.put(key)


  parts = []
  def consumer(queue):
    while queue.ok():
      item = queue.get()
      if item is None:
          break
      key = item
      object_stream = src_client.get_object(key)
      res = dest_client.put_object(key, object_stream)

  task_q = TaskQueue(producer, [consumer] * 16)
  task_q.run()

  end_time = time.time()
  logger.info('Copied %d objects in %s secs', len(keys), end_time-start_time)

def get_oss_client(context, endpoint, bucket):
  creds = context.credentials
  if creds.security_token != None:
    auth = oss2.StsAuth(creds.access_key_id, creds.access_key_secret, creds.security_token)
  else:
    # for local testing, use the public endpoint
    endpoint = str.replace(endpoint, "-internal", "")
    auth = oss2.Auth(creds.access_key_id, creds.access_key_secret)
  return oss2.Bucket(auth, endpoint, bucket)