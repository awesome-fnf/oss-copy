# -*- coding: utf-8 -*-
import logging
import os
import oss2
import json
import time

from task_queue import TaskQueue
from oss_client import get_oss_client

# event format
# {
#   "bucket": "",
#   "dest_bucket": "",
#   "key": "",
#   "upload_id": "",
#   "group_id": 1,
#   "part_size": 1024,
#   "total_size": 1025,
#   "num_of_parts_per_group": 10,
#   "total_num_of_parts": 37
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

  parts = copy(gen_parts, src_client, dest_client, evt["key"], evt["part_size"], evt["total_size"], evt["upload_id"],
    evt["group_id"], evt["num_of_parts_per_group"], evt["total_num_of_parts"])
  return {"parts": parts}

# Extract this function from copy for unit testing
def gen_parts(queue, part_size, total_size, group_id, num_of_parts_per_group, total_num_of_parts):
  start_part_num = group_id*num_of_parts_per_group
  for part_id in range(start_part_num, min(start_part_num+num_of_parts_per_group, total_num_of_parts)):
    part_range = (part_id*part_size, min((part_id+1)*part_size, total_size)-1)
    queue.put((part_id+1, part_range))

def copy(gen_parts, src_client, dest_client, key, part_size, total_size, upload_id, group_id, num_of_parts_per_group, total_num_of_parts):
  logger = logging.getLogger()
  logger.info("Starting to copy %s, group_id %d", key, group_id)
  start_time = time.time()

  def producer(queue):
    gen_parts(queue, part_size, total_size, group_id, num_of_parts_per_group, total_num_of_parts)

  parts = []
  def consumer(queue):
    while queue.ok():
      item = queue.get()
      if item is None:
          break

      part_no, part_range = item
      logger.info("%d -> %s", part_no, part_range)
      object_stream = src_client.get_object(key, byte_range=part_range)
      res = dest_client.upload_part(key, upload_id, part_no, object_stream)
      parts.append({"part_no": part_no, "etag": res.etag})

  task_q = TaskQueue(producer, [consumer] * 16)
  task_q.run()

  end_time = time.time()
  logger.info('Copied %s in %s secs', key, end_time-start_time)
  return parts
