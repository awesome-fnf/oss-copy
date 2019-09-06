# -*- coding: utf-8 -*-
import logging
import os
import oss2
import json
import time

from task_queue import TaskQueue

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

def handler(event, context):
  logger = logging.getLogger()
  evt = json.loads(event)
  logger.info("Handling event: %s", evt)
  src_client = get_oss_client(context, os.environ['SRC_OSS_ENDPOINT'], evt["src_bucket"])
  dest_client = get_oss_client(context, evt.get("dest_oss_endpoint") or os.environ['DEST_OSS_ENDPOINT'], evt["dest_bucket"])

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

def get_oss_client(context, endpoint, bucket):
  creds = context.credentials
  if creds.security_token != None:
    auth = oss2.StsAuth(creds.access_key_id, creds.access_key_secret, creds.security_token)
  else:
    # for local testing, use the public endpoint
    endpoint = str.replace(endpoint, "-internal", "")
    auth = oss2.Auth(creds.access_key_id, creds.access_key_secret)
  return oss2.Bucket(auth, endpoint, bucket)