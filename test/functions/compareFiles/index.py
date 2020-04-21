# -*- coding: utf-8 -*-
import logging
import os
import oss2
import json
import time
import subprocess
import random
import string
from task_queue import TaskQueue

def handler(event, context):
  logger = logging.getLogger()
  evt = json.loads(event)
  logger.info("Handling event: %s", evt)
  src_endpoint = 'https://oss-%s-internal.aliyuncs.com' % context.region
  src_client = get_oss_client(context, src_endpoint, evt["src_bucket"])
  dest_client = get_oss_client(context, evt['dest_oss_endpoint'], evt["dest_bucket"])

  crcs = evt.get("crcs")
  failed_crcs = []

  start_time = time.time()

  count = evt.get("count", 10)
  base_size = evt.get("base_size", 1024)
  def producer(queue):
    for i in range(count):
      queue.put(i)


  def consumer(queue):
    while queue.ok():
      item = queue.get()
      if item is None:
          break
      i = item
      key = '%s/%d' % (evt["prefix"], i)
      result = dest_client.head_object(key)
      crc = result.headers["x-oss-hash-crc64ecma"]
      if crcs != None:
        if crc != str(crcs[i]):
          logger.info("expected %s, actual %s", crcs[i], crc)
          failed_crcs.append(i)
      else:
        result = src_client.head_object(key)
        src_crc = result.headers["x-oss-hash-crc64ecma"]
        if crc != str(src_crc):
          logger.info("expected %s, actual %s", src_crc, crc)
          failed_crcs.append(i)

  task_q = TaskQueue(producer, [consumer] * 16)
  task_q.run()

  return {'failed_crcs': failed_crcs, 'success': len(failed_crcs) == 0}


def get_oss_client(context, endpoint, bucket):
  creds = context.credentials
  if creds.security_token != None:
    auth = oss2.StsAuth(creds.access_key_id, creds.access_key_secret, creds.security_token)
  else:
    # for local testing, use the public endpoint
    endpoint = str.replace(endpoint, "-internal", "")
    auth = oss2.Auth(creds.access_key_id, creds.access_key_secret)
  return oss2.Bucket(auth, endpoint, bucket)
