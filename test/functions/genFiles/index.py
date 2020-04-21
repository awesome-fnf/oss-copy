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
  dest_client = get_oss_client(context, src_endpoint, evt["src_bucket"])

  start_time = time.time()

  count = evt.get("count", 10)
  base_size = evt.get("base_size", 1024)
  def producer(queue):
    for i in range(count):
      queue.put(i)


  crcs = [None]*count
  def consumer(queue):
    while queue.ok():
      item = queue.get()
      if item is None:
          break
      i = item
      size = random.gauss(64, 64)
      size = size if size > 0 else 1
      size = size if size < 128 else 128
      chars = ''.join([random.choice(string.printable) for i in range(int(size*base_size))])
      key = '%s/%d' % (evt["prefix"], i)
      result = dest_client.put_object(key, chars)
      crcs[i]= result.crc

  task_q = TaskQueue(producer, [consumer] * 16)
  task_q.run()

  end_time = time.time()
  logger.info('Saved %d objects in %s secs', len(crcs), end_time-start_time)

  return {"crcs": crcs}


def get_oss_client(context, endpoint, bucket):
  creds = context.credentials
  if creds.security_token != None:
    auth = oss2.StsAuth(creds.access_key_id, creds.access_key_secret, creds.security_token)
  else:
    # for local testing, use the public endpoint
    endpoint = str.replace(endpoint, "-internal", "")
    auth = oss2.Auth(creds.access_key_id, creds.access_key_secret)
  return oss2.Bucket(auth, endpoint, bucket)