# -*- coding: utf-8 -*-
import oss2
import json
import os
import logging


SMALL_THRESHOLD = 10*1024*1024
LARGE_THRESHOLD = 20*1024*1024

def handler(event, context):
  logger = logging.getLogger()
  evt = json.loads(event)
  logger.info("Handling event: %s", evt)
  src_client = get_oss_client(context, os.environ['SRC_OSS_ENDPOINT'], evt["bucket"])

  result = src_client.list_objects(prefix = evt["prefix"], marker =  evt["marker"], delimiter = evt["delimiter"], max_keys = 100)
  logger.info("Found %d objects", len(result.object_list))

  small_part_total = 0
  small_part = []
  small = [] # an array of array partitioned by size
  large = []
  xlarge = []
  for obj in result.object_list:
    if obj.size <= SMALL_THRESHOLD:
      if obj.size + small_part_total <= LARGE_THRESHOLD:
        small_part_total += obj.size
        small_part.append(obj.key)
      else:
        small.append(small_part)
        small_part_total = obj.size
        small_part = []
        small_part.append(obj.key)
    elif obj.size < LARGE_THRESHOLD:
      large.append((obj.key, obj.size))
    else:
      xlarge.append((obj.key, obj.size))

  if len(small_part) > 0:
    small.append(small_part)

  return {
    "small": small, # [["key1","key2","key3"],["key4","key5"]]
    "large": large, # [["key9",123],["key11",124]]
    "xlarge": xlarge, # [["key6",1235],["key7",1234]]
    "hasMore": result.is_truncated,
    "marker": result.next_marker
  }

def get_oss_client(context, endpoint, bucket):
  creds = context.credentials
  if creds.security_token != None:
    auth = oss2.StsAuth(creds.access_key_id, creds.access_key_secret, creds.security_token)
  else:
    # for local testing, use the public endpoint
    endpoint = str.replace(endpoint, "-internal", "")
    auth = oss2.Auth(creds.access_key_id, creds.access_key_secret)
  return oss2.Bucket(auth, endpoint, bucket)