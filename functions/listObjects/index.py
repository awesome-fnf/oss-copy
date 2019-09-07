# -*- coding: utf-8 -*-
import oss2
import json
import os
import logging

def handler(event, context):
  logger = logging.getLogger()
  evt = json.loads(event)
  logger.info("Handling event: %s", evt)
  src_client = get_oss_client(context, os.environ['SRC_OSS_ENDPOINT'], evt["bucket"])

  has_more = False
  marker = evt["marker"]
  max_group_size = evt.get("max_group_size", 50)
  large_threshold = evt.get("large_threshold")
  small_threshold = evt.get("small_threshold")
  small_group_total = 0
  small_group = [] # The total size of small files is less than large_threshold
  small = [] # An array of small_group
  large = []
  xlarge = []
  current_group_size = 0
  leave_early = False

  while True:
    result = src_client.list_objects(prefix = evt["prefix"], marker = marker, delimiter = evt["delimiter"], max_keys = 500)
    logger.info("Found %d objects", len(result.object_list))
    marker = result.next_marker
    has_more = result.is_truncated
    # A function can process amount of files up to large_threshold
    for i in range(0, len(result.object_list)):
      obj = result.object_list[i]
      logger.info("key: %s, size: %s, group size: %d", obj.key, obj.size, current_group_size)
      if (current_group_size*large_threshold + small_group_total + obj.size + large_threshold - 1) // large_threshold > max_group_size:
        # Leave early and override has_more and marker
        has_more = True
        leave_early = True
        marker = result.object_list[i].key
        break
      # Group small files as many as possible but their total size should not exceed large_threshold
      if obj.size <= small_threshold:
        if obj.size + small_group_total <= large_threshold:
          small_group_total += obj.size
          small_group.append(obj.key)
        else:
          small.append(small_group)
          small_group_total = obj.size
          small_group = []
          small_group.append(obj.key)
          current_group_size += 1
      elif obj.size <= large_threshold:
        large.append([obj.key, obj.size])
        current_group_size += 1
      else:
        xlarge.append([obj.key, obj.size])
        # The xlarge file will be divided into small groups and each group size is up to large_threshold
        current_group_size += (obj.size + large_threshold - 1) // large_threshold

    if not has_more or leave_early:
      break

  if len(small_group) > 0:
    small.append(small_group)

  return {
    "small": small, # [["key1","key2","key3"],["key4","key5"]]
    "large": large, # [["key9",size],["key11",size]]
    "xlarge": xlarge, # [["key6",size],["key7",size]]
    "has_more": has_more,
    "marker": marker
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