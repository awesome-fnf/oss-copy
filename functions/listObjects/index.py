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
  group_threshold = evt.get("group_threshold", 50)
  total_group_count = evt.get("total_group_count", 0)
  medium_file_limit = evt.get("medium_file_limit")
  small_file_limit = evt.get("small_file_limit")
  small_group_total = 0
  small_group = [] # The total size of small files is less than medium_file_limit
  small = [] # An array of small_group
  medium = []
  large = []
  current_group_size = 0
  leave_early = False

  while True:
    result = src_client.list_objects(prefix = evt["prefix"], marker = marker, delimiter = evt["delimiter"], max_keys = 500)
    logger.info("Found %d objects", len(result.object_list))
    marker = result.next_marker
    has_more = result.is_truncated
    # A function can process amount of files up to medium_file_limit
    for i in range(0, len(result.object_list)):
      obj = result.object_list[i]
      logger.info("key: %s, size: %s, group size: %d", obj.key, obj.size, current_group_size)
      if (current_group_size*medium_file_limit + small_group_total + obj.size + medium_file_limit - 1) // medium_file_limit > group_threshold:
        # Leave early and override has_more and marker
        has_more = True
        leave_early = True
        marker = result.object_list[i].key
        break
      # Group small files as many as possible but their total size should not exceed medium_file_limit
      if obj.size <= small_file_limit:
        if obj.size + small_group_total <= medium_file_limit:
          small_group_total += obj.size
          small_group.append(obj.key)
        else:
          small.append(small_group)
          small_group_total = obj.size
          small_group = []
          small_group.append(obj.key)
          current_group_size += 1
      elif obj.size <= medium_file_limit:
        medium.append([obj.key, obj.size])
        current_group_size += 1
      else:
        large.append([obj.key, obj.size])
        # The large file will be divided into small groups and each group size is up to medium_file_limit
        current_group_size += (obj.size + medium_file_limit - 1) // medium_file_limit

    if not has_more or leave_early:
      break

  if len(small_group) > 0:
    small.append(small_group)

  total_group_count += (current_group_size*medium_file_limit + small_group_total + medium_file_limit - 1) // medium_file_limit

  return {
    "small": small, # [["key1","key2","key3"],["key4","key5"]]
    "medium": medium, # [["key9",size],["key11",size]]
    "large": large, # [["key6",size],["key7",size]]
    "has_more": has_more,
    "marker": marker,
    "total_group_count": total_group_count
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