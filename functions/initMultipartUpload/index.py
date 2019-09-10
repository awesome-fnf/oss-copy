# -*- coding: utf-8 -*-
import logging
import os
import oss2
from oss2 import SizedFileAdapter, determine_part_size
import json

# event format
# {
#   "src_bucket": "",
#   "dest_bucket": "",
#   "key": "",
#   "part_size": number,
#   "total_size": number,
#   "medium_file_limit": number
# }

FNF_PARALLEL_LIMIT = 100


def handler(event, context):
  logger = logging.getLogger()
  evt = json.loads(event)
  logger.info("Handling event: %s", evt)
  src_client = get_oss_client(context, os.environ['SRC_OSS_ENDPOINT'], evt["src_bucket"])
  dest_client = get_oss_client(context, evt.get("dest_oss_endpoint") or os.environ['DEST_OSS_ENDPOINT'], evt["dest_bucket"])
  
  # Group parts by size and each group will be handled by one function execution.
  total_num_of_parts, num_of_groups, num_of_parts_per_group = calc_groups(evt["total_size"], evt["part_size"], evt["medium_file_limit"])
  upload_id = dest_client.init_multipart_upload(evt["key"]).upload_id

  return {
      "upload_id": upload_id,
      "total_num_of_parts": total_num_of_parts,
      "groups": list(range(num_of_groups)),
      "num_of_parts_per_group": num_of_parts_per_group
      }

def calc_groups(total_size, part_size, max_total_part_size):
  num_of_parts_per_group = min(max_total_part_size // part_size, FNF_PARALLEL_LIMIT - 20)
  total_num_of_parts = (total_size + part_size - 1) // part_size
  num_of_groups = (total_num_of_parts + num_of_parts_per_group - 1) // num_of_parts_per_group
  return total_num_of_parts, num_of_groups, num_of_parts_per_group

def get_oss_client(context, endpoint, bucket):
  creds = context.credentials
  if creds.security_token != None:
    auth = oss2.StsAuth(creds.access_key_id, creds.access_key_secret, creds.security_token)
  else:
    # for local testing, use the public endpoint
    endpoint = str.replace(endpoint, "-internal", "")
    auth = oss2.Auth(creds.access_key_id, creds.access_key_secret)
  return oss2.Bucket(auth, endpoint, bucket)