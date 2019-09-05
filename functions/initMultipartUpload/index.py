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
#   "total_size": number
# }

def handler(event, context):
  logger = logging.getLogger()
  evt = json.loads(event)
  logger.info("Handling event: %s", evt)
  src_client = get_oss_client(context, os.environ['SRC_OSS_ENDPOINT'], evt["src_bucket"])
  dest_client = get_oss_client(context, evt.get("dest_oss_endpoint") or os.environ['DEST_OSS_ENDPOINT'], evt["dest_bucket"])
  
  # Decide the number of parts
  part_size = determine_part_size(evt["total_size"], preferred_size=evt["part_size"])
  num_of_parts = (evt["total_size"] + part_size - 1) // part_size

  upload_id = dest_client.init_multipart_upload(evt["key"]).upload_id

  return {
      "upload_id": upload_id,
      "part_no_list": list(range(1,num_of_parts+1))
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