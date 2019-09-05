# -*- coding: utf-8 -*-
import logging
import os
import oss2
from oss2.models import PartInfo
import json


# event format
# {
#   "dest_bucket": "",
#   "key": "",
#   "upload_id": "",
#   "parts":[
#     {"part_no": 1, "etag": ""},
#     {"part_no": 2, "etag": ""}
#   ]
# }

def handler(event, context):
  logger = logging.getLogger()
  evt = json.loads(event)
  logger.info("Handling event: %s", evt)
  dest_client = get_oss_client(context, evt.get("dest_oss_endpoint") or os.environ['DEST_OSS_ENDPOINT'], evt["dest_bucket"])

  parts = []
  for part in evt["parts"]:
    parts.append(PartInfo(part["part_no"], part["etag"]))

  dest_client.complete_multipart_upload(evt["key"], evt["upload_id"], parts)

  return {}

def get_oss_client(context, endpoint, bucket):
  creds = context.credentials
  if creds.security_token != None:
    auth = oss2.StsAuth(creds.access_key_id, creds.access_key_secret, creds.security_token)
  else:
    # for local testing, use the public endpoint
    endpoint = str.replace(endpoint, "-internal", "")
    auth = oss2.Auth(creds.access_key_id, creds.access_key_secret)
  return oss2.Bucket(auth, endpoint, bucket)