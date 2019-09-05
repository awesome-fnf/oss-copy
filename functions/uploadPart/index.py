# -*- coding: utf-8 -*-
import logging
import os
import oss2
import json

# event format
# {
#   "bucket": "",
#   "dest_bucket": "",
#   "key": "",
#   "upload_id": "",
#   "part_no": 1,
#   "part_size": 1024,
#   "total_size": 1025
# }

def handler(event, context):
  logger = logging.getLogger()
  evt = json.loads(event)
  logger.info("Handling event: %s", evt)
  src_client = get_oss_client(context, os.environ['SRC_OSS_ENDPOINT'], evt["src_bucket"])
  dest_client = get_oss_client(context, evt.get("dest_oss_endpoint") or os.environ['DEST_OSS_ENDPOINT'], evt["dest_bucket"])

  # Download part of the file and upload as part
  part_no = evt["part_no"]
  part_size = evt["part_size"]
  byte_range = ((part_no-1)*part_size, min(part_no*part_size, evt["total_size"])-1)
  # src_client.get_object_to_file(evt["key"], "/tmp/testpart%d" % part_no, byte_range=byte_range)
  object_stream = src_client.get_object(evt["key"], byte_range=byte_range)
  res = dest_client.upload_part(evt["key"], evt["upload_id"], part_no, object_stream)
  return {"part_no": part_no, "etag": res.etag}


def get_oss_client(context, endpoint, bucket):
  creds = context.credentials
  if creds.security_token != None:
    auth = oss2.StsAuth(creds.access_key_id, creds.access_key_secret, creds.security_token)
  else:
    # for local testing, use the public endpoint
    endpoint = str.replace(endpoint, "-internal", "")
    auth = oss2.Auth(creds.access_key_id, creds.access_key_secret)
  return oss2.Bucket(auth, endpoint, bucket)