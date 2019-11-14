# -*- coding: utf-8 -*-
import json
import os
import logging
import re

from aliyunsdkcore import client
from aliyunsdkcore.auth.credentials import StsTokenCredential, AccessKeyCredential
from aliyunsdkcore.acs_exception.exceptions import ClientException, ServerException
from aliyunsdkfnf.request.v20190315 import StartExecutionRequest


def handler(event, context):
  logger = logging.getLogger()
  evt = json.loads(event)
  logger.info("Handling event: %s", evt)

  creds = context.credentials
  local = bool(os.getenv('local', ""))

  if (local):
    acs_creds = AccessKeyCredential(creds.access_key_id, creds.access_key_secret)
    endpoint = "%s.fnf.aliyuncs.com" % context.region
  else:
    acs_creds = StsTokenCredential(creds.access_key_id, creds.access_key_secret, creds.security_token)
    endpoint = "%s-internal.fnf.aliyuncs.com" % context.region

  fnf_client = client.AcsClient(region_id=context.region, credential=acs_creds)

  request = StartExecutionRequest.StartExecutionRequest()
  request.set_FlowName(os.environ["FLOW_NAME"])
  request.set_endpoint(endpoint)

  input = {
    "src_bucket": evt["src_bucket"],
    "dest_bucket": evt["dest_bucket"],
    "prefix": evt["prefix"],
    "marker": evt["marker"]
  }
  request.set_Input(json.dumps(input))
  execution_name = re.sub(r"[^a-zA-Z0-9-_]", "_", evt["marker"])
  request.set_ExecutionName(execution_name)

  logger.info("Starting flow execution: %s", execution_name)
  try:
    resp = fnf_client.do_action_with_exception(request)
    return resp
  except ServerException as e:
    # https://help.aliyun.com/document_detail/122628.html
    if e.get_error_code() == 'ExecutionAlreadyExists':
      logger.warn("Execution %s already exists", execution_name)
      return {}
    else:
      logger.error("Failed to call fnf due to server exception: %s", e)
      raise e
