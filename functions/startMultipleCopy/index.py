# -*- coding: utf-8 -*-
import json
import os
import logging
import re

from aliyunsdkcore import client
from aliyunsdkcore.auth.credentials import StsTokenCredential
from aliyunsdkcore.acs_exception.exceptions import ServerException
from aliyunsdkfnf.request.v20190315 import StartExecutionRequest


def handler(event, context):
  logger = logging.getLogger()
  evt = json.loads(event)
  logger.info("Handling event: %s", evt)

  creds = context.credentials
  sts_token_credential = StsTokenCredential(creds.access_key_id, creds.access_key_secret, creds.security_token)
  fnf_client = client.AcsClient(region_id=context.region, credential=sts_token_credential)

  request = StartExecutionRequest.StartExecutionRequest()
  request.set_FlowName(os.environ["FLOW_NAME"])

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
  # TODO: swallow ExecutionAlreadyExists error
  return fnf_client.do_action_with_exception(request)
