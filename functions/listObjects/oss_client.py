import os
import json
import oss2


from aliyunsdkcore import client
from aliyunsdkcore.auth.credentials import StsTokenCredential, AccessKeyCredential
from aliyunsdkcore.acs_exception.exceptions import ClientException, ServerException
from aliyunsdksts.request.v20150401 import AssumeRoleRequest

def get_oss_client(context, endpoint, bucket, access_role=None):
  creds = context.credentials
  key_id, key_secret, token = creds.access_key_id, creds.access_key_secret, creds.security_token
  local = bool(os.getenv('local', ""))
  if access_role:
    req = AssumeRoleRequest.AssumeRoleRequest()
    req.set_accept_format('json')
    req.set_RoleArn(access_role)
    req.set_RoleSessionName('oss-copy')

    if local:
      acs_creds1 = AccessKeyCredential(creds.access_key_id, creds.access_key_secret)
    else:
      acs_creds1 = StsTokenCredential(creds.access_key_id, creds.access_key_secret, creds.security_token)
      # Since the function instance runs within VPC (FC system), the endpoint has to be either public or vpc endpoint.
      # Here the VPC endpoint is used because it's more secure, latency is low and no public network usage is incurred.
      req.set_endpoint('sts-vpc.%s.aliyuncs.com' % (context.region))
    # Create clt1 with temp credentials provided by fc context
    clt = client.AcsClient(region_id=context.region, credential=acs_creds1)
    body1 = clt.do_action(req)
    ar_resp1 = json.loads(body1)
    # Now we get another temp credentials
    tmp_creds2 = ar_resp1['Credentials']
    key_id, key_secret, token = tmp_creds2['AccessKeyId'], tmp_creds2['AccessKeySecret'], tmp_creds2['SecurityToken']
    auth = oss2.StsAuth(key_id, key_secret, token)
  else:
    if local:
      auth = oss2.Auth(key_id, key_secret)
    else:
      auth = oss2.StsAuth(key_id, key_secret, token)
        # for local testing, use the public endpoint
  endpoint = str.replace(endpoint, "-internal", "") if local else endpoint
  return oss2.Bucket(auth, endpoint, bucket)