import rsa
import time
from botocore.signers import CloudFrontSigner
import json

from config import PRIVATE_KEY_PATH, KEY_PAIR_ID, CLOUDFRONT_URL_HOST_PRE, EXPIRE_SECONDS

def rsa_signer(message):
    private_key = open(PRIVATE_KEY_PATH, 'r').read()
    return rsa.sign(
        message,
        rsa.PrivateKey.load_pkcs1(private_key.encode('utf8')),
        'SHA-1')  # CloudFront requires SHA-1 hash

def getUrlSigner() :
    cf_signer = CloudFrontSigner(KEY_PAIR_ID, rsa_signer)
    expires = int(time.time() + EXPIRE_SECONDS)
    policy = {}                                                                                                                                                                           
    policy['Statement'] = [{}]                                                                                                                                                            
    policy['Statement'][0]['Resource'] = CLOUDFRONT_URL_HOST_PRE + '*'                                                                                                                                           
    policy['Statement'][0]['Condition'] = {}                                                                                                                                              
    policy['Statement'][0]['Condition']['DateLessThan'] = {}                                                                                                                              
    policy['Statement'][0]['Condition']['DateLessThan']['AWS:EpochTime'] = expires
    policy = json.dumps(policy)                                                                                                     
    signed_url = cf_signer.generate_presigned_url('', policy=policy)

    return signed_url
