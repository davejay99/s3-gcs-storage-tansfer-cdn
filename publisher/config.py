GCP_PROJECT             = 'GCP_PROJECT_ID'
PUBSUB_TOP_ID           = 'PUBSUB_TOP_ID_WHERE_OBJECT_LIST_IS_PUSHED'

AWS_KEY_ID                  = 'AWS_KEY_ID'
AWS_KEY_SECRET              = 'AWS_KEY_SECRET'
AWS_REGION                  = 'AWS_REGION'
AWS_S3_BUCKET               = 'AWS_S3_BUCKET'
CLOUDFRONT_URL_HOST_PRE     = 'CLOUDFRONT_URL'
CLOUDFRONT_URL_HOST_POST    =  ''

MAX_MESSAGES_PER_BATCH  = 100
MAX_OBJECTS_PER_MESSAGE = 50

ENABLE_URL_SIGNING = True               # set to True if using signed url
EXPIRE_SECONDS = 3600*24*7              # expiration period for the signed url
PRIVATE_KEY_PATH = 'private_key.pem'    # path to the private key to sign the url policy
KEY_PAIR_ID = 'KEY_ID'                  # public key id used for cloudfront signed url

VERBOSE_LOG = True
LOG_NAME    = 'storage_tansfer'