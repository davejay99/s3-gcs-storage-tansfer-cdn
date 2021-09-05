import boto3
import config
from get_signed_policy import getUrlSigner

from config import ENABLE_URL_SIGNING

if ENABLE_URL_SIGNING:
    url_signer = getUrlSigner()

def list_s3_objects(**kwargs):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=config.AWS_KEY_ID,
        aws_secret_access_key=config.AWS_KEY_SECRET,
        region_name=config.AWS_REGION,
    )
    # s3_acl = boto3.resource(
    #     "s3",
    #     aws_access_key_id=config.AWS_KEY_ID,
    #     aws_secret_access_key=config.AWS_KEY_SECRET,
    #     region_name=config.AWS_REGION,
    # )
    prefix = kwargs.get("prefix", "")
    list_objects = s3.list_objects_v2(Bucket=config.AWS_S3_BUCKET, Prefix=prefix)
    next_token = list_objects.get("NextContinuationToken")
    total_size = 0
    for content in list_objects["Contents"]:
        if content["Size"] <= 0:
            continue
        # filter code
        metadata = s3.head_object(Bucket=config.AWS_S3_BUCKET, Key=content["Key"])
        temp_dict = {}
        if content["StorageClass"] == 'STANDARD' :
            if metadata['Metadata'] != {} :
                temp_dict['user_metadata'] = metadata['Metadata']
            if 'ContentType' in metadata :
                temp_dict['content_type'] = metadata['ContentType']
            if 'ContentEncoding' in metadata :
                temp_dict['content_encoding'] = metadata['ContentEncoding']
            if 'ContentDisposition' in metadata :
                temp_dict['content_disposition'] = metadata['ContentDisposition']
            if 'CacheControl' in metadata :
                temp_dict['cache_control'] = metadata['CacheControl']
            if 'ContentLanguage' in metadata :
                temp_dict['content_language'] = metadata['ContentLanguage']
            temp_dict['aws_bucket'] = config.AWS_S3_BUCKET
            temp_dict['storage_class'] = 'STANDARD'
            temp_dict['key'] = content["Key"]
            temp_dict['url'] = config.CLOUDFRONT_URL_HOST_PRE + content["Key"]
            if ENABLE_URL_SIGNING:
                temp_dict['url'] = temp_dict['url'] + url_signer
            # acl = s3_acl.ObjectAcl(config.AWS_S3_BUCKET, content["Key"]).grants
            # ans = False
            # for grantee in acl :
            #     if 'Grantee' in grantee :
            #         if grantee['Grantee']['URI'] == 'http://acs.amazonaws.com/groups/global/AllUsers' :
            #             ans = True
            #             break
            # if not ans :
            #     temp_dict['url'] = getPreSignedUrl(temp_dict['url'])
            temp_dict['url'] = temp_dict['url'] + config.CLOUDFRONT_URL_HOST_POST
            total_size += content["Size"]
            yield temp_dict
    while next_token is not None:
        list_objects = s3.list_objects_v2(
            Bucket=config.AWS_S3_BUCKET, Prefix=prefix, ContinuationToken=next_token
        )
        next_token = list_objects.get("NextContinuationToken")
        for content in list_objects["Contents"]:
            if content["Size"] <= 0:
                continue
            # filter code
            metadata = s3.head_object(Bucket=config.AWS_S3_BUCKET, Key=content["Key"])
            temp_dict = {}
            if content["StorageClass"] == 'STANDARD' :
                if metadata['Metadata'] != {} :
                    temp_dict['user_metadata'] = metadata['Metadata']
                if 'ContentType' in metadata :
                    temp_dict['content_type'] = metadata['ContentType']
                if 'ContentType' in metadata :
                    temp_dict['content_encoding'] = metadata['ContentEncoding']
                if 'ContentType' in metadata :
                    temp_dict['content_disposition'] = metadata['ContentDisposition']
                if 'ContentType' in metadata :
                    temp_dict['cache_control'] = metadata['CacheControl']
                if 'ContentType' in metadata :
                    temp_dict['content_language'] = metadata['ContentLanguage']
                temp_dict['aws_bucket'] = config.AWS_S3_BUCKET
                temp_dict['storage_class'] = 'STANDARD'
                temp_dict['key'] = content["Key"]
                temp_dict['url'] = config.CLOUDFRONT_URL_HOST_PRE + content["Key"]
                if ENABLE_URL_SIGNING:
                    temp_dict['url'] = url_signer
                # acl = s3_acl.ObjectAcl(config.AWS_S3_BUCKET, content["Key"]).grants
                # ans = False
                # for grantee in acl :
                #     if 'Grantee' in grantee :
                #         if grantee['Grantee']['URI'] == 'http://acs.amazonaws.com/groups/global/AllUsers' :
                #             ans = True
                #             break
                # if not ans :
                #     temp_dict['url'] = getPreSignedUrl(temp_dict['url'])
                temp_dict['url'] = temp_dict['url'] + config.CLOUDFRONT_URL_HOST_POST
                total_size += content["Size"]
                yield temp_dict
    
    print(f"Total files size: {total_size}")