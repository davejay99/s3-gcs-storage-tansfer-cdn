import json
from google.cloud import storage
from google.cloud import pubsub_v1
from google.cloud.exceptions import GoogleCloudError
import time
import pycurl
from subscriber import getUrlPubSub
import os
from multiprocessing.pool import ThreadPool
from google.cloud import storage
import uuid
import traceback
from logger import write_entry

from config import GCS_BUCKET, PUBSUB_ERROR_TOP_ID, GCP_PROJECT, MAX_MESSAGES_PER_BATCH, NUM_THREADS, STATUS_OK, VERBOSE_LOG, LOG_NAME

storage_client = storage.Client()
bucket_client = storage_client.bucket(GCS_BUCKET)

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(GCP_PROJECT, PUBSUB_ERROR_TOP_ID)

def upload_blob_disk(blob_config) :
    # initiliaze timer
    start_time = time.time()
    
    # generate random file name
    file_path = str(uuid.uuid4())

    # open file handler
    try :
        file = open(file_path, "wb+")
    except IOError as e:
        temp_dict = {
            "message": "FILE_HANDLING_ERROR",
            "error": e.strerror,
            "url": blob_config['url'],
            "object": blob_config['key'],
            "aws_bucket": blob_config['aws_bucket'],
            "gcs_bucket": GCS_BUCKET
        }
        write_entry(LOG_NAME, temp_dict, severity='ERROR')
        data = json.dumps(blob_config)
        data = data.encode("utf-8")
        future = publisher.publish(
            topic_path, data
        )
        print(future.result())
    except: 
        temp_dict = {
            "message": "FILE_HANDLING_ERROR",
            "error": traceback.format_exc(),
            "url": blob_config['url'],
            "object": blob_config['key'],
            "aws_bucket": blob_config['aws_bucket'],
            "gcs_bucket": GCS_BUCKET
        }
        write_entry(LOG_NAME, temp_dict, severity='ERROR')
        data = json.dumps(blob_config)
        data = data.encode("utf-8")
        future = publisher.publish(
            topic_path, data
        )
        print(future.result())

    # initialize pycurl
    c = pycurl.Curl()
    c.setopt(c.URL, blob_config['url'])
    c.setopt(c.WRITEDATA, file)
    # c.setopt(c.WRITEHEADER, headers)

    # perform download
    try :
        c.perform()
    except pycurl.error as e:
        temp_dict = {
            "message": "OBJECT_DOWNLOAD_ERROR",
            "error": e,
            "url": blob_config['url'],
            "object": blob_config['key'],
            "aws_bucket": blob_config['aws_bucket'],
            "gcs_bucket": GCS_BUCKET
        }
        write_entry(LOG_NAME, temp_dict, severity='ERROR')
        data = json.dumps(blob_config)
        data = data.encode("utf-8")
        future = publisher.publish(
            topic_path, data
        )
        print(future.result())

    file.close()

    if c.errstr() == '' :
        res_code = c.getinfo(pycurl.RESPONSE_CODE)
        if res_code in STATUS_OK :
            c.close()
            
            # # log when object is download
            # temp_dict = {
            #     "message": "OBJECT_DOWNLOADED",
            #     "object": blob_config['key'],
            #     "aws_bucket": blob_config['aws_bucket'],
            #     "gcs_bucket": GCS_BUCKET
            # }
            # write_entry(LOG_NAME, temp_dict, severity='INFO')

            # intiliaze object handler and configure metadata
            blob = bucket_client.blob(blob_config['key'])
            if 'user_metadata' in blob_config :
                blob.metadata = blob_config['user_metadata']
            if 'content_type' in blob_config :
                blob.content_type = blob_config['content_type']
            if 'content_encoding' in blob_config :
                blob.content_encoding = blob_config['content_encoding']
            if 'content_disposition' in blob_config :
                blob.content_disposition = blob_config['content_disposition']
            if 'cache_control' in blob_config :
                blob.cache_control = blob_config['cache_control']
            if 'content_language' in blob_config :
                blob.content_language = blob_config['content_language'] 
            blob.storage_class = blob_config['storage_class']

            # upload object
            try :
                blob.upload_from_filename(file_path)
            except GoogleCloudError as e :
                temp_dict = {
                    "message": "OBJECT_UPLOAD_ERROR",
                    "error": e.message,
                    "url": blob_config['url'],
                    "object": blob_config['key'],
                    "aws_bucket": blob_config['aws_bucket'],
                    "gcs_bucket": GCS_BUCKET
                }
                write_entry(LOG_NAME, temp_dict, severity='ERROR')
                data = json.dumps(blob_config)
                data = data.encode("utf-8")
                future = publisher.publish(
                    topic_path, data
                )
                print(future.result())

            # # log when object is uploaded
            # temp_dict = {
            #     "message": "OBJECT_UPLOADED",
            #     "object": blob_config['key'],
            #     "aws_bucket": blob_config['aws_bucket'],
            #     "gcs_bucket": GCS_BUCKET
            # }
            # write_entry(LOG_NAME, temp_dict, severity='INFO')
        
        else :
            temp_dict = {
                "message": "OBJECT_DOWNLOAD_ERROR",
                "error": "Received " + str(res_code) + " code." ,
                "url": blob_config['url'],
                "object": blob_config['key'],
                "aws_bucket": blob_config['aws_bucket'],
                "gcs_bucket": GCS_BUCKET
            }
            write_entry(LOG_NAME, temp_dict, severity='ERROR')
            data = json.dumps(blob_config)
            data = data.encode("utf-8")
            future = publisher.publish(
                topic_path, data
            )
            print(future.result())
    
    else :
        temp_dict = {
            "message": "OBJECT_DOWNLOAD_ERROR",
            "error": c.errstr(),
            "url": blob_config['url'],
            "object": blob_config['key'],
            "aws_bucket": blob_config['aws_bucket'],
            "gcs_bucket": GCS_BUCKET
        }
        write_entry(LOG_NAME, temp_dict, severity='ERROR')
        data = json.dumps(blob_config)
        data = data.encode("utf-8")
        future = publisher.publish(
            topic_path, data
        )
        print(future.result())

    # remove downloaded file
    os.remove(file_path)

    # end timer and print
    # print("--- %.3f seconds ---" % (time.time() - start_time) + ' ' + blob_config['key'])


def thread_pool (i) :
    while True :
        # get object config list
        json_list = getUrlPubSub(NUM_MESSAGES=MAX_MESSAGES_PER_BATCH)
        if json_list == "NO_MESSAGES" :
            print("queue empty..")
            time.sleep(5)
            continue
        for blob_list in json_list :
            for blob_config in blob_list :
                # log when object is initialized
                temp_dict = {
                    "message": "OBJECT_INITIATED",
                    "object": blob_config['key'],
                    "aws_bucket": blob_config['aws_bucket'],
                    "gcs_bucket": GCS_BUCKET
                }
                print(temp_dict)
                if VERBOSE_LOG:
                    write_entry(LOG_NAME, temp_dict, severity='INFO')
                upload_blob_disk(blob_config=blob_config)


def main (NUM_THREADS = NUM_THREADS) :
    # initiliaze transfer data path
    path = os.getcwd() + "/transfer_data"
    orig_dir = os.getcwd()
    if not os.path.exists(path) :
        os.mkdir(path)
    os.chdir(path)

    # initialize threadpool
    p = ThreadPool(NUM_THREADS)
    p.map(thread_pool, range(NUM_THREADS))
    p.close()
    p.join()
    os.chdir(orig_dir)


if __name__ == '__main__':
    try :
        main()
    except :
        temp_dict = {
            "message": "MAIN_PROGRAM_ERROR",
            "program": "WORKER",
            "error": traceback.format_exc(),
            "gcp_bucket": GCS_BUCKET
        }
        write_entry(LOG_NAME, temp_dict, severity='ERROR')