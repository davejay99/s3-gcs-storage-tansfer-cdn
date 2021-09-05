"""Publishes multiple messages to a Pub/Sub topic with an error handler."""
from google.cloud import pubsub_v1
from list_objects import list_s3_objects
from concurrent import futures
import json
from logger import write_entry
import traceback

from config import GCP_PROJECT, PUBSUB_TOP_ID, MAX_OBJECTS_PER_MESSAGE, MAX_MESSAGES_PER_BATCH, VERBOSE_LOG, LOG_NAME

def publisher (NUM_SINGLE_BATCH=MAX_OBJECTS_PER_MESSAGE) :
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(GCP_PROJECT, PUBSUB_TOP_ID)

    single_batch = []
    for temp_dict in list_s3_objects():
        single_batch.append(temp_dict)
        if len(single_batch) >= NUM_SINGLE_BATCH :
            data = json.dumps(single_batch)
            data = data.encode("utf-8")
            future = publisher.publish(
                topic_path, data
            )
            single_batch = []
            print(future.result())
        
    if len(single_batch) > 0:
        data = json.dumps(single_batch)
        data = data.encode("utf-8")
        future = publisher.publish(
            topic_path, data
        )
        print(future.result())

def batchPublisher (NUM_SINGLE_BATCH=MAX_OBJECTS_PER_MESSAGE) :
    # Configure the batch to publish as soon as there are 10 messages
    # or 1 KiB of data, or 1 second has passed.
    batch_settings = pubsub_v1.types.BatchSettings(
        max_messages=MAX_MESSAGES_PER_BATCH,  # default 100
    )
    publisher = pubsub_v1.PublisherClient(batch_settings)
    topic_path = publisher.topic_path(GCP_PROJECT, PUBSUB_TOP_ID)
    publish_futures = []

    # Resolve the publish future in a separate thread.
    def callback(future: pubsub_v1.publisher.futures.Future) -> None:
        message_id = future.result()
        print(message_id)

    single_batch = []
    for temp_dict in list_s3_objects():
        single_batch.append(temp_dict)
        if len(single_batch) >= NUM_SINGLE_BATCH :
            data = json.dumps(single_batch)
            data = data.encode("utf-8")
            publish_future = publisher.publish(topic_path, data)
            # Non-blocking. Allow the publisher client to batch multiple messages.
            publish_future.add_done_callback(callback)
            publish_futures.append(publish_future)
            temp_dict = {
                "message": "PUBSUB_MESSAGE_PUBLISHED",
                "program": "PUBLISHER",
                "message_list": json.dumps(single_batch)
            }
            print(temp_dict)
            if VERBOSE_LOG:
                write_entry(LOG_NAME, temp_dict, severity='INFO')
            single_batch = []
    
    if len(single_batch) > 0:
        data = json.dumps(single_batch)
        data = data.encode("utf-8")
        publish_future = publisher.publish(topic_path, data)
        # Non-blocking. Allow the publisher client to batch multiple messages.
        publish_future.add_done_callback(callback)
        publish_futures.append(publish_future)
        temp_dict = {
            "message": "PUBSUB_MESSAGE_PUBLISHED",
            "program": "PUBLISHER",
            "message_list": json.dumps(single_batch)
        }
        print(temp_dict)
        if VERBOSE_LOG:
            write_entry(LOG_NAME, temp_dict, severity='INFO')

    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

    print(f"Published messages with batch settings to {topic_path}.")


if __name__ == '__main__':
    try :
        batchPublisher()
    except :
        temp_dict = {
            "message": "MAIN_PROGRAM_ERROR",
            "program": "PUBLISHER",
            "error": traceback.format_exc()
        }
        write_entry(LOG_NAME, temp_dict, severity='ERROR')