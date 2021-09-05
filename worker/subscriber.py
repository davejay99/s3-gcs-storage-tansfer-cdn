from google.api_core import retry
from google.cloud import pubsub_v1
import json

from config import GCP_PROJECT, PUBSUB_SUBSCRIPTION_ID

def getUrlPubSub (NUM_MESSAGES=1) :
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(GCP_PROJECT, PUBSUB_SUBSCRIPTION_ID)
    json_message = []
    with subscriber :
        response = subscriber.pull(
            request={"subscription": subscription_path, "max_messages": NUM_MESSAGES},
            retry=retry.Retry(deadline=300),
        )
        if not response.received_messages:
            return "NO_MESSAGES"
        ack_ids = []
        for received_message in response.received_messages:
            # print(f"Received: ", received_message.message.data.decode())
            json_message.append(json.loads(received_message.message.data.decode()))
            ack_ids.append(received_message.ack_id)
        # Acknowledges the received messages so they will not be sent again.
        subscriber.acknowledge(
            request={"subscription": subscription_path, "ack_ids": ack_ids}
        )
    
    return json_message
        