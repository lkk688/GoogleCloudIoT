#ref: https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/pubsub/cloud-client/quickstart/sub.py

#usage: python sub.py $PROJECT my-subscription
import argparse
from google.cloud import pubsub_v1

def sub(project_id, subscription_name):
    """Receives messages from a Pub/Sub subscription."""
    # [START pubsub_quickstart_sub_client]
    # Initialize a Subscriber client
    subscriber_client = pubsub_v1.SubscriberClient()
    # [END pubsub_quickstart_sub_client]
    # Create a fully qualified identifier in the form of
    # `projects/{project_id}/subscriptions/{subscription_name}`
    subscription_path = subscriber_client.subscription_path(
        project_id, subscription_name
    )

    def callback(message):
        print(
            "Received message {} of message ID {}\n".format(
                message, message.message_id
            )
        )
        # Acknowledge the message. Unack'ed messages will be redelivered.
        message.ack()
        print("Acknowledged message {}\n".format(message.message_id))

    streaming_pull_future = subscriber_client.subscribe(
        subscription_path, callback=callback
    )
    print("Listening for messages on {}..\n".format(subscription_path))

    try:
        # Calling result() on StreamingPullFuture keeps the main thread from
        # exiting while messages get processed in the callbacks.
        streaming_pull_future.result()
    except:  # noqa
        streaming_pull_future.cancel()

    subscriber_client.close()

#usage: python3 sub.py cmpelkk cmpe181dev1-subscription
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("project_id", help="Google Cloud project ID")
    parser.add_argument("subscription_name", help="Pub/Sub subscription name")

    args = parser.parse_args()

    sub(args.project_id, args.subscription_name)