from google.cloud import pubsub_v1
import os
from typing import Dict, List, Optional

_publisher_client: Optional[pubsub_v1.PublisherClient] = None
_subscriber_client: Optional[pubsub_v1.SubscriberClient] = None


def _get_publisher_client() -> pubsub_v1.PublisherClient:
    global _publisher_client
    if _publisher_client is None:
        _publisher_client = pubsub_v1.PublisherClient()
    return _publisher_client


def publish_self(data: Dict[str, str]) -> None:
    """Publish the data (as attributes, not in the message body) to ourselves using pub/sub."""
    client = _get_publisher_client()
    topic_path = client.topic_path(os.environ.get("PUBSUB_PROJECT"), os.environ.get("PUBSUB_TOPIC"))
    future = client.publish(topic_path, b'', **data)
    future.result()  # wait on the future so we know it's done


def publish_rawls(data: Dict[str, str]) -> None:
    """Publish the data (as attributes, not in the message body) to Rawls using pub/sub."""
    client = _get_publisher_client()
    topic_path = client.topic_path(os.environ.get("RAWLS_PUBSUB_PROJECT"), os.environ.get("RAWLS_PUBSUB_TOPIC"))
    future = client.publish(topic_path, b'', **data)
    future.result()  # wait on the future so we know it's done


def _get_subscriber_client() -> pubsub_v1.SubscriberClient:
    global _subscriber_client
    if _subscriber_client is None:
        _subscriber_client = pubsub_v1.SubscriberClient()
    return _subscriber_client


def pull_self(num_messages: int):
    client = _get_subscriber_client
    subscription_path = client.subscription_path(os.environ.get("PUBSUB_PROJECT"), os.environ.get("PUBSUB_SUBSCRIPTION"))
    response = client.pull(subscription_path, max_messages=num_messages)
    return response.received_messages


def acknowledge_self_messages(ack_ids: List[int]):
    client = _get_subscriber_client
    subscription_path = client.subscription_path(os.environ.get("PUBSUB_PROJECT"), os.environ.get("PUBSUB_SUBSCRIPTION"))
    _subscriber_client.acknowledge(ack_ids)