from google.cloud import pubsub_v1
import os
from typing import Dict, Optional

_client: Optional[pubsub_v1.PublisherClient] = None


def _get_client() -> pubsub_v1.PublisherClient:
    global _client
    if _client is None:
        _client = pubsub_v1.PublisherClient()
    return _client


def publish(data: Dict[str, str]) -> None:
    """Publish the data (as attributes, not in the message body) to ourselves using pub/sub."""
    client = _get_client()
    topic_path = client.topic_path(os.environ.get("PUBSUB_PROJECT"), os.environ.get("PUBSUB_TOPIC"))
    future = client.publish(topic_path, b'', **data)
    future.result()  # wait on the future so we know it's done
