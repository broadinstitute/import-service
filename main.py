import logging, os
from app import create_app
import google.cloud.logging

if "GAE_APPLICATION" in os.environ:
    client = google.cloud.logging.Client()
    client.setup_logging()
else:
    # For local runs, use normal Python logging (so we don't send all our test logs to Stackdriver!)
    logging.basicConfig(format="%(module)s.%(funcName)s: %(message)s", level=logging.INFO)

app = create_app()

# FiaB instances of this service live inside the Broad network and thus PubSub can't push notifications to the REST
# handler. Setting PULL_PUBSUB will spin up a thread that pulls messages from PubSub instead.
pull_pubsub = os.environ.get("PULL_PUBSUB", "False")
if pull_pubsub.lower() == "true":
    import threading
    from app.external import pubsub, pubsub_pull
    pubsub.create_topic_and_sub()
    pubsub_pull_thread = threading.Thread(target=pubsub_pull.loop, args=(app,))
    pubsub_pull_thread.start()
