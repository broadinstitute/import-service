import os
from app import create_app
import google.cloud.logging

client = google.cloud.logging.Client()
client.setup_logging()

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
