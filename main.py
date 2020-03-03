import logging, os, sys
from app import create_app


# Google's suggested integration with Stackdriver logging produces duplicate logs in Stackdriver.
# This does it right. See https://stackoverflow.com/a/58655297/2941784
if "GAE_APPLICATION" in os.environ:
    from google.cloud.logging.handlers import AppEngineHandler
    import google.cloud.logging as glogging
    client = glogging.Client()
    formatter = logging.Formatter("%(module)s.%(funcName)s: %(message)s")
    handler = AppEngineHandler(client, stream=sys.stderr)
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(logging.INFO)
else:
    # For local runs, use normal Python logging (so we don't send all our test logs to Stackdriver!)
    logging.basicConfig(format="%(module)s.%(funcName)s: %(message)s", level=logging.INFO)


app = create_app()

# FiaB instances of this service live inside the Broad network and thus PubSub can't push notifications to the REST
# handler. Setting PULL_PUBSUB will disable the REST handler and spin up a thread that pulls messages from PubSub instead.
pull_pubsub = os.environ.get("PULL_PUBSUB", False)
if pull_pubsub.lower() == "true":
    import threading
    from app.external import pubsub, pubsub_pull
    pubsub.create_topic_and_sub()
    pubsub_pull_thread = threading.Thread(target=pubsub_pull.loop, args=(app,))
    pubsub_pull_thread.start()
