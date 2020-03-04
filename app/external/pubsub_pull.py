import flask
import logging
import traceback
import time

from app.external import pubsub
from app.server import routes
from app.server.requestutils import pubsubify_excs, PUBSUB_STATUS_RETRY


@pubsubify_excs
def process_one(attributes: dict) -> flask.Response:
    return routes.pubsub_dispatch(attributes)


def loop(app: flask.Flask):
    """Main loop for pulling messages from PubSub when in PULL_MODE."""
    while True:
        try:
            poll(app)
        except Exception:
            # Catch-all for nasty surprises. If this thread dies, we stop polling.
            logging.error(f"Exception in pubsub_pull thread:\n{traceback.format_exc()}")
        time.sleep(5)


def poll(app: flask.Flask):
    maybe_message = pubsub.pull_self(1)  # this is a list with 0 or 1 elems
    for msg in maybe_message:
        logging.info(f"Got message from pubsub: \n{msg}")
        # call out to process_one to let @pubsubify_excs catch application exceptions.
        # this means any other exception thrown in this thread will be from pubsub machinery.
        # we need to provide a Flask app context so it doesn't complain when we call make_response
        with app.app_context():
            app_response = process_one(msg.message.attributes)
        if getattr(app_response, 'status_code', 200) != PUBSUB_STATUS_RETRY:
            # if app code throws an exception, pubsubify_excs will catch it and turn it into a Flask response.
            # if the status code on that response is PUBSUB_STATUS_RETRY, don't ack the message so pubsub retries it.
            pubsub.acknowledge_self_messages([msg.ack_id])
