import flask

from functions.common.httputils import httpify_excs

# Stackdriver logging picks up native Python logs, but ignores the formatting and only shows the message.
# Being able to use this formatter would save us some typing. But since it doesn't work on GCF, I've left it
# commented out for now.
# I've got a StackOverflow question out about this: https://stackoverflow.com/q/58955720/2941784
#
# import logging
# logging.basicConfig(format="%(module)s.%(funcName)s: %(message)s", level=logging.INFO)

@httpify_excs
def iservice(request: flask.Request) -> flask.Response:
    from functions import service  # scope this import so it's not dragged in for other functions
    """HTTP function for accepting an import request"""
    return flask.make_response(service.handle(request, "/iservice"))


# Keep this updated this with a list of all HTTP cloud functions, it's used to build the unit testing client
ALL_HTTP_FUNCTIONS = [iservice]


def taskchunk(event, context) -> None:
    from functions import chunk_task
    chunk_task.handle(event["attributes"])
    return None  # background functions want you to return something
