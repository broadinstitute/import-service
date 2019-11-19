import flask


def iservice(request: flask.Request) -> flask.Response:
    from functions import service  # scope this import so it's not dragged in for other functions
    """HTTP function for accepting an import request"""
    return flask.make_response(service.handle(request))


# Keep this updated this with a list of all HTTP cloud functions, it's used to build the unit testing client
ALL_HTTP_FUNCTIONS = [iservice]


def taskchunk(event, context) -> None:
    from functions import chunk_task
    chunk_task.handle(event["attributes"])
    return None  # background functions want you to return something
