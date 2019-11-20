import flask
from app.common.httputils import httpify_excs


routes = flask.Blueprint('import-service', __name__, '/')


@routes.route('/iservice/<path:rest>', methods=["POST"])
@httpify_excs
def iservice(rest) -> flask.Response:
    from app import service  # scope this import so it's not dragged in for other functions
    """HTTP function for accepting an import request"""
    return flask.make_response(service.handle(flask.request))


@routes.route('/_ah/push-handlers/receive_messages', methods=['POST'])
def taskchunk(event, context) -> flask.Response:

    #TODO: steal from here
    # https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/appengine/standard_python37/pubsub/main.py

    from app import chunk_task
    chunk_task.handle(event["attributes"])
    return flask.make_response("ok")
