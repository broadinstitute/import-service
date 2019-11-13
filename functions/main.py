import flask
import service


def iservice(request: flask.Request) -> flask.Response:
    return flask.make_response(service.handle(request))


# Update this with a list of all HTTP cloud functions, it's used to build the unit testing client
ALL_HTTP_FUNCTIONS = [iservice]

