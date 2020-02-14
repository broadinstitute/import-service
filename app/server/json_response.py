import flask


def fixup_mimetype(resp: flask.Response):
    if flask.request.path.startswith("/apidocs"):
        resp.mimetype = "text/html"
    else:
        resp.mimetype = "application/json"
    return resp
