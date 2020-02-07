import flask.testing


def test_gzip(client_with_modifiable_routes: flask.testing.FlaskClient):
    client = client_with_modifiable_routes

    def gzip_me() -> flask.Response:
        # default min size for gzip compression is 500B
        return flask.make_response(flask.jsonify({"foo": "a"*500}))

    client.application.add_url_rule("/gzip_test", view_func=gzip_me, methods=["GET"])

    resp = client.get("/gzip_test", headers = {"Accept-Encoding": "gzip"})
    assert resp.headers["Content-Encoding"] == "gzip"
