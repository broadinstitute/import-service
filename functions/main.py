import flask
import service


def iservice(request: flask.Request) -> flask.Response:
    return flask.make_response(service.handle(request))


def flaskapp() -> flask.Flask:
    app = flask.Flask(__name__)
    @app.route('/test', methods=['POST'])
    def test():
        return iservice(flask.request)
    return app


if __name__ == "__main__":
    c = flaskapp().test_client()
    resp = c.post('/test', json={"bees":"buzz"})
    print(f"HTTP {resp.status} message {resp.data}")
