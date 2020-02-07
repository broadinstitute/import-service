import copy
import flask
import pytest
import app.server.routes
from app.tests import testutils
from typing import Dict


@pytest.mark.usefixtures("incoming_valid_pubsub")
def test_unpack_camelcase(fake_import, client, monkeypatch):
    """Make sure that camelCase -> snake_case conversion in pubsub message keys works okay."""
    def fake_endpoint(msg: Dict[str, str]) -> flask.Response:
        return flask.make_response(msg["should_be_snake"])

    new_dispatcher = copy.deepcopy(app.server.routes.pubsub_dispatch)
    new_dispatcher.update({"cameltest": fake_endpoint})
    monkeypatch.setattr(app.server.routes, 'pubsub_dispatch', new_dispatcher)

    # conversion should work
    resp = client.post("/_ah/push-handlers/receive_messages",
                       json=testutils.pubsub_json_body({"action": "cameltest", "shouldBeSnake": "ssssss"}))
    assert resp.status_code == 200

    # conversion should work if there's nothing to convert, either
    resp2 = client.post("/_ah/push-handlers/receive_messages",
                       json=testutils.pubsub_json_body({"action": "cameltest", "should_be_snake": "ssssss"}))
    assert resp2.status_code == 200
