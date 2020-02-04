import pytest

from app.db import model, session_ctx
from app.util import exceptions
from app.server.routes import routes
from app.server.requestutils import *

import flask
import flask.testing


def test_pubsubify_excs(fake_import: model.Import, client_with_modifiable_routes: flask.testing.FlaskClient):
    client = client_with_modifiable_routes
    # pre-populate an import that will get error'd
    with session_ctx() as sess:
        new_import = fake_import
        sess.add(new_import)
        sess.commit()

    @pubsubify_excs
    def ise_exc() -> flask.Response:
        raise exceptions.ISvcException("a bad happened", imports=[new_import])

    client.application.add_url_rule('/test_pubsubify_excs', view_func=ise_exc, methods=["GET"])

    resp = client.get('/test_pubsubify_excs')
    assert resp.status_code == 202

    with session_ctx() as sess:
        recovered_import: Import = Import.reacquire(new_import.id, sess)
        assert recovered_import.status == model.ImportStatus.Error
        assert recovered_import.error_message == "a bad happened"
