import pytest

from app.db import model, session_ctx
from app.util import exceptions
from app.server.routes import routes
from app.server.requestutils import *
from app.server.requestutils import _part_to_regex, _pattern_to_regex

import flask
import flask.testing


def test_part_to_regex():
    assert _part_to_regex("foo") == "foo"
    assert _part_to_regex("<boo>") == r"(?P<boo>[\w-]+)"


def test_pattern_to_regex():
    assert _pattern_to_regex("/foo/boo") == r"/foo/boo"
    assert _pattern_to_regex("/foo/<boo>/woo") == r"/foo/(?P<boo>[\w-]+)/woo"


def test_expect_urlshape():
    assert expect_urlshape("/foo", "/foo") == {}

    with pytest.raises(exceptions.NotFoundException):
        expect_urlshape("/foo", "/boo")

    assert expect_urlshape("/foo/<boo>/zoo", "/foo/woo-woo/zoo") == {"boo": "woo-woo"}

    with pytest.raises(exceptions.NotFoundException):
        expect_urlshape("/foo/<boo>/zoo", "foo/woo/")


def test_pubsubify_excs(client_with_modifiable_routes: flask.testing.FlaskClient):
    client = client_with_modifiable_routes
    # pre-populate an import that will get error'd
    with session_ctx() as sess:
        new_import = model.Import("aa", "aa", "uuid", "aa@aa.aa", "gs://aa/aa", "pfb")
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
