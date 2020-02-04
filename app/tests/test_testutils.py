import pytest

from . import dummy
from . import testutils


def test_patch_request():
    with testutils.patch_request("app.tests.dummy", "get", status_code=200, text="beans"):
        resp = dummy.request()
        assert resp.ok
        assert resp.status_code == 200
        assert resp.text == "beans"

    with testutils.patch_request("app.tests.dummy", "get", status_code=400, json={"oh": "no"}):
        resp = dummy.request()
        assert not resp.ok
        assert resp.status_code == 400
        assert resp.json() == {"oh": "no"}


@pytest.mark.usefixtures(
    testutils.fxpatch("app.tests.dummy.dummy", return_value="funny"))
def test_fxpatch():
    from . import dummy
    assert dummy.dummy("test") == "funny"


def test_pubsub_json_body():
    result = testutils.pubsub_json_body({})
    result["message"]["attributes"].update({"foo": "bar"})
    assert testutils.pubsub_json_body({"foo": "bar"}) == result
