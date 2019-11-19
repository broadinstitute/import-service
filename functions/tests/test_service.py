import flask.testing
import jsonschema

from ..common import db
from ..common.model import *
from .. import service


def test_schema_valid():
    jsonschema.Draft7Validator.check_schema(service.NEW_IMPORT_SCHEMA)


def test_wrong_httpmethod(client: flask.testing.FlaskClient):
    resp = client.get('/iservice/namespace/name/import')
    assert resp.status_code == 405


def test_wrong_path(client: flask.testing.FlaskClient):
    resp = client.post('/iservice/import')
    assert resp.status_code == 404


def test_missing_json(client: flask.testing.FlaskClient):
    resp = client.post('/iservice/namespace/name/import')
    assert resp.status_code == 400


def test_not_json(client):
    resp = client.post('/iservice/namespace/name/import', data="not a json object")
    assert resp.status_code == 400


def test_bad_json(client):
    resp = client.post('/iservice/namespace/name/import', json={"bees":"buzz"})
    assert resp.status_code == 400


def test_good_json(client):
    resp = client.post('/iservice/namespace/name/import', json={"path": "foo", "filetype": "pfb"})
    assert resp.status_code == 200

    # response contains the job ID, check it's actually in the database
    sess = db.get_session()
    dbres = sess.query(Import).filter(Import.id == resp.get_data(as_text=True)).all()
    assert len(dbres) == 1
    assert dbres[0].id == str(resp.get_data(as_text=True))
