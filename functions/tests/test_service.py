import flask.testing
import jsonschema

from ..common import db
from ..common.model import *
from .. import service


def test_schema_valid():
    jsonschema.Draft7Validator.check_schema(service.NEW_IMPORT_SCHEMA)


def test_missing_json(client: flask.testing.FlaskClient):
    resp = client.post('/iservice')
    assert resp.status_code == 400


def test_not_json(client):
    resp = client.post('/iservice', data="not a json object")
    assert resp.status_code == 400


def test_bad_json(client):
    resp = client.post('/iservice', json={"bees":"buzz"})
    assert resp.status_code == 400


def test_good_json(client):
    resp = client.post('/iservice', json={"path": "foo", "filetype": "pfb"})
    assert resp.status_code == 200

    # response contains the job ID, check it's actually in the database
    sess = db.get_session()
    dbres = sess.query(Import).filter(Import.id == resp.get_data(as_text=True)).all()
    assert len(dbres) == 1
    assert dbres[0].id == str(resp.get_data(as_text=True))
