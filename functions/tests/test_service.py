from functions import service
import jsonschema


def test_schema_valid():
    jsonschema.Draft7Validator.check_schema(service.NEW_IMPORT_SCHEMA)


def test_missing_json(client):
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
