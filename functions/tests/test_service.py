from functions import service
import jsonschema


def test_schema_valid():
    jsonschema.Draft7Validator.check_schema(service.NEW_IMPORT_SCHEMA)


def test_not_json(client):
    resp = client.post('/iservice', json={"bees":"buzz"})
    print(f"HTTP {resp.status} message {resp.data}")
    assert resp.status_code == 400
