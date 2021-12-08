import json
from urllib.parse import urlparse

# TODO: delete these tests in favor of test_tdr_manifest_to_rawls.py

def test_url_parsing():
    parsedurl = urlparse("gs://datarepo-tools-83c20e98-snapshot-export-bucket/4WKdTpfwTmOevbNouz171g/manifest.json")
    assert parsedurl.scheme == "gs"
    assert parsedurl.hostname == "datarepo-tools-83c20e98-snapshot-export-bucket"
    assert parsedurl.netloc == "datarepo-tools-83c20e98-snapshot-export-bucket"
    assert parsedurl.path == "/4WKdTpfwTmOevbNouz171g/manifest.json"

def test_raw_dicts():
    # read the file into a raw dict
    jso = json.load(open('./app/tests/response_1638551384572.json'))

    snapshot = jso["snapshot"]
    format = jso["format"]["parquet"]["location"]  # pylint: disable=redefined-builtin

    tablekeys = []
    for t in snapshot["tables"]:
        pk = "datarepo_row_id"
        if (t["primaryKey"] is not None):
            pk = t["primaryKey"]
        tablekeys.append(f"Table {t['name']} has primary key {pk}")

    assert len(tablekeys) == len(snapshot["tables"])

    relationships = []
    for r in snapshot["relationships"]:
        f = r["from"]
        t = r["to"]
        relationships.append(f"{f['table']}.{f['column']} -> {t['table']}.{t['column']}")

    assert len(relationships) == len(snapshot["relationships"])

    assert snapshot["id"] == "9516afec-583f-11ec-bf63-0242ac130002"
    assert snapshot["name"] == "unit_test_snapshot"

    exports = dict(map(lambda e: (e["name"], e["paths"]), format["tables"]))

    assert ['gs://my-project-snapshot-export-bucket/feKB6na2Ta2EI5xZDwCqjA/annotation/annotation-000000000000.parquet'] == exports['annotation']
