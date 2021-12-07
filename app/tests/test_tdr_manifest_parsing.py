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

    tablekeys = []
    for t in jso["tables"]:
        pk = "datarepo_row_id"
        if (t["primaryKey"] is not None):
            pk = t["primaryKey"]
        tablekeys.append(f"Table {t['name']} has primary key {pk}")

    assert len(tablekeys) == len(jso["tables"])

    relationships = []
    for r in jso["relationships"]:
        f = r["from"]
        t = r["to"]
        relationships.append(f"{f['table']}.{f['column']} -> {t['table']}.{t['column']}")

    assert len(relationships) == len(jso["relationships"])

    assert jso["id"] == "8bf100c0-7ac3-4860-80ca-28093f4adb61"
    assert jso["name"] == "hca_dev_90bd693340c048d48d76778c103bf545__20210827_20211110"
