import json

from app.external.tdr_manifest import TDRManifestParser


def test_manifest_parsing():
    # read the file into a raw dict
    jso = json.load(open('./app/tests/response_1638551384572.json'))

    # parse into tables
    tables = TDRManifestParser(jso).get_tables()
    assert len(tables) == 27
    for t in tables:
        assert len(t.parquet_files) > 0, f"table {t.name} should have parquet export files"

# TODO: add tests for table ordering based on references, any other features

