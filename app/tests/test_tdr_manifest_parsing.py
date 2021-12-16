import json

from app.external.tdr_manifest import TDRManifestParser


def test_manifest_parsing():
    # read the file into a raw dict
    jso = json.load(open('./app/tests/response_1638551384572.json'))

    # parse into tables
    parsed = TDRManifestParser(jso)
    tables = parsed.get_tables()
    assert len(tables) == 27
    for t in tables:
        assert len(t.parquet_files) > 0, f"table {t.name} should have parquet export files"
        for p in t.parquet_files:
            assert t.name in p, "parquet export file path should contain parent table name"

    all_table_names = map(lambda t: t.name, tables)

    # hardcoded to match the values in /Users/davidan/work/src/import-service/app/tests/response_1638551384572.json
    expected_table_names = ["messages", "location", "cost", "product", "footnote", "diagram", "graphic",
    "sequence", "project", "person", "signature", "photo", "regulation", "annotation", "lab", "edges", "xray",
    "test", "vial", "process", "test_result", "room", "chemical", "letter", "species", "reading", "genome"]

    assert parsed.get_snapshot_id() == '9516afec-583f-11ec-bf63-0242ac130002'
    assert parsed.get_snapshot_name() == 'unit_test_snapshot'
    assert (set(all_table_names) == set(expected_table_names))

# TODO: add tests for table ordering based on references, any other features
