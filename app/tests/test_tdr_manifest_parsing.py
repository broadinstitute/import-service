import json

from app.external.tdr_manifest import TDRManifestParser

resource_path = './app/tests/resources/'

def test_manifest_parsing():
    # read the file into a raw dict
    jso = json.load(open(resource_path + 'test_tdr_response.json'))

    # parse into tables
    parsed = TDRManifestParser(jso)
    tables = parsed.get_tables()
    assert len(tables) == 27
    for t in tables:
        assert len(t.parquet_files) > 0, f"table {t.name} should have parquet export files"
        for p in t.parquet_files:
            assert t.name in p, "parquet export file path should contain parent table name"

    all_table_names = map(lambda t: t.name, tables)

    # hardcoded to match the values in test response
    expected_table_names = ["messages", "location", "cost", "product", "footnote", "diagram", "graphic",
    "sequence", "project", "person", "signature", "photo", "regulation", "annotation", "lab", "edges", "xray",
    "test", "vial", "process", "test_result", "room", "chemical", "letter", "species", "reading", "genome"]

    assert parsed.get_snapshot_id() == '9516afec-583f-11ec-bf63-0242ac130002'
    assert parsed.get_snapshot_name() == 'unit_test_snapshot'
    assert (set(all_table_names) == set(expected_table_names))

# TODO AS-1044: add tests for table ordering based on references, any other features
def test_manifest_ordering_by_reference():
    jso = json.load(open(resource_path + 'tdr_response_with_no_cycle.json'))

    # parse into tables
    parsed = TDRManifestParser(jso)
    tables = parsed.get_tables()

    def get_table_indices(tableName: str):
        [i for i,t in enumerate(tables) if t.name == tableName]

    #  Dependency Tree:
    #      product
    #      /     \
    # footnote   cost
    #    |         |
    #  diagram    location
    #    |
    #  messages
    product_index = get_table_indices("product")[0]
    footnote_index = get_table_indices("footnote")[0]
    diagram_index = get_table_indices("diagram")[0]
    messages_index = get_table_indices("messages")[0]
    cost_index = get_table_indices("cost")[0]
    location_index = get_table_indices("location")[0]
    assert(product_index > footnote_index)
    assert(footnote_index > diagram_index)
    assert(diagram_index > messages_index)
    assert(cost_index > product_index)
    assert(location_index > cost_index)

def test_cyclic_manifest_ordering_error():
    jso = json.load(open(resource_path + 'tdr_response_with_cycle.json'))

    # parse into tables
    parsed = TDRManifestParser(jso)
    parsed.get_tables()

    # we shouldn't reach this
    assert(False)

 
