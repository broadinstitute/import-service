from typing import Generator, List, Sequence
import pandas as pd
import pyarrow.parquet as pq
from app.db.model import Import
from app.external.rawls_entity_model import AddUpdateAttribute
from app.external.tdr_manifest import TDRTable

from app.translators.tdr_manifest_to_rawls import ParquetTranslator, TDRManifestToRawls


# this unit test asserts that the parquet translation results in an iterator with one entity per snapshot table.
# this test is only valid for the noop end-to-end spike, in which we don't actually look inside the parquet file.
# once we implement real parquet translation, this test must be deleted or updated.
# def test_noop_translate_tdr_manifest(fake_tdr_manifest, fake_import):
#     """Proper translation of parquet files to rawls json."""
#     translator = TDRManifestToRawls()
#     result_iterator = translator.translate(fake_import, fake_tdr_manifest, "tdrexport")

#     reclist = list(result_iterator)

#     # test-data manifest has 27 tables
#     assert (len(reclist) == 27)

def test_translate_data_frame():
    d = {'datarepo_row_id': ['a', 'b', 'c'], 'one': [1, 2, 3], 'two': ['foo', 'bar', 'baz']}
    df = pd.DataFrame(data=d)
    column_names = ['datarepo_row_id', 'one', 'two']

    fake_table = TDRTable('unittest', 'datarepo_row_id', [], {})
    fake_filelocation = "doesntmatter"
    fake_import_details = Import('workspace_name:', 'workspace_ns', 'workspace_uuid', 'workspace_google_project', 'submitter', 'import_url', 'filetype', True)
    translator = ParquetTranslator(fake_table, fake_filelocation, fake_import_details)

    entities_gen = translator.translate_data_frame(df, column_names)
    assert isinstance(entities_gen, Generator)
    entities = list(entities_gen)
    assert len(entities) == 3
    assert entities[0].name == 'a'
    assert entities[0].entityType == 'unittest'
    assert entities[0].operations == [AddUpdateAttribute('datarepo_row_id', 'a'), AddUpdateAttribute('one', 1), AddUpdateAttribute('two', 'foo')]
    assert entities[1].name == 'b'
    assert entities[1].entityType == 'unittest'
    assert entities[1].operations == [AddUpdateAttribute('datarepo_row_id', 'b'), AddUpdateAttribute('one', 2), AddUpdateAttribute('two', 'bar')]
    assert entities[2].name == 'c'
    assert entities[2].entityType == 'unittest'
    assert entities[2].operations == [AddUpdateAttribute('datarepo_row_id', 'c'), AddUpdateAttribute('one', 3), AddUpdateAttribute('two', 'baz')]
    


# def test_read_parquet(sample_tdr_parquet_file):
#     translator = TDRManifestToRawls()
#     entities_to_add = list(translator.convert_parquet_file_to_entities(sample_tdr_parquet_file, 'testtype', 'datarepo_row_id'))

#     pq_table = pq.read_table(sample_tdr_parquet_file)
#     assert len(entities_to_add) == pq_table.num_rows
#     assert len(entities_to_add[0].operations) == pq_table.num_columns

    # first_attr_name = attrs_to_add[0][0].attributeName
    # assert first_attr_name == pq_table.column_names[0]
    # assert pq_table.to_batches()[0].to_pydict()[first_attr_name][0] == attrs_to_add[0][0].addUpdateAttribute
