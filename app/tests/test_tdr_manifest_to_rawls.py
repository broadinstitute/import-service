from datetime import datetime
from typing import Generator

import pandas as pd
import pyarrow.parquet as pq
from app.db.model import Import
from app.external.rawls_entity_model import AddUpdateAttribute
from app.external.tdr_manifest import TDRTable
from app.translators.tdr_manifest_to_rawls import ParquetTranslator


# data frame to ([Entity])
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

def get_fake_parquet_translator() -> ParquetTranslator:
    fake_table = TDRTable('unittest', 'datarepo_row_id', [], {})
    fake_filelocation = "doesntmatter"
    fake_import_details = Import('workspace_name:', 'workspace_ns', 'workspace_uuid', 'workspace_google_project', 'submitter', 'import_url', 'filetype', True)
    return ParquetTranslator(fake_table, fake_filelocation, fake_import_details)

# file-like to ([Entity])
def test_translate_parquet_file_to_entities(sample_tdr_parquet_file):
    # TODO: can we programmatically generate a parquet file-like, so we explicitly know its contents?
    translator = get_fake_parquet_translator()

    entity_gen = translator.translate_parquet_file_to_entities(sample_tdr_parquet_file)

    entities_to_add = list(entity_gen) # materialize

    pq_table = pq.read_table(sample_tdr_parquet_file)
    assert len(entities_to_add) == pq_table.num_rows
    assert len(entities_to_add[0].operations) == pq_table.num_columns

# KVP to AttributeOperation
def test_translate_parquet_attr():
    translator = get_fake_parquet_translator()
    # translator has entityType 'unittest', so 'unittest_id' should be namespaced
    assert translator.translate_parquet_attr('unittest_id', 123) == AddUpdateAttribute('pfb:unittest_id', 123)
    assert translator.translate_parquet_attr('somethingelse', 123) == AddUpdateAttribute('somethingelse', 123)
    assert translator.translate_parquet_attr('datarepo_row_id', 123) == AddUpdateAttribute('datarepo_row_id', 123)


    assert translator.translate_parquet_attr('foo', True) == AddUpdateAttribute('foo', True)
    assert translator.translate_parquet_attr('foo', 'astring') == AddUpdateAttribute('foo', 'astring')
    assert translator.translate_parquet_attr('foo', 123) == AddUpdateAttribute('foo', 123)
    assert translator.translate_parquet_attr('foo', 456.78) == AddUpdateAttribute('foo', 456.78)
    
    curtime = datetime.now()
    assert translator.translate_parquet_attr('foo', curtime) == AddUpdateAttribute('foo', str(curtime))

    arr = ['a', 'b', 'c']
    assert translator.translate_parquet_attr('foo', arr) == AddUpdateAttribute('foo', str(arr))



