import io
from datetime import datetime
from typing import IO, Dict, Generator

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from app.db.model import Import
from app.external.rawls_entity_model import AddListMember, AddUpdateAttribute, CreateAttributeValueList, RemoveAttribute
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
    assert entities[0].operations == [AddUpdateAttribute('tdr:datarepo_row_id', 'a'), AddUpdateAttribute('tdr:one', 1), AddUpdateAttribute('tdr:two', 'foo')]
    assert entities[1].name == 'b'
    assert entities[1].entityType == 'unittest'
    assert entities[1].operations == [AddUpdateAttribute('tdr:datarepo_row_id', 'b'), AddUpdateAttribute('tdr:one', 2), AddUpdateAttribute('tdr:two', 'bar')]
    assert entities[2].name == 'c'
    assert entities[2].entityType == 'unittest'
    assert entities[2].operations == [AddUpdateAttribute('tdr:datarepo_row_id', 'c'), AddUpdateAttribute('tdr:one', 3), AddUpdateAttribute('tdr:two', 'baz')]

def get_fake_parquet_translator() -> ParquetTranslator:
    fake_table = TDRTable('unittest', 'datarepo_row_id', [], {})
    fake_filelocation = "doesntmatter"
    fake_import_details = Import('workspace_name:', 'workspace_ns', 'workspace_uuid', 'workspace_google_project', 'submitter', 'import_url', 'filetype', True)
    return ParquetTranslator(fake_table, fake_filelocation, fake_import_details)

def data_dict_to_file_like(d: Dict) -> IO:
    file_like = io.BytesIO()
    df = pd.DataFrame(data=d)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_like)
    return file_like

# file-like to ([Entity])
def test_translate_parquet_file_to_entities():
    translator = get_fake_parquet_translator()

    # programmatically generate a parquet file-like, so we explicitly know its contents
    file_like = data_dict_to_file_like({'datarepo_row_id': ['a', 'b', 'c'], 'one': [1, 2, 3], 'two': ['foo', 'bar', 'baz']})

    entities_gen = translator.translate_parquet_file_to_entities(file_like)

    assert isinstance(entities_gen, Generator)
    entities = list(entities_gen)
    assert len(entities) == 3
    assert entities[0].name == 'a'
    assert entities[0].entityType == 'unittest'
    assert entities[0].operations == [AddUpdateAttribute('tdr:datarepo_row_id', 'a'), AddUpdateAttribute('tdr:one', 1), AddUpdateAttribute('tdr:two', 'foo')]
    assert entities[1].name == 'b'
    assert entities[1].entityType == 'unittest'
    assert entities[1].operations == [AddUpdateAttribute('tdr:datarepo_row_id', 'b'), AddUpdateAttribute('tdr:one', 2), AddUpdateAttribute('tdr:two', 'bar')]
    assert entities[2].name == 'c'
    assert entities[2].entityType == 'unittest'
    assert entities[2].operations == [AddUpdateAttribute('tdr:datarepo_row_id', 'c'), AddUpdateAttribute('tdr:one', 3), AddUpdateAttribute('tdr:two', 'baz')]

# file-like to ([Entity])
def test_translate_parquet_file_with_missing_pk():
    fake_table = TDRTable('unittest', 'custompk', [], {})
    fake_filelocation = "doesntmatter"
    fake_import_details = Import('workspace_name:', 'workspace_ns', 'workspace_uuid', 'workspace_google_project', 'submitter', 'import_url', 'filetype', True)
    translator = ParquetTranslator(fake_table, fake_filelocation, fake_import_details)

    # programmatically generate a parquet file-like, so we explicitly know its contents
    # note the differences in this data frame as compared to get_fake_parquet_translator()
    file_like = data_dict_to_file_like({'datarepo_row_id': ['a', 'b', 'c'], 'custompk': ['first', None, 'third'], 'two': ['foo', 'bar', 'baz']})

    entities_gen = translator.translate_parquet_file_to_entities(file_like)

    assert isinstance(entities_gen, Generator)
    entities = list(entities_gen)
    assert len(entities) == 2
    assert entities[0].name == 'first'
    assert entities[0].entityType == 'unittest'
    assert entities[0].operations == [AddUpdateAttribute('tdr:datarepo_row_id', 'a'), AddUpdateAttribute('tdr:custompk', 'first'), AddUpdateAttribute('tdr:two', 'foo')]
    assert entities[1].name == 'third'
    assert entities[1].entityType == 'unittest'
    assert entities[1].operations == [AddUpdateAttribute('tdr:datarepo_row_id', 'c'), AddUpdateAttribute('tdr:custompk', 'third'), AddUpdateAttribute('tdr:two', 'baz')]

# file-like to ([Entity])
def test_translate_parquet_file_with_array_attrs():
    fake_table = TDRTable('unittest', 'custompk', [], {})
    fake_filelocation = "doesntmatter"
    fake_import_details = Import('workspace_name:', 'workspace_ns', 'workspace_uuid', 'workspace_google_project', 'submitter', 'import_url', 'filetype', True)
    translator = ParquetTranslator(fake_table, fake_filelocation, fake_import_details)

    # programmatically generate a parquet file-like, so we explicitly know its contents
    # note the differences in this data frame as compared to get_fake_parquet_translator()
    file_like = data_dict_to_file_like({'datarepo_row_id': ['a', 'b', 'c'], 'custompk': ['first', 'second', 'third'], 'arrayattr': [
        ['Philip', 'Glass'],
        ['Wolfgang', 'Amadeus', 'Mozart'],
        ['Dmitri', 'Shostakovich']
    ]})

    entities_gen = translator.translate_parquet_file_to_entities(file_like)

    assert isinstance(entities_gen, Generator)
    entities = list(entities_gen)
    assert len(entities) == 3
    assert entities[0].name == 'first'
    assert entities[0].entityType == 'unittest'
    assert entities[0].operations == [AddUpdateAttribute('tdr:datarepo_row_id', 'a'), AddUpdateAttribute('tdr:custompk', 'first'),
        RemoveAttribute('tdr:arrayattr'), CreateAttributeValueList('tdr:arrayattr'),
        AddListMember('tdr:arrayattr', 'Philip'), AddListMember('tdr:arrayattr', 'Glass')
    ]
    assert entities[1].name == 'second'
    assert entities[1].entityType == 'unittest'
    assert entities[1].operations == [AddUpdateAttribute('tdr:datarepo_row_id', 'b'), AddUpdateAttribute('tdr:custompk', 'second'),
        RemoveAttribute('tdr:arrayattr'), CreateAttributeValueList('tdr:arrayattr'),
        AddListMember('tdr:arrayattr', 'Wolfgang'), AddListMember('tdr:arrayattr', 'Amadeus'), AddListMember('tdr:arrayattr', 'Mozart')
    ]
    assert entities[2].name == 'third'
    assert entities[2].entityType == 'unittest'
    assert entities[2].operations == [AddUpdateAttribute('tdr:datarepo_row_id', 'c'), AddUpdateAttribute('tdr:custompk', 'third'),
        RemoveAttribute('tdr:arrayattr'), CreateAttributeValueList('tdr:arrayattr'),
        AddListMember('tdr:arrayattr', 'Dmitri'), AddListMember('tdr:arrayattr', 'Shostakovich')
    ]

# KVP to AttributeOperation
def test_translate_parquet_attr():
    translator = get_fake_parquet_translator()
    # translator has entityType 'unittest', so 'unittest_id' should be namespaced
    assert translator.translate_parquet_attr('unittest_id', 123) == [AddUpdateAttribute('tdr:unittest_id', 123)]
    assert translator.translate_parquet_attr('somethingelse', 123) == [AddUpdateAttribute('tdr:somethingelse', 123)]
    assert translator.translate_parquet_attr('datarepo_row_id', 123) == [AddUpdateAttribute('tdr:datarepo_row_id', 123)]

    assert translator.translate_parquet_attr('foo', True) == [AddUpdateAttribute('tdr:foo', True)]
    assert translator.translate_parquet_attr('foo', 'astring') == [AddUpdateAttribute('tdr:foo', 'astring')]
    assert translator.translate_parquet_attr('foo', 123) == [AddUpdateAttribute('tdr:foo', 123)]
    assert translator.translate_parquet_attr('foo', 456.78) == [AddUpdateAttribute('tdr:foo', 456.78)]

    curtime = datetime.now()
    assert translator.translate_parquet_attr('foo', curtime) == [AddUpdateAttribute('tdr:foo', str(curtime))]

    arr = ['a', 'b', 'c']
    assert translator.translate_parquet_attr('foo', arr) == [AddUpdateAttribute('tdr:foo', str(arr))]

def test_translate_parquet_attr_arrays():
    translator = get_fake_parquet_translator()

    assert translator.translate_parquet_attr('myarray', np.array([1, 2, 3])) == [
        RemoveAttribute('tdr:myarray'), CreateAttributeValueList('tdr:myarray'),
        AddListMember('tdr:myarray', 1), AddListMember('tdr:myarray', 2), AddListMember('tdr:myarray', 3)]

    assert translator.translate_parquet_attr('myarray', np.array(['foo', 'bar'])) == [
        RemoveAttribute('tdr:myarray'), CreateAttributeValueList('tdr:myarray'),
        AddListMember('tdr:myarray', 'foo'), AddListMember('tdr:myarray', 'bar')]

    time1 = datetime.now()
    time2 = datetime.now()
    assert translator.translate_parquet_attr('myarray', np.array([time1, time2])) == [
        RemoveAttribute('tdr:myarray'), CreateAttributeValueList('tdr:myarray'),
        AddListMember('tdr:myarray', str(time1)), AddListMember('tdr:myarray', str(time2))]
