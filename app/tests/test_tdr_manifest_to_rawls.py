import io
import uuid
from datetime import datetime
from typing import IO, Dict, Generator, Sequence

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from app.db.model import Import
from app.external.rawls_entity_model import AddListMember, AddUpdateAttribute, AttributeOperation, CreateAttributeValueList, Entity, RemoveAttribute
from app.external.tdr_manifest import TDRTable
from app.translators.tdr_manifest_to_rawls import ParquetTranslator

def _import_timestamp(dt: datetime) -> AddUpdateAttribute:
    return AddUpdateAttribute('import:timestamp', dt.isoformat())

def _import_sourceid(uuid: str = 'source_snapshot_uuid') -> AddUpdateAttribute:
    return AddUpdateAttribute('import:snapshot_id', uuid)

# data frame to ([Entity])
def test_translate_data_frame():
    d = {'datarepo_row_id': ['a', 'b', 'c'], 'one': [1, 2, 3], 'two': ['foo', 'bar', 'baz']}
    df = pd.DataFrame(data=d)
    column_names = ['datarepo_row_id', 'one', 'two']

    now = datetime.now()
    random_snapshot_id = str(uuid.uuid4())

    fake_table = TDRTable('unittest', 'datarepo_row_id', [], {})
    fake_filelocation = "doesntmatter"
    fake_import_details = Import('workspace_name:', 'workspace_ns', 'workspace_uuid', 'workspace_google_project', 'submitter', 'import_url', 'filetype', True)
    fake_import_details.submit_time = now # ensure we know the submit_time
    translator = ParquetTranslator(fake_table, fake_filelocation, fake_import_details, random_snapshot_id)

    entities_gen = translator.translate_data_frame(df, column_names)
    assert isinstance(entities_gen, Generator)
    entities = list(entities_gen)
    assert len(entities) == 3
    assert entities[0].name == 'a'
    assert entities[0].entityType == 'unittest'
    assert entities[0].operations == [AddUpdateAttribute('tdr:datarepo_row_id', 'a'), AddUpdateAttribute('tdr:one', 1), AddUpdateAttribute('tdr:two', 'foo'), _import_sourceid(random_snapshot_id), _import_timestamp(now)]
    assert entities[1].name == 'b'
    assert entities[1].entityType == 'unittest'
    assert entities[1].operations == [AddUpdateAttribute('tdr:datarepo_row_id', 'b'), AddUpdateAttribute('tdr:one', 2), AddUpdateAttribute('tdr:two', 'bar'), _import_sourceid(random_snapshot_id), _import_timestamp(now)]
    assert entities[2].name == 'c'
    assert entities[2].entityType == 'unittest'
    assert entities[2].operations == [AddUpdateAttribute('tdr:datarepo_row_id', 'c'), AddUpdateAttribute('tdr:one', 3), AddUpdateAttribute('tdr:two', 'baz'), _import_sourceid(random_snapshot_id), _import_timestamp(now)]

def get_fake_parquet_translator(import_submit_time: datetime = datetime.now()) -> ParquetTranslator:
    fake_table = TDRTable('unittest', 'datarepo_row_id', [], {})
    fake_filelocation = "doesntmatter"
    fake_import_details = Import('workspace_name:', 'workspace_ns', 'workspace_uuid', 'workspace_google_project', 'submitter', 'import_url', 'filetype', True)
    fake_import_details.submit_time = import_submit_time # ensure we know the submit_time
    return ParquetTranslator(fake_table, fake_filelocation, fake_import_details, 'source_snapshot_uuid')

def data_dict_to_file_like(d: Dict) -> IO:
    file_like = io.BytesIO()
    df = pd.DataFrame(data=d)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_like)
    return file_like

# file-like to ([Entity])
def test_translate_parquet_file_to_entities():
    now = datetime.now()

    translator = get_fake_parquet_translator(now)

    # programmatically generate a parquet file-like, so we explicitly know its contents
    file_like = data_dict_to_file_like({'datarepo_row_id': ['a', 'b', 'c'], 'one': [1, 2, 3], 'two': ['foo', 'bar', 'baz']})

    entities_gen = translator.translate_parquet_file_to_entities(file_like)

    assert isinstance(entities_gen, Generator)
    entities = list(entities_gen)
    assert len(entities) == 3
    assert entities[0].name == 'a'
    assert entities[0].entityType == 'unittest'
    assert entities[0].operations == [AddUpdateAttribute('tdr:datarepo_row_id', 'a'), AddUpdateAttribute('tdr:one', 1), AddUpdateAttribute('tdr:two', 'foo'), _import_sourceid(), _import_timestamp(now)]
    assert entities[1].name == 'b'
    assert entities[1].entityType == 'unittest'
    assert entities[1].operations == [AddUpdateAttribute('tdr:datarepo_row_id', 'b'), AddUpdateAttribute('tdr:one', 2), AddUpdateAttribute('tdr:two', 'bar'), _import_sourceid(), _import_timestamp(now)]
    assert entities[2].name == 'c'
    assert entities[2].entityType == 'unittest'
    assert entities[2].operations == [AddUpdateAttribute('tdr:datarepo_row_id', 'c'), AddUpdateAttribute('tdr:one', 3), AddUpdateAttribute('tdr:two', 'baz'), _import_sourceid(), _import_timestamp(now)]

# file-like to ([Entity])
def test_translate_parquet_file_with_missing_pk():
    now = datetime.now()

    fake_table = TDRTable('unittest', 'custompk', [], {})
    fake_filelocation = "doesntmatter"
    fake_import_details = Import('workspace_name:', 'workspace_ns', 'workspace_uuid', 'workspace_google_project', 'submitter', 'import_url', 'filetype', True)
    fake_import_details.submit_time = now # ensure we know the submit_time
    translator = ParquetTranslator(fake_table, fake_filelocation, fake_import_details, 'source_snapshot_uuid')

    # programmatically generate a parquet file-like, so we explicitly know its contents
    # note the differences in this data frame as compared to get_fake_parquet_translator()
    file_like = data_dict_to_file_like({'datarepo_row_id': ['a', 'b', 'c'], 'custompk': ['first', None, 'third'], 'two': ['foo', 'bar', 'baz']})

    entities_gen = translator.translate_parquet_file_to_entities(file_like)

    assert isinstance(entities_gen, Generator)
    entities = list(entities_gen)
    assert len(entities) == 2
    assert entities[0].name == 'first'
    assert entities[0].entityType == 'unittest'
    assert entities[0].operations == [AddUpdateAttribute('tdr:datarepo_row_id', 'a'), AddUpdateAttribute('tdr:custompk', 'first'), AddUpdateAttribute('tdr:two', 'foo'), _import_sourceid(), _import_timestamp(now)]
    assert entities[1].name == 'third'
    assert entities[1].entityType == 'unittest'
    assert entities[1].operations == [AddUpdateAttribute('tdr:datarepo_row_id', 'c'), AddUpdateAttribute('tdr:custompk', 'third'), AddUpdateAttribute('tdr:two', 'baz'), _import_sourceid(), _import_timestamp(now)]

# file-like to ([Entity])
def test_translate_parquet_file_with_array_attrs():
    now = datetime.now()

    fake_table = TDRTable('unittest', 'custompk', [], {})
    fake_filelocation = "doesntmatter"
    fake_import_details = Import('workspace_name:', 'workspace_ns', 'workspace_uuid', 'workspace_google_project', 'submitter', 'import_url', 'filetype', True)
    fake_import_details.submit_time = now # ensure we know the submit_time
    translator = ParquetTranslator(fake_table, fake_filelocation, fake_import_details, 'source_snapshot_uuid')

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
        AddListMember('tdr:arrayattr', 'Philip'), AddListMember('tdr:arrayattr', 'Glass'),
        _import_sourceid(), _import_timestamp(now)
    ]
    assert entities[1].name == 'second'
    assert entities[1].entityType == 'unittest'
    assert entities[1].operations == [AddUpdateAttribute('tdr:datarepo_row_id', 'b'), AddUpdateAttribute('tdr:custompk', 'second'),
        RemoveAttribute('tdr:arrayattr'), CreateAttributeValueList('tdr:arrayattr'),
        AddListMember('tdr:arrayattr', 'Wolfgang'), AddListMember('tdr:arrayattr', 'Amadeus'), AddListMember('tdr:arrayattr', 'Mozart'),
        _import_sourceid(), _import_timestamp(now)
    ]
    assert entities[2].name == 'third'
    assert entities[2].entityType == 'unittest'
    assert entities[2].operations == [AddUpdateAttribute('tdr:datarepo_row_id', 'c'), AddUpdateAttribute('tdr:custompk', 'third'),
        RemoveAttribute('tdr:arrayattr'), CreateAttributeValueList('tdr:arrayattr'),
        AddListMember('tdr:arrayattr', 'Dmitri'), AddListMember('tdr:arrayattr', 'Shostakovich'),
        _import_sourceid(), _import_timestamp(now)
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

def test_translate_parquet_attr_null():
    translator = get_fake_parquet_translator()

    assert translator.translate_parquet_attr('containsnull', None) == [AddUpdateAttribute('tdr:containsnull', None)]

def test_translate_parquet_attr_NaN():
    translator = get_fake_parquet_translator()

    assert translator.translate_parquet_attr('containsNaN', np.nan) == [AddUpdateAttribute('tdr:containsNaN', None)]

def test_translate_parquet_attr_arrays_containing_null():
    translator = get_fake_parquet_translator()

    assert translator.translate_parquet_attr('myarray', np.array([1, 2, None, 4])) == [
        RemoveAttribute('tdr:myarray'), CreateAttributeValueList('tdr:myarray'),
        AddListMember('tdr:myarray', 1), AddListMember('tdr:myarray', 2),
        AddListMember('tdr:myarray', None), AddListMember('tdr:myarray', 4)]

    assert translator.translate_parquet_attr('myarray', np.array(['foo', None, 'bar'])) == [
        RemoveAttribute('tdr:myarray'), CreateAttributeValueList('tdr:myarray'),
        AddListMember('tdr:myarray', 'foo'), AddListMember('tdr:myarray', None), AddListMember('tdr:myarray', 'bar')]

    assert translator.translate_parquet_attr('myarray', np.array([False, None, True])) == [
        RemoveAttribute('tdr:myarray'), CreateAttributeValueList('tdr:myarray'),
        AddListMember('tdr:myarray', False), AddListMember('tdr:myarray', None), AddListMember('tdr:myarray', True)]

    time1 = datetime.now()
    time2 = datetime.now()
    assert translator.translate_parquet_attr('myarray', np.array([time1, None, time2])) == [
        RemoveAttribute('tdr:myarray'), CreateAttributeValueList('tdr:myarray'),
        AddListMember('tdr:myarray', str(time1)), AddListMember('tdr:myarray', None),AddListMember('tdr:myarray', str(time2))]

def test_translate_parquet_attr_arrays_containing_NaN():
    translator = get_fake_parquet_translator()

    assert translator.translate_parquet_attr('myarray', np.array([1, 2, np.nan, 4])) == [
        RemoveAttribute('tdr:myarray'), CreateAttributeValueList('tdr:myarray'),
        AddListMember('tdr:myarray', 1), AddListMember('tdr:myarray', 2),
        AddListMember('tdr:myarray', None), AddListMember('tdr:myarray', 4)]

def test_actual_parquet_file_with_NaN():
    translator = get_fake_parquet_translator()

    def find_add_update_attr(ops: Sequence[AttributeOperation], attrname: str) -> AddUpdateAttribute:
        return next(i for i in ops if isinstance(i, AddUpdateAttribute) and i.attributeName == attrname)

    def assert_attr_value(ops: Sequence[AttributeOperation], attrname: str, expected):
        attr = find_add_update_attr(e.operations, attrname)
        assert attr.addUpdateAttribute == expected

    # 1000 Genomes public data. This Parquet file contains one row. That row
    # contains nulls in float and boolean columns
    with open('app/tests/resources/1000_Genomes_1row_sample_info.parquet', 'rb') as file_like:
        entities = list(translator.translate_parquet_file_to_entities(file_like))
        assert len(entities) == 1
        e: Entity = entities[0]
        assert len(e.operations) == 65
        # spot-check a few of the attributes
        assert_attr_value(e.operations, 'tdr:VerifyBam_LC_Affy_Chip', None) # Float, contains null in BQ
        assert_attr_value(e.operations, 'tdr:VerifyBam_E_Affy_Chip', None) # Float, contains null in BQ
        assert_attr_value(e.operations, 'tdr:Grandparents', '') # String, contains empty string in BQ
        assert_attr_value(e.operations, 'tdr:VerifyBam_LC_Omni_Chip', 0.00026) # Float, contains 2.6E-4 in BQ
        assert_attr_value(e.operations, 'tdr:In_Final_Phase_Variant_Calling', True) # Boolean, contains true in BQ
        assert_attr_value(e.operations, 'tdr:In_Low_Coverage_Pilot', None)  # Boolean, contains null in BQ
        assert_attr_value(e.operations, 'tdr:Population_Description', 'British in England and Scotland') # String, contains 'British in England and Scotland' in BQ
