import io
import uuid
from datetime import datetime
from typing import IO, Dict, Generator, Sequence

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from app.db.model import Import
from app.external.rawls_entity_model import AddListMember, AddUpdateAttribute, AttributeOperation, \
    CreateAttributeEntityReferenceList, CreateAttributeValueList, Entity, EntityReference, RemoveAttribute
from app.external.tdr_manifest import TDRTable, TDRManifestParser
from app.translators.tdr_manifest_to_rawls import ParquetTranslator, TDRManifestToRawls
from app.translators import tdr_manifest_to_rawls
import json

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

    fake_table = TDRTable('unittest', 'datarepo_row_id', [], {}, [])
    fake_filelocation = "doesntmatter"
    fake_import_details = Import('workspace_name:', 'workspace_ns', 'workspace_uuid', 'workspace_google_project', 'submitter', 'import_url', 'filetype', True)
    fake_import_details.submit_time = now # ensure we know the submit_time
    translator = ParquetTranslator(fake_table, fake_filelocation, fake_import_details, random_snapshot_id)

    entities_gen = translator.translate_data_frame(df, column_names, False)
    assert isinstance(entities_gen, Generator)
    entities = list(entities_gen)
    assert len(entities) == 3
    assert entities[0].name == 'a'
    assert entities[0].entityType == 'unittest'
    # assert entities[0].operations == [AddUpdateAttribute('datarepo_row_id', 'a'), AddUpdateAttribute('one', 1), AddUpdateAttribute('two', 'foo'), _import_sourceid(random_snapshot_id), _import_timestamp(now)]

    assert entities[0].operations == [_import_timestamp(now), _import_sourceid(random_snapshot_id), AddUpdateAttribute('datarepo_row_id', 'a'), AddUpdateAttribute('one', 1), AddUpdateAttribute('two', 'foo')]

    assert entities[1].name == 'b'
    assert entities[1].entityType == 'unittest'
    assert entities[1].operations == [_import_timestamp(now), _import_sourceid(random_snapshot_id), AddUpdateAttribute('datarepo_row_id', 'b'), AddUpdateAttribute('one', 2), AddUpdateAttribute('two', 'bar')]
    assert entities[2].name == 'c'
    assert entities[2].entityType == 'unittest'
    assert entities[2].operations == [_import_timestamp(now), _import_sourceid(random_snapshot_id), AddUpdateAttribute('datarepo_row_id', 'c'), AddUpdateAttribute('one', 3), AddUpdateAttribute('two', 'baz')]

def get_fake_parquet_translator(import_submit_time: datetime = datetime.now(), table_name: str='unittest', primary_key: str='datarepo_row_id') -> ParquetTranslator:
    fake_table = TDRTable(table_name, primary_key, [], {'test_ref_column': 'other_entity_type'}, [])
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
    assert entities[0].operations == [_import_timestamp(now), _import_sourceid(), AddUpdateAttribute('datarepo_row_id', 'a'), AddUpdateAttribute('one', 1), AddUpdateAttribute('two', 'foo')]
    assert entities[1].name == 'b'
    assert entities[1].entityType == 'unittest'
    assert entities[1].operations == [_import_timestamp(now), _import_sourceid(), AddUpdateAttribute('datarepo_row_id', 'b'), AddUpdateAttribute('one', 2), AddUpdateAttribute('two', 'bar')]
    assert entities[2].name == 'c'
    assert entities[2].entityType == 'unittest'
    assert entities[2].operations == [_import_timestamp(now), _import_sourceid(), AddUpdateAttribute('datarepo_row_id', 'c'), AddUpdateAttribute('one', 3), AddUpdateAttribute('two', 'baz')]

# file-like to ([Entity])
def test_translate_parquet_file_with_missing_pk():
    now = datetime.now()
    translator = get_fake_parquet_translator(import_submit_time=now, primary_key='custompk')

    # programmatically generate a parquet file-like, so we explicitly know its contents
    # note the differences in this data frame as compared to get_fake_parquet_translator()
    file_like = data_dict_to_file_like({'datarepo_row_id': ['a', 'b', 'c'], 'custompk': ['first', None, 'third'], 'two': ['foo', 'bar', 'baz']})

    entities_gen = translator.translate_parquet_file_to_entities(file_like)

    assert isinstance(entities_gen, Generator)
    entities = list(entities_gen)
    assert len(entities) == 2
    assert entities[0].name == 'first'
    assert entities[0].entityType == 'unittest'
    assert entities[0].operations == [_import_timestamp(now), _import_sourceid(), AddUpdateAttribute('datarepo_row_id', 'a'), AddUpdateAttribute('custompk', 'first'), AddUpdateAttribute('two', 'foo')]
    assert entities[1].name == 'third'
    assert entities[1].entityType == 'unittest'
    assert entities[1].operations == [_import_timestamp(now), _import_sourceid(), AddUpdateAttribute('datarepo_row_id', 'c'), AddUpdateAttribute('custompk', 'third'), AddUpdateAttribute('two', 'baz')]

# file-like to ([Entity])
def test_translate_parquet_file_with_array_attrs():
    now = datetime.now()
    translator = get_fake_parquet_translator(import_submit_time=now, primary_key='custompk')

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
    assert entities[0].operations == [_import_timestamp(now), _import_sourceid(),
        AddUpdateAttribute('datarepo_row_id', 'a'),
        AddUpdateAttribute('custompk', 'first'),
        RemoveAttribute('arrayattr'), CreateAttributeValueList('arrayattr'),
        AddListMember('arrayattr', 'Philip'), AddListMember('arrayattr', 'Glass')
    ]
    assert entities[1].name == 'second'
    assert entities[1].entityType == 'unittest'
    assert entities[1].operations == [_import_timestamp(now), _import_sourceid(),
        AddUpdateAttribute('datarepo_row_id', 'b'),
        AddUpdateAttribute('custompk', 'second'),
        RemoveAttribute('arrayattr'), CreateAttributeValueList('arrayattr'),
        AddListMember('arrayattr', 'Wolfgang'), AddListMember('arrayattr', 'Amadeus'), AddListMember('arrayattr', 'Mozart')
    ]
    assert entities[2].name == 'third'
    assert entities[2].entityType == 'unittest'
    assert entities[2].operations == [_import_timestamp(now), _import_sourceid(),
        AddUpdateAttribute('datarepo_row_id', 'c'),
        AddUpdateAttribute('custompk', 'third'),
        RemoveAttribute('arrayattr'), CreateAttributeValueList('arrayattr'),
        AddListMember('arrayattr', 'Dmitri'), AddListMember('arrayattr', 'Shostakovich')
    ]

# KVP to AttributeOperation
def test_translate_parquet_attr():
    translator = get_fake_parquet_translator()
    # translator has entityType 'unittest', so 'unittest_id' should be namespaced
    assert translator.translate_parquet_attr('unittest_id', 123) == [AddUpdateAttribute('tdr:unittest_id', 123)]
    assert translator.translate_parquet_attr('somethingelse', 123) == [AddUpdateAttribute('somethingelse', 123)]
    assert translator.translate_parquet_attr('datarepo_row_id', 123) == [AddUpdateAttribute('datarepo_row_id', 123)]

    # name and entityType should always be namespaced
    assert translator.translate_parquet_attr('name', 123) == [AddUpdateAttribute('tdr:name', 123)]
    assert translator.translate_parquet_attr('EnTiTyType', 123) == [AddUpdateAttribute('tdr:EnTiTyType', 123)]

    # import should always be namespaced
    assert translator.translate_parquet_attr('import:fake_timestamp', 123) == [AddUpdateAttribute('import:fake_timestamp', 123)]

    assert translator.translate_parquet_attr('foo', True) == [AddUpdateAttribute('foo', True)]
    assert translator.translate_parquet_attr('foo', 'astring') == [AddUpdateAttribute('foo', 'astring')]
    assert translator.translate_parquet_attr('foo', 123) == [AddUpdateAttribute('foo', 123)]
    assert translator.translate_parquet_attr('foo', 456.78) == [AddUpdateAttribute('foo', 456.78)]

    # entity reference attribute tests
    assert translator.translate_parquet_attr('test_ref_column', 'some_sample') == \
        [AddUpdateAttribute('test_ref_column', EntityReference('some_sample', 'other_entity_type'))]

    curtime = datetime.now()
    assert translator.translate_parquet_attr('foo', curtime) == [AddUpdateAttribute('foo', str(curtime))]

    # adding an array to the entity removes the previous entity and replaces with the new values
    arr = ['a', 'b']
    assert translator.translate_parquet_attr('foo', arr) ==\
           [RemoveAttribute('foo'), CreateAttributeValueList('foo'), AddListMember('foo', str('a')), AddListMember('foo', str('b'))]

def test_translate_parquet_attr_arrays():
    translator = get_fake_parquet_translator()

    assert translator.translate_parquet_attr('myarray', np.array([1, 2, 3])) == [
        RemoveAttribute('myarray'), CreateAttributeValueList('myarray'),
        AddListMember('myarray', 1), AddListMember('myarray', 2), AddListMember('myarray', 3)]

    assert translator.translate_parquet_attr('myarray', np.array(['foo', 'bar'])) == [
        RemoveAttribute('myarray'), CreateAttributeValueList('myarray'),
        AddListMember('myarray', 'foo'), AddListMember('myarray', 'bar')]

    time1 = datetime.now()
    time2 = datetime.now()
    assert translator.translate_parquet_attr('myarray', np.array([time1, time2])) == [
        RemoveAttribute('myarray'), CreateAttributeValueList('myarray'),
        AddListMember('myarray', str(time1)), AddListMember('myarray', str(time2))]

def test_translate_parq_reference_arrays():
    translator = get_fake_parquet_translator()

    assert translator.translate_parquet_attr('test_ref_column', np.array(['sample1', 'sample2'])) == [
        RemoveAttribute('test_ref_column'), CreateAttributeEntityReferenceList('test_ref_column'),
        AddListMember('test_ref_column', EntityReference('sample1', 'other_entity_type')),
        AddListMember('test_ref_column', EntityReference('sample2', 'other_entity_type'))
    ]

def test_translate_parquet_attr_null():
    translator = get_fake_parquet_translator()

    assert translator.translate_parquet_attr('containsnull', None) == [AddUpdateAttribute('containsnull', None)]

def test_translate_parquet_attr_NaN():
    translator = get_fake_parquet_translator()

    assert translator.translate_parquet_attr('containsNaN', np.nan) == [AddUpdateAttribute('containsNaN', None)]

def test_translate_parquet_attr_arrays_containing_null():
    translator = get_fake_parquet_translator()

    assert translator.translate_parquet_attr('myarray', np.array([1, 2, None, 4])) == [
        RemoveAttribute('myarray'), CreateAttributeValueList('myarray'),
        AddListMember('myarray', 1), AddListMember('myarray', 2),
        AddListMember('myarray', None), AddListMember('myarray', 4)]

    assert translator.translate_parquet_attr('myarray', np.array(['foo', None, 'bar'])) == [
        RemoveAttribute('myarray'), CreateAttributeValueList('myarray'),
        AddListMember('myarray', 'foo'), AddListMember('myarray', None), AddListMember('myarray', 'bar')]

    assert translator.translate_parquet_attr('myarray', np.array([False, None, True])) == [
        RemoveAttribute('myarray'), CreateAttributeValueList('myarray'),
        AddListMember('myarray', False), AddListMember('myarray', None), AddListMember('myarray', True)]

    time1 = datetime.now()
    time2 = datetime.now()
    assert translator.translate_parquet_attr('myarray', np.array([time1, None, time2])) == [
        RemoveAttribute('myarray'), CreateAttributeValueList('myarray'),
        AddListMember('myarray', str(time1)), AddListMember('myarray', None),AddListMember('myarray', str(time2))]

def test_translate_parquet_attr_arrays_containing_NaN():
    translator = get_fake_parquet_translator()

    assert translator.translate_parquet_attr('myarray', np.array([1, 2, np.nan, 4])) == [
        RemoveAttribute('myarray'), CreateAttributeValueList('myarray'),
        AddListMember('myarray', 1), AddListMember('myarray', 2),
        AddListMember('myarray', None), AddListMember('myarray', 4)]

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
        assert_attr_value(e.operations, 'VerifyBam_LC_Affy_Chip', None) # Float, contains null in BQ
        assert_attr_value(e.operations, 'VerifyBam_E_Affy_Chip', None) # Float, contains null in BQ
        assert_attr_value(e.operations, 'Grandparents', '') # String, contains empty string in BQ
        assert_attr_value(e.operations, 'VerifyBam_LC_Omni_Chip', 0.00026) # Float, contains 2.6E-4 in BQ
        assert_attr_value(e.operations, 'In_Final_Phase_Variant_Calling', True) # Boolean, contains true in BQ
        assert_attr_value(e.operations, 'In_Low_Coverage_Pilot', None)  # Boolean, contains null in BQ
        assert_attr_value(e.operations, 'Population_Description', 'British in England and Scotland') # String, contains 'British in England and Scotland' in BQ

def test_actual_parquet_file_with_primary_key_as_tablename():
    translator = get_fake_parquet_translator(table_name='sample', primary_key='sample_id')

    def find_add_update_attr(ops: Sequence[AttributeOperation], attrname: str) -> AddUpdateAttribute:
        return next(i for i in ops if isinstance(i, AddUpdateAttribute) and i.attributeName == attrname)

    def assert_attr_value(ops: Sequence[AttributeOperation], attrname: str, expected):
        attr = find_add_update_attr(ops, attrname)
        assert attr.addUpdateAttribute == expected

    # 1000 Genomes public data. This Parquet file contains one row. That row
    # contains nulls in float and boolean columns
    with open('app/tests/resources/short_sample_table.parquet', 'rb') as file_like:
        entities = list(translator.translate_parquet_file_to_entities(file_like))
        assert len(entities) == 1
        e: Entity = entities[0]
        assert len(e.operations) == 5

        # spot-check a few of the attributes
        assert e.name == 'testSample'
        with pytest.raises(StopIteration):
            find_add_update_attr(e.operations, 'sample_id')
        with pytest.raises(StopIteration):
            find_add_update_attr(e.operations, 'tdr:sample_id')
        assert_attr_value(e.operations, 'other', 'hi')
        assert_attr_value(e.operations, 'last_attribute', 'bye')

def test_namespace_added_where_required():
    translator = get_fake_parquet_translator()
    # translator has entityType 'unittest', so 'unittest_id' should be namespaced
    assert translator.add_namespace_if_required('unittest_id') == 'tdr:unittest_id'
    assert translator.add_namespace_if_required('somethingelse') == 'somethingelse'
    assert translator.add_namespace_if_required('datarepo_row_id') == 'datarepo_row_id'

    # name and entityType should always be namespaced
    assert translator.add_namespace_if_required('name') == 'tdr:name'
    assert translator.add_namespace_if_required('EnTiTyType') == 'tdr:EnTiTyType'

    # import should always be namespaced
    assert translator.add_namespace_if_required('import:fake_timestamp') == 'import:fake_timestamp'

def test_if_namespace_prefix_will_be_added():
    # no additional prefix required if a prefix is already present to make this valid
    assert not ParquetTranslator.prefix_required('import:name', 'any')

    # prefix is always required for 'name' and 'entityType'
    assert ParquetTranslator.prefix_required('name',  'any')
    assert ParquetTranslator.prefix_required('entityType',  'any')
    assert ParquetTranslator.prefix_required('nAmE',  'any')

    # prefix is required if it's not the primary key but is tableName_id
    assert ParquetTranslator.prefix_required('sample_id',  'sample')
    assert ParquetTranslator.prefix_required('sample_ID',  'sample')

    # otherwise no prefix!
    assert not ParquetTranslator.prefix_required('aliquot_id',  'sample')
    assert not ParquetTranslator.prefix_required('bam',  'sample')
    assert not ParquetTranslator.prefix_required('sample_id_id',  'sample')
    assert not ParquetTranslator.prefix_required('sample_ID_id',  'sample')


def get_fake_cyclic_parquet_translator(table: TDRTable) -> ParquetTranslator:
    fake_import_details = Import('workspace_name:', 'workspace_ns', 'workspace_uuid', 'workspace_google_project', 'submitter', 'import_url', 'filetype', True)
    return ParquetTranslator(table, "doesntmatter", fake_import_details, 'source_snapshot_uuid', None, True)
def open_fake_gcs_file(import_details, bucket, path, submitter, auth_key):
    # Trick python into thinking this is from gcs, then open a local file
    return open(path.strip("/"), 'rb')

@pytest.mark.usefixtures("sam_valid_pet_key")
def test_translate_cyclic_table(monkeypatch):
    tmtr = TDRManifestToRawls()
    fake_import_details = Import('workspace_name:', 'workspace_ns', 'workspace_uuid', 'workspace_google_project', 'submitter', 'import_url', 'filetype', True)
    jso = json.load(open('app/tests/resources/simple_cycle.json'))
    monkeypatch.setattr(tdr_manifest_to_rawls.gcs, "open_file", open_fake_gcs_file)
    parsed_manifest = TDRManifestParser(jso, fake_import_details.id)
    tables = parsed_manifest.get_tables()
    test = tmtr.translate_tables(fake_import_details, "snapshot_id", tables, parsed_manifest.is_cyclical())
    import itertools
    all_ops = [op for entity in list(itertools.chain(*test)) for op in entity.operations]
    #The beginning of the list should be entirely non-references
    first_ops = list(itertools.takewhile(lambda op:"_ref" not in op.attributeName, all_ops))
    assert first_ops
    #All the references should be at the end
    ref_ops = all_ops[len(first_ops):]
    assert ref_ops == list(filter(lambda op: "_ref" in op.attributeName, ref_ops))