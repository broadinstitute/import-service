from typing import IO, Iterator, Sequence

import pytest
from app.external.rawls_entity_model import (AddListMember, AddUpdateAttribute,
                                             AttributeOperation,
                                             CreateAttributeValueList,
                                             RemoveAttribute)
from app.translators.pfb_to_rawls import PFBToRawls


@pytest.fixture(scope="function")
def minimal_data_pfb() -> Iterator[IO]:
    with open("app/tests/resources/minimal-data.pfb", 'rb') as out:
        yield out

@pytest.fixture(scope="function")
def fake_pfb_with_array() -> Iterator[IO]:
    with open("app/tests/resources/data-with-array.pfb", 'rb') as out:
        yield out

# file-like to ([Entity])
def test_translate_pfb_file_to_entities(fake_import, minimal_data_pfb):
    # see app/tests/resources/minimal_data.md to understand what's in "minimal_data_pfb"

    def find_add_update_attr(ops: Sequence[AttributeOperation], attrname: str) -> AddUpdateAttribute:
        return next(i for i in ops if isinstance(i, AddUpdateAttribute) and i.attributeName == attrname)

    def assert_attr_value(ops: Sequence[AttributeOperation], attrname: str, expected):
        attr = find_add_update_attr(ops, attrname)
        assert attr.addUpdateAttribute == expected

    translator = PFBToRawls()
    entities = list(translator.translate(fake_import, minimal_data_pfb))

    assert len(entities) == 1
    ent = entities[0]
    assert ent.name == 'HG01101_cram'
    assert ent.entityType == 'submitted_aligned_reads'
    assert len(ent.operations) == 16

    # spot-check a few operations
    assert_attr_value(ent.operations, 'pfb:md5sum', 'bdf121aadba028d57808101cb4455fa7')
    assert_attr_value(ent.operations, 'pfb:file_size', 512)
    assert_attr_value(ent.operations, 'pfb:file_state', 'registered')
    assert_attr_value(ent.operations, 'pfb:ga4gh_drs_uri', 'drs://example.org/dg.4503/cc32d93d-a73c-4d2c-a061-26c0410e74fa')

def test_translate_pfb_file_with_array_to_entities(fake_import, fake_pfb_with_array):
    # see app/tests/resources/data_with_array.md to understand what's in "fake_pfb_with_array"

    translator = PFBToRawls()
    entities = list(translator.translate(fake_import, fake_pfb_with_array))

    array_prop = 'pfb:file_state'

    assert len(entities) == 1
    ent = entities[0]
    assert ent.name == 'HG01101_cram'
    assert ent.entityType == 'submitted_aligned_reads'
    assert len(ent.operations) == 20 # will be 16 prior to respecting arrays

    # we should have 1 RemoveAttribute, 1 CreateAttributeValueList, and 3 AddListMember
    remove_attribute_ops = filter(lambda op: isinstance(op, RemoveAttribute), ent.operations)
    assert len(list(remove_attribute_ops)) == 1

    create_list_ops = filter(lambda op: isinstance(op, CreateAttributeValueList), ent.operations)
    assert len(list(create_list_ops)) == 1

    add_list_member_ops = filter(lambda op: isinstance(op, AddListMember), ent.operations)
    assert len(list(add_list_member_ops)) == 3

    # finally, let's validate that these operations occur in the proper order,
    # that they all work on the 'file_state' property,
    # and that the AddListMembers have the correct values
    start_index = ent.operations.index(RemoveAttribute(array_prop))
    actual = ent.operations[start_index:start_index+5]
    expected = [RemoveAttribute(array_prop),
        CreateAttributeValueList(array_prop),
        AddListMember(array_prop, '00000000-0000-0000-0000-000000000000'),
        AddListMember(array_prop, '11111111-1111-1111-1111-111111111111'),
        AddListMember(array_prop, '22222222-2222-2222-2222-222222222222')]
    assert actual == expected

def test_none_values_in_array():
    translator = PFBToRawls()
    rec = {
        'name': 'foo',
        'id': 'bar',
        'object': {
            'arr': [1, None, 2, None, 3, ],
        },
        'relations': {}
    }
    entity = translator.translate_record(rec, {}, 'pfb')

    expectedops = [
        RemoveAttribute('pfb:arr'),
        CreateAttributeValueList('pfb:arr'),
        AddListMember('pfb:arr', 1),
        AddListMember('pfb:arr', 2),
        AddListMember('pfb:arr', 3)
    ]

    assert entity.operations == expectedops
