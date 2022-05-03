from app.external.rawls_entity_model import AddListMember, CreateAttributeValueList, RemoveAttribute
from app.translators.pfb_to_rawls import PFBToRawls

# file-like to ([Entity])
def test_translate_pfb_file_with_array_to_entities(fake_import, fake_pfb_with_array):
    # see app/tests/resources/data_with_array.md to understand what's in "fake_pfb_with_array"

    translator = PFBToRawls()
    entities = list(translator.translate(fake_import, fake_pfb_with_array))

    array_prop = 'file_state'

    assert len(entities) == 1
    ent = entities[0]
    assert ent.name == 'HG01101_cram'
    assert ent.entityType == 'submitted_aligned_reads'
    assert len(ent.operations) == 20 # will be 16 prior to respecting arrays

    # we should have 1 RemoveAttribute, 1 CreateAttributeValueList, and 3 AddListMember
    remove_attribute_ops = filter(lambda op: isinstance(op, RemoveAttribute), ent.operations)
    assert len(remove_attribute_ops) == 1

    create_list_ops = filter(lambda op: isinstance(op, CreateAttributeValueList), ent.operations)
    assert len(create_list_ops) == 1

    add_list_member_ops = filter(lambda op: isinstance(op, AddListMember), ent.operations)
    assert len(create_list_ops) == 3

    # finally, let's validate that these operations occur in the proper order,
    # that they all work on the 'file_state' property,
    # and that the AddListMembers have the correct values
    start_index = ent.operations.index(RemoveAttribute(array_prop))
    slice = ent.operations[start_index:start_index+4]
    expected = list(
        RemoveAttribute(array_prop),
        CreateAttributeValueList(array_prop),
        AddListMember(array_prop, '00000000-0000-0000-0000-000000000000'),
        AddListMember(array_prop, '11111111-1111-1111-1111-111111111111'),
        AddListMember(array_prop, '22222222-2222-2222-2222-222222222222'))
    assert slice == expected

