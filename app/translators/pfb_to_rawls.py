import base64
import itertools
from typing import IO, Iterator, List, Set, Tuple

from app.db.model import Import
from app.external.rawls_entity_model import (AddListMember, AddUpdateAttribute,
                                             AttributeOperation,
                                             CreateAttributeValueList, Entity,
                                             EntityReference, RemoveAttribute)
from app.translators.translator import Translator
from pfb.reader import PFBReader


class PFBToRawls(Translator):
    def __init__(self, options=None):
        if options is None:
            options = {}
        defaults = {'b64-decode-enums': False, 'prefix-object-ids': True}
        self.options = {**defaults, **options}

    def translate(self, import_details: Import, file_like: IO) -> Iterator[Entity]:
        with PFBReader(file_like) as reader:
            schema = reader.schema
            enums = self.list_enums(schema)

            # You might think that the following is equivalent:
            # (self.translate_record(record,enums) for record in reader if record['name'] != 'Metadata')
            # But that fails, and this doesn't, possibly because something upstream is reading the generator
            # more than once.
            for record in reader:
                if record['name'] != 'Metadata':
                    yield self.translate_record(record, enums, import_details.filetype)

    def translate_record(self, record, enums, file_type) -> Entity:
        entity_type = record['name']
        name = record['id']

        def make_op(key, value) -> List[AttributeOperation]:
            if self.options['b64-decode-enums'] and (entity_type, key) in enums:
                value = self.b64_decode(value).decode("utf-8")
            if self.options['prefix-object-ids'] and key == 'object_id':
                value = 'drs://' + value
            if key == 'name':
                # with namespaces in place, why do we need the entity_type prefix here?
                # we won't remove it now so as to not break any compatibility
                key = file_type + ':' + entity_type + '_name'
            else:
                key = file_type + ':' + key

            if isinstance(value, list):
                ops = [AddListMember(key, v) for v in value]
                return [RemoveAttribute(key), CreateAttributeValueList(key), *ops]
            else:
                return [AddUpdateAttribute(key, value)]

        attributes = [make_op(key, value)
                      for key, value in record['object'].items() if value is not None]
        relations = [make_op(relation['dst_name'], EntityReference(entityType=relation['dst_name'], entityName=relation['dst_id']))
                     for relation in record['relations']]

        return Entity(name, entity_type, list(itertools.chain(*attributes, *relations)))

    @classmethod
    def b64_decode(cls, encoded_value):
        return base64.b64decode(encoded_value + "=" * (-len(encoded_value) % 4))

    @classmethod
    def list_enums(cls, schema) -> Set[Tuple[str, str]]:
        enums = {(entity_type['name'], field['name'])
                 for entity_type in schema
                 for field in entity_type['fields']
                 for enum in field['type'] if isinstance(enum, dict) and enum['type'] == 'enum'}
        return enums

