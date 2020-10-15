import base64
from app.translators.translator import Translator
from pfb.reader import PFBReader
from typing import Iterator, Dict, Set, Tuple, Any


class PFBToRawls(Translator):
    def __init__(self, options=None):
        if options is None:
            options = {}
        defaults = {'b64-decode-enums': False, 'prefix-object-ids': True}
        self.options = {**defaults, **options}

    def translate(self, file_like, file_type) -> Iterator[Dict[str, Any]]:
        with PFBReader(file_like) as reader:
            schema = reader.schema
            enums = self.list_enums(schema)

            # You might think that the following is equivalent:
            # (self.translate_record(record,enums) for record in reader if record['name'] != 'Metadata')
            # But that fails, and this doesn't, possibly because something upstream is reading the generator
            # more than once.
            for record in reader:
                if record['name'] != 'Metadata':
                    yield self.translate_record(record, enums, file_type)

    def translate_record(self, record, enums, file_type) -> Dict[str, Any]:
        entity_type = record['name']
        name = record['id']

        def make_op(key, value):
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
            return self.make_add_update_op(key, value)

        attributes = [make_op(key, value)
                      for key, value in record['object'].items() if value is not None]
        relations = [make_op(relation['dst_name'],
                             {'entityType': relation['dst_name'], 'entityName': relation['dst_id']})
                     for relation in record['relations']]

        return {
            'name': name,
            'entityType': entity_type,
            'operations': [*attributes, *relations]
        }

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

    @classmethod
    def make_add_update_op(cls, key, value) -> Dict[str, str]:
        return {
            'op': 'AddUpdateAttribute',
            'attributeName': key,
            'addUpdateAttribute': value
        }

