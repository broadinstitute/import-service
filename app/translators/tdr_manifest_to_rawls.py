import json
import logging
from typing import IO, Iterator, List

from app.external.rawls_entity_model import (AddListMember, AddUpdateAttribute,
                                             AttributeOperation,
                                             CreateAttributeValueList, Entity,
                                             RemoveAttribute)
from app.translators.translator import Translator


class TDRManifestToRawls(Translator):
    def __init__(self, options=None):
        """Translator for Parquet files."""
        if options is None:
            options = {}
        # TODO AS-1037: set defaults for 'create references', 'use PK', etc?
        defaults = {}
        self.options = {**defaults, **options}

    def translate(self, file_like: IO, file_type: str) -> Iterator[Entity]:
        logging.info(f'executing a TDRManifestToRawls translation for {file_type}: {file_like}')
        # read and parse entire manifest file
        jso = json.load(file_like)

        snapshot = jso['snapshot'] # the snapshot model: table names, primary keys, relationships
        format = jso['format']['parquet']['location'] # the parquet export files

        # build dict of table->parquet files for the exports
        exports = dict(map(lambda e: (e['name'], e['paths']), format['tables']))

        recs = []
        ops = [] # type: List[AttributeOperation]

        # for each table in the snapshot model, extract the table name and primary key
        for t in snapshot['tables']:
            table_name = t['name']

            tdr_pk = t['primaryKey']
            if tdr_pk is None:
                pk = 'datarepo_row_id'
            elif not isinstance(tdr_pk, list):
                pk = tdr_pk
            elif isinstance(tdr_pk, list) and len(tdr_pk) == 1:
                pk = tdr_pk[0]
            else:
                pk = 'datarepo_row_id'

            ops.append(AddUpdateAttribute('tablename', table_name))
            ops.append(AddUpdateAttribute('primarykey', pk))

            # if this table name also exists as exported parquet files, grab those too
            if table_name in exports:
                ops.append(RemoveAttribute('parquetFiles'))
                ops.append(CreateAttributeValueList('parquetFiles'))
                for f in exports[table_name]:
                    ops.append(AddListMember('parquetFiles', f))

            recs.append(Entity(t, 'snapshottable', ops))

        # TODO AS-1036: build relationship graph, determine proper ordering for tables
        # TODO AS-1037: in proper table order,
            # get new pet token as necessary, since pet may have expired
            # parse all parquet files for this table into rawls json, add those attrs to the result

        return iter(recs)

