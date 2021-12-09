import json
import logging
from typing import List  # pylint: disable=unused-import
from typing import IO, Iterator

from app.external.rawls_entity_model import AttributeOperation  # pylint: disable=unused-import
from app.external.rawls_entity_model import (AddListMember, AddUpdateAttribute,
                                             CreateAttributeValueList, Entity,
                                             RemoveAttribute)
from app.external.tdr_manifest import TDRManifestParser
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
        tables = TDRManifestParser(jso).get_tables()

        recs = []
        ops = [] # type: List[AttributeOperation]

        # for each table in the snapshot model, build the Rawls entities
        for t in tables:
            ops.append(AddUpdateAttribute('tablename', t.name))
            ops.append(AddUpdateAttribute('primarykey', t.primary_key))

            # TODO: parse entities out of Parquet files.
            # get new pet token as necessary, since pet may have expired. May need to pass in the Import object
            # to this translate method in order to have all the info we need to get a pet.
            # parse all parquet files for this table into rawls json, add those attrs to the result. See line 59 for
            # identifying all the parquet files
            ops.append(RemoveAttribute('parquetFiles'))
            ops.append(CreateAttributeValueList('parquetFiles'))
            for f in t.parquet_files:
                ops.append(AddListMember('parquetFiles', f))

            recs.append(Entity(name=t.name, entityType='snapshottable', operations=ops))

        return iter(recs)
