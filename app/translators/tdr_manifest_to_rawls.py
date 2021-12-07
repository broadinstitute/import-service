import json
import logging
from typing import Any, Dict, Iterator

from app.external import rawls
from app.external.rawls_entity_model import AddUpdateAttribute, Entity
from app.translators.translator import Translator


class TDRManifestToRawls(Translator):
    def __init__(self, options=None):
        """Translator for Parquet files."""
        if options is None:
            options = {}
        # TODO AS-1037: set defaults for "create references", "use PK", etc?
        defaults = {}
        self.options = {**defaults, **options}

    def translate(self, file_like, file_type) -> Iterator[Entity]:
        logging.info(f"executing a TDRManifestToRawls translation for {file_type}: {file_like}")
        # read and parse entire manifest file
        jso = json.load(file_like)

        # TODO: the actual TDR manifest file does not match the code below. When
        # the TDR exports are ready, update this code to reflect the actual file structure.

        # extract table names from manifest
        recs = []
        ops = []
        for t in jso["tables"]:
            tdr_pk = t["primaryKey"]
            if tdr_pk is None:
                pk = "datarepo_row_id"
            elif not isinstance(tdr_pk, list):
                pk = tdr_pk
            elif isinstance(tdr_pk, list) and len(tdr_pk) == 1:
                pk = tdr_pk[0]
            else:
                pk = "datarepo_row_id"

            ops.append(AddUpdateAttribute('tablename', t))
            ops.append(AddUpdateAttribute('primarykey', pk))

            recs.append(Entity(t, 'snapshottable', ops))

        # TODO AS-1059: create map of table names->parquet files
        # TODO AS-1036: build relationship graph, determine proper ordering for tables
        # TODO AS-1037: in proper table order,
            # get new pet token as necessary
            # parse all parquet files for this table into rawls json, add to result iterator

        return iter(recs)

