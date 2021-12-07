import logging
import json

from app.translators.translator import Translator
from typing import Iterator, Dict, Any

# TODO AS-1037: rename to e.g. TDRManifestToRawls?
class ParquetToRawls(Translator):
    def __init__(self, options=None):
        """Translator for Parquet files."""
        if options is None:
            options = {}
        # TODO AS-1037: set defaults for "create references", "use PK", etc?
        defaults = {}
        self.options = {**defaults, **options}

    def translate(self, file_like, file_type) -> Iterator[Dict[str, Any]]:
        logging.info(f"executing a ParquetToRawls translation for {file_type}: {file_like}")
        # read and parse entire manifest file
        jso = json.load(file_like)

        # extract table names from manifest
        recs = []
        ops = []
        for t in jso["tables"]:
            tdr_pk = t["primaryKey"]
            if tdr_pk is None:
                pk = "datarepo_row_id"
            elif isinstance(tdr_pk, list) and len(tdr_pk) == 1:
                pk = tdr_pk[0]
            else:
                pk = tdr_pk

            ops.append(self.make_add_update_op('table', t))
            ops.append(self.make_add_update_op('primarykey', pk))

            recs.append( {
                'name': t,
                'entityType': 'table',
                'operations': [*ops]
            } )

        # TODO AS-1037: create map of table names->parquet files
        # TODO AS-1037: build relationship graph, determine proper ordering for tables
        # TODO AS-1037: determine proper PK columns for each table
        # TODO AS-1037: in proper table order,
            # get new pet token as necessary
            # parse all parquet files for this table into rawls json, add to result iterator

        return iter(recs)

    # TODO: don't copy this wholesale from pfb_to_rawls; share!
    @classmethod
    def make_add_update_op(cls, key, value) -> Dict[str, str]:
        return {
            'op': 'AddUpdateAttribute',
            'attributeName': key,
            'addUpdateAttribute': value
        }
