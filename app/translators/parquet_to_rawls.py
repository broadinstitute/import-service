import logging

from app.translators.translator import Translator
from typing import Iterator, Dict, Any

SAMPLE_ENTITY = {
    'name': 'tdrexport',
    'entityType': 'tdrexport',
    'operations': [
        {
            'op': 'AddUpdateAttribute',
            'attributeName': 'description',
            'addUpdateAttribute': 'this is a hardcoded entity and attribute to test the snapshot import flow'
        }
    ]
}

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
        logging.info(f"executing a no-op ParquetToRawls translation for {file_type}: {file_like}")
        # TODO AS-1037: read and parse entire manifest file
        # TODO AS-1037: create map of table names->parquet files
        # TODO AS-1037: use TDR client lib model classes to hold snapshot description (for table PKs, relationships)
        # TODO AS-1037: build relationship graph, determine proper ordering for tables
        # TODO AS-1037: determine proper PK columns for each table
        # TODO AS-1037: in proper table order,
            # get new pet token as necessary
            # parse all parquet files for this table into rawls json, add to result iterator

        return iter([SAMPLE_ENTITY])
