import copy
import itertools
import json
import logging
import os
from typing import IO, Iterator, List
from urllib.parse import urlparse

import pandas as pd
import pyarrow
import pyarrow.parquet as pq
from app.db.model import Import
from app.external import gcs
from app.external.rawls_entity_model import (AddUpdateAttribute,
                                             AttributeValue, Entity)
from app.external.tdr_manifest import TDRManifestParser, TDRTable
from app.translators.translator import Translator


class TDRManifestToRawls(Translator):
    def __init__(self, options=None):
        """Translator for TDR manifests."""
        if options is None:
            options = {}
        # TODO AS-1037: set defaults for 'create references', 'use PK', etc?
        defaults = {}
        self.options = {**defaults, **options}

    def translate(self, import_details: Import, file_like: IO) -> Iterator[Entity]:
        logging.info(f'{import_details.id} executing a TDRManifestToRawls translation for {import_details.filetype}: {file_like}')
        tables = self.get_tables(file_like)
        return itertools.chain(*self.translate_tables(import_details, tables))

    @classmethod
    def get_tables(cls, file_like: IO) -> List[TDRTable]:
        # read and parse entire manifest file
        jso = json.load(file_like)
        return TDRManifestParser(jso).get_tables()

    @classmethod
    def translate_tables(cls, import_details: Import, tables: List[TDRTable]) -> Iterator[Iterator[Entity]]:
        """Converts a list of TDR tables, each of which contain urls to parquet files, to an iterator of Entity objects."""
        for t in tables:
            for f in t.parquet_files:
                pt = ParquetTranslator(t, f, import_details)
                yield pt.translate()

class ParquetTranslator:
    def __init__(self, table: TDRTable, filelocation: str, import_details: Import):
        """Translator for Parquet files coming from a TDR manifest."""
        self.table = table
        self.import_details = import_details
        self.filelocation = filelocation
        self.file_nickname = os.path.split(filelocation)[1]

    def translate(self) -> Iterator[Entity]:
        """Converts a parquet file, represented as a url, to an iterator of Entity objects."""
        logging.info(f'{self.import_details.id} attempting parquet translation of {self.file_nickname} from {self.filelocation} ...')
        url = urlparse(self.filelocation)
        bucket = url.netloc
        path = url.path
        # TODO: the call to gcs.open_file will get a new pet key each time. This is overly aggressive; we could probably
        # reuse tokens to reduce API calls to Sam (and thus chances to fail). Ideally, when opening a file if we encounter
        # an auth error, we'd *then* get a new pet key and retry.
        with gcs.open_file(self.import_details.workspace_google_project, bucket, path, self.import_details.submitter) as pqfile:
            return self.translate_parquet_file_to_entities(pqfile)

    # investigate parquet streaming, instead of reading the whole file into memory
    # BUT, it looks like the export files use random-access binary format, not streaming format,
    # so streaming is not possible.
    #
    # with pyarrow.ipc.open_stream(file_like) as reader:
    #     assert reader.schema == 'foo'
    #     entity_batches = (self.translate_data_frame(b.to_pandas(), b.column_names) for b in reader)
    #     return itertools.chain(*entity_batches)
    def translate_parquet_file_to_entities(self, file_like: IO) -> Iterator[Entity]:
        """Converts single parquet file-like object to an iterator of Entity objects."""
        pq_table: pyarrow.Table = pq.read_table(file_like)
        column_names = copy.deepcopy(pq_table.column_names)
        # see https://arrow.apache.org/docs/python/pandas.html#reducing-memory-use-in-table-to-pandas for discussion of
        # memory-reducing options
        df: pd.DataFrame = pq_table.to_pandas(split_blocks=True, self_destruct=True)
        del pq_table
        return self.translate_data_frame(df, column_names)

    def translate_data_frame(self, df: pd.DataFrame, column_names: List[str]) -> Iterator[Entity]:
        """Convert a pandas dataframe - assumed from a Parquet file - to an iterator of Entity objects."""
        logging.info(f'{self.import_details.id} expecting {len(df.index)} rows in {self.file_nickname} ...')
        for _, row in df.iterrows():
            ops = self.translate_parquet_row(row, column_names)
            # TODO: better primary key detection/resilience?
            yield Entity(row[self.table.primary_key], self.table.name, list(ops))

    def translate_parquet_row(self, row: pd.Series, column_names: List[str]) -> Iterator[AddUpdateAttribute]:
        """Convert a single row of a pandas dataframe - assumed from a Parquet file - to an Entity."""
        # TODO: AS-1041 append import:snapshotid and import:timestamp attributes
        # self.convert_parquet_attr('pfb:timestamp', self.import_details.submit_time)

        for colname in column_names:
            yield self.translate_parquet_attr(colname, row[colname])

    def translate_parquet_attr(self, name: str, value: AttributeValue):
        """Convert a single cell of a pandas dataframe - assumed from a Parquet file - to an AddUpdateAttribute."""
        # {entity_type}_id is a reserved name. If the import contains a column named thusly,
        # move that column into the "import:" namespace to avoid conflicts
        if (name != f'{self.table.name}_id'):
            usable_name = name
        else:
            # TODO: need to enable new namespaces in Rawls. As of this writing, Rawls only supports 'pfb', 'library', and 'tag'
            # in addition to the default namespace. For now, use the pfb namespace just so we can see it working
            usable_name = f'pfb:{name}'

        # BigQuery/Parquet can contain datatypes that the Rawls model doesn't handle and/or are not
        # natively serializable into JSON, such as Timestamps. Inspect the types we know about,
        # and str() the rest of them.
        # TODO: AS-1038 if this cell should be a reference, create as a EntityReference instead.
        # TODO: if this cell is an array, create as RemoveAttribute/CreateAttributeValueList/AddListMember(s) instead
        if (isinstance(value, (str, int, float, bool))):
            usable_value = value
        else:
            usable_value = str(value)

        return AddUpdateAttribute(usable_name, usable_value)
