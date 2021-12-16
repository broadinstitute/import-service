import json
import logging
from typing import List  # pylint: disable=unused-import
from typing import IO, Iterator
from urllib.parse import urlparse

import pyarrow

from app.db.model import Import
from app.external import gcs
from app.external.rawls_entity_model import AttributeOperation  # pylint: disable=unused-import
from app.external.rawls_entity_model import (AddListMember, AddUpdateAttribute,
                                             CreateAttributeValueList, Entity,
                                             RemoveAttribute)
from app.external.tdr_manifest import TDRManifestParser
from app.translators.translator import Translator

import pandas as pd
import pyarrow.parquet as pq


class TDRManifestToRawls(Translator):
    def __init__(self, options=None):
        """Translator for Parquet files."""
        if options is None:
            options = {}
        # TODO AS-1037: set defaults for 'create references', 'use PK', etc?
        defaults = {}
        self.options = {**defaults, **options}

    def translate(self, import_details: Import, file_like: IO, file_type: str) -> Iterator[Entity]:
        logging.info(f'{import_details.id} executing a TDRManifestToRawls translation for {file_type}: {file_like}')
        # read and parse entire manifest file
        jso = json.load(file_like)
        tables = TDRManifestParser(jso).get_tables()

        recs = []

        # for each table in the snapshot model, build the Rawls entities
        for t in tables:
            # ops = []  # type: List[AttributeOperation]
            # ops.append(AddUpdateAttribute('tablename', t.name))
            # ops.append(AddUpdateAttribute('primarykey', t.primary_key))

            # TODO: parse entities out of Parquet files.
            # get new pet token as necessary, since pet may have expired. May need to pass in the Import object
            # to this translate method in order to have all the info we need to get a pet.
            # parse all parquet files for this table into rawls json, add those attrs to the result. See line 59 for
            # identifying all the parquet files
            # ops.append(RemoveAttribute('parquetFiles'))
            # ops.append(CreateAttributeValueList('parquetFiles'))
            for f in t.parquet_files:
                recs.append(self.translate_parquet_file(import_details, f, t.name, t.primary_key))
                # with gcs.open_file(import_details.workspace_google_project, 'bucket', 'path', import_details.submitter) as pqfile:
                # ops.append(AddListMember('parquetFiles', f))

            # recs.append(Entity(name=t.name, entityType='snapshottable', operations=ops))

        return iter(recs)

    def translate_parquet_file(self, import_details: Import, filelocation: str, entity_type: str, pk: str):
        logging.info(f'{import_details.id} attempting parquet export file {filelocation} ...')
        url = urlparse(filelocation)
        bucket = url.netloc
        path = url.path
        with gcs.open_file(import_details.workspace_google_project, bucket, path, import_details.submitter) as pqfile:
            return list(self.convert_parquet_file_to_entity_attributes(pqfile, entity_type, pk))


    def convert_parquet_file_to_entity_attributes(self, file_like: IO, entity_type: str, pk: str):
        """Converts single parquet file to [[AddUpdateAttribute, AddUpdateAttribute, ..],
        [AddUpdateAttribute, AddUpdateAttribute, ..]...] with each element in the outer list representing
        an enity/row's attributes.  For an entity type spanning multiple parquet files, calls to this
        method should be accumulated into a single list"""
        
        # with pa.ipc.open_stream(file_like) as pqreader:
        #     schema = pqreader.schema
        #     for b in pqreader:
        #         print(f'heres a batch, it has length {len(b)}')
                # self.convert_parquet_batch(b)
        
        pq_table: pyarrow.Table = pq.read_table(file_like)

        df = pq_table.to_pandas()
        for index, row in df.iterrows():
            ops = self.convert_parquet_row(row, pq_table.column_names)
            # TODO: better primary key detection
            yield Entity(row[pk], entity_type, list(ops))

    def convert_parquet_row(self, row, column_names):
        for colname in column_names:
            yield self.convert_parquet_attr(colname, row[colname])

    def convert_parquet_attr(self, name, value):
        return AddUpdateAttribute(name, value)
