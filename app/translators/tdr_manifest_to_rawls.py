import itertools
import json
import logging
from typing import List  # pylint: disable=unused-import
from typing import IO, Iterator
from urllib.parse import urlparse

import pyarrow

from app.db.model import Import
from app.external import gcs
from app.external.rawls_entity_model import AttributeOperation, AttributeValue  # pylint: disable=unused-import
from app.external.rawls_entity_model import (AddListMember, AddUpdateAttribute,
                                             CreateAttributeValueList, Entity,
                                             RemoveAttribute)
from app.external.tdr_manifest import TDRManifestParser, TDRTable
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
        return itertools.chain(*self.translate_tables(import_details, tables))

    def translate_tables(self, import_details: Import, tables: List[TDRTable]) -> Iterator[Iterator[Entity]]:
        """Converts a list of TDR tables, each of which contain urls to parquet files, to an iterator of Entity objects"""
        for t in tables:
            for f in t.parquet_files:
                yield self.translate_parquet_file(import_details, f, t.name, t.primary_key)

    def translate_parquet_file(self, import_details: Import, filelocation: str, entity_type: str, pk: str) -> Iterator[Entity]:
        """Converts a parquet file, represented as a url, to an iterator of Entity objects"""
        logging.info(f'{import_details.id} attempting parquet export file {filelocation} ...')
        url = urlparse(filelocation)
        bucket = url.netloc
        path = url.path
        # TODO: the call to gcs.open_file will get a new pet key each time. This is overly aggressive; we could probably
        # reuse tokens to reduce API calls to Sam (and thus chances to fail). Ideally, when opening a file if we encounter
        # an auth error, we'd *then* get a new pet key and retry.
        with gcs.open_file(import_details.workspace_google_project, bucket, path, import_details.submitter) as pqfile:
            return self.convert_parquet_file_to_entities(pqfile, entity_type, pk)

    def convert_parquet_file_to_entities(self, file_like: IO, entity_type: str, pk: str) -> Iterator[Entity]:
        """Converts single parquet file-like object to an iterator of Entity objects"""
        # TODO: investigate parquet streaming, instead of reading the whole file into memory        
        pq_table: pyarrow.Table = pq.read_table(file_like)
        df: pd.DataFrame = pq_table.to_pandas()
        return self.translate_data_frame(df, pq_table.column_names, entity_type, pk)

    def translate_data_frame(self, df: pd.DataFrame, column_names: List[str], entity_type: str, pk: str) -> Iterator[Entity]:
        """convert a pandas dataframe - assumed from a Parquet file - to an iterator of Entity objects"""
        for index, row in df.iterrows():
            ops = self.convert_parquet_row(row, column_names, entity_type, pk)
            # TODO: better primary key detection/resilience?
            yield Entity(row[pk], entity_type, list(ops))
    
    def convert_parquet_row(self, row: pd.Series, column_names: List[str], entity_type: str, pk: str) -> Iterator[AddUpdateAttribute]:
        """convert a single row of a pandas dataframe - assumed from a Parquet file - to an Entity"""
        # TODO: AS-1041 append import:snapshotid and import:timestamp attributes
        for colname in column_names:
            yield self.convert_parquet_attr(colname, row[colname], entity_type, pk)

    def convert_parquet_attr(self, name: str, value: AttributeValue, entity_type: str, pk: str):
        """convert a single cell of a pandas dataframe - assumed from a Parquet file - to an AddUpdateAttribute"""
        # TODO: AS-1038 if this cell should be a reference, create as a EntityReference instead.
        # TODO: if this cell is an array, create as RemoveAttribute/CreateAttributeValueList/AddListMember(s) instead
        if (name != f'{entity_type}_id'):
            usable_name = name
        else:
            usable_name = f'import:{name}'

        if (isinstance(value, (str, int, float, bool))):
            usable_value = value
        else:
            usable_value = str(value)
        return AddUpdateAttribute(usable_name, usable_value)
