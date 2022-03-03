import copy
import itertools
import json
import logging
import os
from typing import IO, Any, Dict, Iterator, List
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import pyarrow
import pyarrow.parquet as pq
from app.db import db
from app.db.model import Import
from app.external import gcs, sam
from app.external.rawls_entity_model import (AddListMember, AddUpdateAttribute,
                                             AttributeOperation,
                                             AttributeValue, CreateAttributeEntityReferenceList,
                                             CreateAttributeValueList, Entity,
                                             EntityReference, RemoveAttribute)
from app.external.tdr_manifest import TDRManifestParser, TDRTable
from app.translators.translator import Translator


class TDRManifestToRawls(Translator):
    def __init__(self, options=None):
        """Translator for TDR manifests."""
        if options is None:
            options = {}
        # do we need any options for import behavior, such as 'create references', 'use PK', 'resolve DRS', etc?
        defaults = {}
        self.options = {**defaults, **options}

    def translate(self, import_details: Import, file_like: IO) -> Iterator[Entity]:
        logging.info(f'{import_details.id} executing a TDRManifestToRawls translation for {import_details.filetype}: {file_like}')
        jso = json.load(file_like)
        parsed_manifest = TDRManifestParser(jso, import_details.id)
        source_snapshot_id = parsed_manifest.get_snapshot_id()

        ## Save the snapshot id, so we can sync permissions to it when we are notified of a successful import
        TDRManifestToRawls.save_snapshot_id(import_details.id, source_snapshot_id)

        tables = parsed_manifest.get_tables()
        return itertools.chain(*self.translate_tables(import_details, source_snapshot_id, tables))

    @classmethod
    def translate_tables(cls, import_details: Import, source_snapshot_id: str, tables: List[TDRTable]) -> Iterator[Iterator[Entity]]:
        """Converts a list of TDR tables, each of which contain urls to parquet files, to an iterator of Entity objects."""
        pet_key = sam.admin_get_pet_key(import_details.workspace_google_project, import_details.submitter)
        for t in tables:
            for f in t.parquet_files:
                pt = ParquetTranslator(t, f, import_details, source_snapshot_id, pet_key)
                yield pt.translate()

    @staticmethod
    def save_snapshot_id(import_id: str, snapshot_id: str):
        """Saves the snapshot id to the DB so we can use it later to sync permissions."""
        with db.session_ctx() as sess:
            update_successful = Import.save_snapshot_id_exclusively(import_id, snapshot_id, sess)

        if not update_successful:
            error_message = f'Failed to save snapshot id {snapshot_id} for import job {import_id}, which will prevent permissions syncing'
            logging.error(error_message)
            raise IOError(error_message)

class ParquetTranslator:
    def __init__(self, table: TDRTable, filelocation: str, import_details: Import, source_snapshot_id: str, auth_key: Dict[str, Any] = None):
        """Translator for Parquet files coming from a TDR manifest."""
        self.table = table
        self.import_details = import_details
        self.filelocation = filelocation
        self.auth_key = auth_key
        self.file_nickname = os.path.split(filelocation)[1]
        self.source_snapshot_id = source_snapshot_id

    def translate(self) -> Iterator[Entity]:
        """Converts a parquet file, represented as a url, to an iterator of Entity objects."""
        logging.info(f'{self.import_details.id} attempting parquet translation of {self.file_nickname} from {self.filelocation} ...')
        url = urlparse(self.filelocation)
        bucket = url.netloc
        path = url.path
        with gcs.open_file(self.import_details.workspace_google_project, bucket, path, self.import_details.submitter, self.auth_key) as pqfile:
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
            # we should never encounter a case where the primary key is missing, but let's be safe:
            entity_name = row.get(self.table.primary_key)
            if entity_name is None:
                logging.info(f'{self.import_details.id} found a row with no pk "{self.table.primary_key}" value; skipping this row: ${row}')
                continue
            else:
                yield Entity(str(entity_name), self.table.name, list(ops))

    def translate_parquet_row(self, row: pd.Series, column_names: List[str]) -> List[AttributeOperation]:
        """Convert a single row of a pandas dataframe - assumed from a Parquet file - to an Entity."""
        # annotate row with the timestamp of the import
        tsattr = self.translate_parquet_attr('import:timestamp', self.import_details.submit_time.isoformat())
        # annotate row with the snapshotid from TDR
        sourceidattr = self.translate_parquet_attr('import:snapshot_id', self.source_snapshot_id)

        all_attr_ops = [self.translate_parquet_attr(colname, row[colname]) for colname in column_names]
        return list(itertools.chain(*all_attr_ops, sourceidattr, tsattr))

    def translate_parquet_attr(self, name: str, value) -> List[AttributeOperation]:
        """Convert a single cell of a pandas dataframe - assumed from a Parquet file - to an AddUpdateAttribute."""
        # add attributes to the "tdr:" namespace if needed to avoid  conflicts, like 'name', which is reserved in Rawls
        usable_name = self.add_namespace_if_required(name)

        # Check if value is a reference, check if it's an array for finding ops
        is_reference = name in self.table.reference_attrs
        reference_target_type = self.table.reference_attrs.get(name, None)
        is_array = isinstance(value, np.ndarray)

        if not is_array:
            # most common case, results in AddUpdateAttribute
            return [AddUpdateAttribute(usable_name, self.create_attribute_value(value, is_reference, reference_target_type))]
        elif is_reference and is_array:
            # list of entity references, results in a CreateAttributeEntityReferenceList
            ops = [AddListMember(usable_name, self.create_attribute_value(v, is_reference, reference_target_type)) for v in value]
            return [RemoveAttribute(usable_name), CreateAttributeEntityReferenceList(usable_name), *ops]
        else: # elif not is_reference and is_array, adds a list of values (non-entity references)
            ops = [AddListMember(usable_name, self.create_attribute_value(v)) for v in value]
            return [RemoveAttribute(usable_name), CreateAttributeValueList(usable_name), *ops]

    @classmethod
    def create_attribute_value(self, value, is_reference: bool = False, reference_target_type = None) -> AttributeValue:
        if is_reference:
            return EntityReference(str(value), reference_target_type)
        elif isinstance(value, (int, float)):
            if np.isnan(value):
                return None
            return value
        elif value is None or isinstance(value, (str, bool)):
            return value
        else:
            # test if this is a numpy.ndarray member
            item_op = getattr(value, "item", None)
            if item_op is not None and callable(item_op):
                return self.create_attribute_value(value.item())
            else:
                # BigQuery/Parquet can contain datatypes that the Rawls model doesn't handle and/or are not
                # natively serializable into JSON, such as Timestamps. Inspect the types we know about,
                # and str() the rest of them.
                return str(value)

    # only add TDR namespace if needed. See this doc:
    # https://docs.google.com/document/d/1_dEbPtgF7eeYUNRFK6CDUqeGWtiE9ISeTlfjSEtl-FA
    def add_namespace_if_required(self, name: str) -> str:
        return f'tdr:{name}' if ParquetTranslator.prefix_required(name, self.table.name, self.table.primary_key) \
            else name

    @staticmethod
    def prefix_required(name: str, table_name: str, primary_key: str) -> bool:
        case_insensitive_name = name.lower()
        case_insensitive_primary_key = primary_key.lower() if primary_key is not None else None
        return case_insensitive_name == 'name' \
            or case_insensitive_name == 'entityType'.lower() \
            or (case_insensitive_name.endswith('_id') \
                and case_insensitive_name[:-3] == table_name.lower() \
                and case_insensitive_name != case_insensitive_primary_key)
