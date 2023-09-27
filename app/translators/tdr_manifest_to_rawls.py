import copy
import itertools
import json
import logging
import os
from typing import IO, Any, Dict, Iterator, List
from urllib.parse import urlparse
import io
import uuid

import numpy as np
import pandas as pd
import pyarrow
import pyarrow.parquet as pq

from app.auth.userinfo import UserInfo
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
from app.util import http, exceptions

VALID_AZURE_DOMAIN = "core.windows.net"
GOOGLE_STORAGE_DOMAIN = "storage.googleapis.com"

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
        return itertools.chain(*self.translate_tables(import_details, source_snapshot_id, tables, parsed_manifest.is_cyclical()))

    @classmethod
    def translate_tables(cls, import_details: Import, source_snapshot_id: str, tables: List[TDRTable], is_cyclical: bool) -> Iterator[Iterator[Entity]]:
        """Converts a list of TDR tables, each of which contain urls to parquet files, to an iterator of Entity objects."""
        pet_key = sam.admin_get_pet_key(import_details.workspace_google_project, import_details.submitter)
        if not is_cyclical:
            yield from itertools.chain(TDRManifestToRawls.translate_table_parquet_files(import_details, source_snapshot_id, tables, False, pet_key, False))
        else:
            yield from itertools.chain(TDRManifestToRawls.translate_table_parquet_files(import_details, source_snapshot_id, tables, True, pet_key, False), TDRManifestToRawls.translate_table_parquet_files(import_details, source_snapshot_id, tables, True, pet_key, True))
    @classmethod
    def translate_table_parquet_files(cls, import_details: Import, source_snapshot_id: str, tables: List[TDRTable],
                                      is_cyclical: bool, pet_key: Dict[str, Any], translate_ref: bool) -> Iterator[Iterator[Entity]]:
        """Converts only the ref/non_ref attributes from a list of TDR tables to an iterator of Entity objects."""
        for t in tables:
            for f in t.parquet_files:
                pt = ParquetTranslator(t, f, import_details, source_snapshot_id, pet_key, is_cyclical)
                yield pt.translate(translate_ref)

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
    def __init__(self, table: TDRTable, filelocation: str, import_details: Import, source_snapshot_id: str, auth_key: Dict[str, Any] = None, is_cyclical: bool = False):  # type: ignore
        """Translator for Parquet files coming from a TDR manifest."""
        self.table = table
        self.import_details = import_details
        self.filelocation = filelocation
        self.auth_key = auth_key
        self.file_nickname = os.path.split(filelocation)[1]
        self.source_snapshot_id = source_snapshot_id
        self.is_cyclical = is_cyclical

    def translate(self, ref_only: bool = False) -> Iterator[Entity]:
        """Converts a parquet file, represented as a url, to an iterator of Entity objects."""
        logging.info(f'{self.import_details.id} attempting parquet translation of {self.file_nickname} from {self.filelocation} ...')
        parsedurl = urlparse(self.filelocation)
        user_info = UserInfo("---", self.import_details.submitter, True)
        if (parsedurl.scheme == 'gs'):
            bucket = parsedurl.netloc
            path = parsedurl.path
            with gcs.open_file(self.import_details.workspace_google_project, bucket, path, self.import_details.submitter, self.auth_key) as pqfile:
                return self.translate_parquet_file_to_entities(pqfile, is_https=False, is_azure=False, ref_only=ref_only)
        elif (parsedurl.scheme == 'https'):
            hostname = parsedurl.netloc
            if not (hostname.endswith(VALID_AZURE_DOMAIN) or hostname == GOOGLE_STORAGE_DOMAIN):
                logging.error(f"unsupported domain in url {self.filelocation} provided")
                raise exceptions.InvalidPathException(self.filelocation, user_info, "Unsupported domain")
            with http.http_as_filelike(self.filelocation) as pqfile:
                return self.translate_parquet_file_to_entities(pqfile, is_https=True, is_azure=hostname.endswith(VALID_AZURE_DOMAIN), ref_only=ref_only)
        else:
            logging.error(f"unsupported scheme {parsedurl.scheme} provided")
            raise exceptions.InvalidPathException(self.filelocation, user_info, "Unsupported scheme")

    # investigate parquet streaming, instead of reading the whole file into memory
    # BUT, it looks like the export files use random-access binary format, not streaming format,
    # so streaming is not possible.
    #
    # with pyarrow.ipc.open_stream(file_like) as reader:
    #     assert reader.schema == 'foo'
    #     entity_batches = (self.translate_data_frame(b.to_pandas(), b.column_names) for b in reader)
    #     return itertools.chain(*entity_batches)
    def translate_parquet_file_to_entities(self, file_like: IO, is_azure: bool = False, is_https: bool = False, ref_only: bool = False) -> Iterator[Entity]:
        """Converts single parquet file-like object to an iterator of Entity objects."""
        pq_table: pyarrow.Table
        if (is_azure or is_https):
            # We need to read and wrap the bytes of the parquet file in a BytesIO object which implements methods required by the parquet reader
            pq_table = pq.read_table(io.BytesIO(file_like.read()))
        else:
            pq_table = pq.read_table(file_like)

        column_names = copy.deepcopy(pq_table.column_names)
        # see https://arrow.apache.org/docs/python/pandas.html#reducing-memory-use-in-table-to-pandas for discussion of
        # memory-reducing options
        df: pd.DataFrame = pq_table.to_pandas(split_blocks=True, self_destruct=True)
        del pq_table
        return self.translate_data_frame(df, column_names, is_azure, ref_only)

    def translate_data_frame(self, df: pd.DataFrame, column_names: List[str], is_azure: bool, ref_only: bool = False) -> Iterator[Entity]:
        """Convert a pandas dataframe - assumed from a Parquet file - to an iterator of Entity objects."""
        logging.info(f'{self.import_details.id} expecting {len(df.index)} rows in {self.file_nickname} ...')
        array_fields = [c.name for c in list(filter(lambda c: c.array_of, self.table.columns))]
        for _, row in df.iterrows():
            ops = self.translate_parquet_row(row, column_names, is_azure, array_fields, ref_only)
            # we should never encounter a case where the primary key is missing, but let's be safe:
            entity_name = row.get(self.table.primary_key)
            if entity_name is None:
                logging.info(f'{self.import_details.id} found a row with no pk "{self.table.primary_key}" value; skipping this row: ${row}')
                continue
            else:
                yield Entity(str(entity_name), self.table.name, list(ops))

    def translate_parquet_row(self, row: pd.Series, column_names: List[str], is_azure: bool, array_fields: List[str], ref_only: bool = False) -> List[AttributeOperation]:
        """Convert a single row of a pandas dataframe - assumed from a Parquet file - to an Entity."""
        all_attr_ops = []
        if not self.is_cyclical or not ref_only:
            # annotate row with the timestamp of the import
            tsattr = self.translate_parquet_attr('import:timestamp', self.import_details.submit_time.isoformat())
            # annotate row with the snapshotid from TDR
            sourceidattr = self.translate_parquet_attr('import:snapshot_id', self.source_snapshot_id)
            all_attr_ops.extend([tsattr, sourceidattr])

        for colname in column_names:
            value = row[colname]
            if is_azure is True:
                # In Azure parquet files, the datarepo_row_id field is stored as bytes so we should convert to string
                if colname == 'datarepo_row_id':
                    value = str(uuid.UUID(bytes=value))
                # In Azure parquet files, array fields are stored as stringified Json arrays that we should convert to arrays
                if colname in array_fields and value is not None and value != "":
                    try:
                        value = json.loads(value)
                    except json.JSONDecodeError:
                        logging.warning(f"Couldn't parse value {value}")

            # For cyclical tables, we are either processing only the reference attributes or only the non-reference attributes at a time
            if self.is_cyclical and (colname in self.table.reference_attrs) != ref_only:
                continue
            else:
                all_attr_ops.append(self.translate_parquet_attr(colname, value))
        return list(itertools.chain(*all_attr_ops))

    def translate_parquet_attr(self, name: str, value) -> List[AttributeOperation]:
        """Convert a single cell of a pandas dataframe - assumed from a Parquet file - to an AddUpdateAttribute."""

        # Don't add an attribute if it's the primary key and it has the same name as {tableName}_id
        if ParquetTranslator.attribute_should_be_skipped(name, self.table.primary_key, self.table.name): return []

        # add attributes to the "tdr:" namespace if needed to avoid  conflicts, like 'name', which is reserved in Rawls
        usable_name = self.add_namespace_if_required(name)

        # Check if value is a reference, check if it's an array for finding ops
        is_reference = name in self.table.reference_attrs
        reference_target_type = self.table.reference_attrs.get(name, None)
        is_array = isinstance(value, np.ndarray) or isinstance(value, list)

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
        return f'tdr:{name}' if ParquetTranslator.prefix_required(name, self.table.name) \
            else name

    @staticmethod
    def prefix_required(name: str, table_name: str) -> bool:
        case_insensitive_name = name.lower()
        return case_insensitive_name == 'name' \
            or case_insensitive_name == 'entityType'.lower() \
            or (case_insensitive_name.endswith('_id') \
                and case_insensitive_name[:-3] == table_name.lower())

    @staticmethod
    def attribute_should_be_skipped(name: str, primary_key: str, table_name: str) -> bool:
        case_insensitive_name = name.lower()
        case_insensitive_primary_key = primary_key.lower() if primary_key is not None else None
        return case_insensitive_name == case_insensitive_primary_key \
            and case_insensitive_name.endswith('_id') \
            and case_insensitive_name[:-3] == table_name.lower()

