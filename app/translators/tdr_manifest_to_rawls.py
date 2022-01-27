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
import pyarrow.dataset as ds
import pyarrow.parquet as pq
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
        fs = gcs.get_gcs_filesystem(self.import_details.workspace_google_project, self.import_details.submitter, self.auth_key)
        return self.translate_parquet_file_to_entities(gs, bucket,  path)

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
        pq_dataset: pyarrow.Dataset = ds.dataset(pq_table)
        column_names = copy.deepcopy(pq_dataset.schema.names)

        # since max file size is 1 GB for parquet files exported from big query, investigate breaking
        # parquet files up. even with batches, the full parquet file is read into memory. We use batches
        # because this limits the number of rows we expand into a pandas df at once
        batches = pq_dataset.scanner(batch_size=1000).to_batches()

        for batch in batches:
            df: pd.DataFrame = batch.to_pandas(split_blocks=True, self_destruct=True)
            del batch
            yield from self.translate_data_frame(df, column_names)

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
        tsattr = self.translate_parquet_attr('timestamp', self.import_details.submit_time.isoformat(), 'import')
        # annotate row with the snapshotid from TDR
        sourceidattr = self.translate_parquet_attr('snapshot_id', self.source_snapshot_id, 'import')

        all_attr_ops = [self.translate_parquet_attr(colname, row[colname]) for colname in column_names]
        return list(itertools.chain(*all_attr_ops, sourceidattr, tsattr))

    def translate_parquet_attr(self, name: str, value, namespace: str = "tdr") -> List[AttributeOperation]:
        """Convert a single cell of a pandas dataframe - assumed from a Parquet file - to an AddUpdateAttribute."""
        # add all attributes to the "tdr:" namespace to avoid  conflicts,
        # e.g. with {entity_type}_id which is a is a reserved name in Rawls.
        usable_name = f'{namespace}:{name}'

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
