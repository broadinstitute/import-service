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
from app.db.model import Import
from app.external import gcs, sam
from app.external.rawls_entity_model import (AddListMember, AddUpdateAttribute,
                                             AttributeOperation,
                                             AttributeValue,
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
        pet_key = sam.admin_get_pet_key(import_details.workspace_google_project, import_details.submitter)
        for t in tables:
            for f in t.parquet_files:
                pt = ParquetTranslator(t, f, import_details, pet_key)
                yield pt.translate()

class ParquetTranslator:
    def __init__(self, table: TDRTable, filelocation: str, import_details: Import, auth_key: Dict[str, Any] = {}):
        """Translator for Parquet files coming from a TDR manifest."""
        self.table = table
        self.import_details = import_details
        self.filelocation = filelocation
        self.auth_key = auth_key
        self.file_nickname = os.path.split(filelocation)[1]

    def translate(self) -> Iterator[Entity]:
        """Converts a parquet file, represented as a url, to an iterator of Entity objects."""
        logging.info(f'{self.import_details.id} attempting parquet translation of {self.file_nickname} from {self.filelocation} ...')
        url = urlparse(self.filelocation)
        bucket = url.netloc
        path = url.path
        # TODO AS-1073: the call to gcs.open_file will get a new pet key each time. This is overly aggressive; we could probably
        # reuse tokens to reduce API calls to Sam (and thus chances to fail). Ideally, when opening a file if we encounter
        # an auth error, we'd *then* get a new pet key and retry.
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
        # TODO AS-1041: append snapshotid and timestamp attributes, using a non-default namespace to avoid conflicts
        # we have the timestamp from the import_details object:
        # self.convert_parquet_attr('pfb:timestamp', self.import_details.submit_time)
        # but we don't currently have the snapshotid, you'll need to find a way to pass that info down to here
        # the snapshotid is available from TDRManifestParser.get_snapshot_id (which isn't available here)

        all_attr_ops = [self.translate_parquet_attr(colname, row[colname]) for colname in column_names]
        return list(itertools.chain(*all_attr_ops))

    def translate_parquet_attr(self, name: str, value) -> List[AttributeOperation]:
        """Convert a single cell of a pandas dataframe - assumed from a Parquet file - to an AddUpdateAttribute."""
        # {entity_type}_id is a reserved name. If the import contains a column named thusly,
        # move that column into the "import:" namespace to avoid conflicts
        if (name != f'{self.table.name}_id'):
            usable_name = name
        else:
            # TODO AS-1040: need to enable new namespaces in Rawls. As of this writing, Rawls only supports 'pfb', 'library', and 'tag'
            # in addition to the default namespace. For now, use the pfb namespace just so we can see it working
            usable_name = f'pfb:{name}'

        # TODO: AS-1038 detect/create references
        is_reference = False
        is_array = isinstance(value, np.ndarray)

        if not is_reference and not is_array:
            # most common case, results in AddUpdateAttribute
            return [AddUpdateAttribute(usable_name, self.create_attribute_value(value))]
        elif is_reference and is_array:
            # TODO: AS-1038 detect/create references
            # RemoveAttribute/CreateAttributeEntityReferenceList/AddListMember(s)
            return []
        elif is_array:
            ops = [AddListMember(usable_name, self.create_attribute_value(v)) for v in value]
            return [RemoveAttribute(usable_name), CreateAttributeValueList(usable_name), *ops]
        else: # elif is_reference:
            # TODO: AS-1038 detect/create references
            # AddUpdateAttribute(name, EntityReference(value))
            return []

    @classmethod
    def create_attribute_value(cls, value, is_reference: bool = False, reference_target_type = None) -> AttributeValue:
        if is_reference:
            return EntityReference(str(value), reference_target_type)
        elif isinstance(value, (str, int, float, bool)):
            return value
        else:
            # test if this is a numpy.ndarray member
            item_op = getattr(value, "item", None)
            if item_op is not None and callable(item_op):
                return cls.create_attribute_value(value.item())
            else:
                # BigQuery/Parquet can contain datatypes that the Rawls model doesn't handle and/or are not
                # natively serializable into JSON, such as Timestamps. Inspect the types we know about,
                # and str() the rest of them.
                return str(value)
