import json
import logging
from typing import List  # pylint: disable=unused-import
from typing import IO, Iterator

from app.external.rawls_entity_model import AttributeOperation  # pylint: disable=unused-import
from app.external.rawls_entity_model import (AddListMember, AddUpdateAttribute,
                                             CreateAttributeValueList, Entity,
                                             RemoveAttribute)
from app.external.tdr_manifest import TDRManifestParser
from app.translators.translator import Translator
import pyarrow.parquet as pq


class TDRManifestToRawls(Translator):
    def __init__(self, options=None):
        """Translator for Parquet files."""
        if options is None:
            options = {}
        # TODO AS-1037: set defaults for 'create references', 'use PK', etc?
        defaults = {}
        self.options = {**defaults, **options}

    def translate(self, file_like: IO, file_type: str) -> Iterator[Entity]:
        logging.info(f'executing a TDRManifestToRawls translation for {file_type}: {file_like}')
        # read and parse entire manifest file
        jso = json.load(file_like)
        tables = TDRManifestParser(jso).get_tables()

        recs = []

        # for each table in the snapshot model, build the Rawls entities
        for t in tables:
            ops = []  # type: List[AttributeOperation]
            ops.append(AddUpdateAttribute('tablename', t.name))
            ops.append(AddUpdateAttribute('primarykey', t.primary_key))

            # TODO: parse entities out of Parquet files.
            # get new pet token as necessary, since pet may have expired. May need to pass in the Import object
            # to this translate method in order to have all the info we need to get a pet.
            # parse all parquet files for this table into rawls json, add those attrs to the result. See line 59 for
            # identifying all the parquet files
            ops.append(RemoveAttribute('parquetFiles'))
            ops.append(CreateAttributeValueList('parquetFiles'))
            for f in t.parquet_files:
                ops.append(AddListMember('parquetFiles', f))

            recs.append(Entity(name=t.name, entityType='snapshottable', operations=ops))

        return iter(recs)

    @classmethod
    def convert_parquet_file_to_entity_attributes(cls, file_like: IO):
        """Converts single parquet file to [[AddUpdateAttribute, AddUpdateAttribute, ..],
        [AddUpdateAttribute, AddUpdateAttribute, ..]...] with each element in the outer list representing
        an enity/row's attributes.  For an entity type spanning multiple parquet files, calls to this
        method should be accumulated into a single list"""
        pq_table = pq.read_table(file_like)
        res = []
        # for the planned usage where we would be passing in a single parquet file at a time, I believe that
        # to_batches() will always return a single batch
        # if you do something like
        # dataset = pq.ParquetDataset('cell_suspension')
        # dataset.read().to_batches()
        # it will create one batch per file in the cell_suspension directory
        # collapse the last 4 lines to:
        # return cls.convert_parquet_batch(pq_table.to_batches()[0])
        # if we can confirm we'll always have one batch
        for b in pq_table.to_batches():
            res.extend(cls.convert_parquet_batch(b))
        return res

    @classmethod
    def convert_parquet_batch(cls, batch):
        dict_for_batch = batch.to_pydict()
        return [cls.convert_parquet_row(dict_for_batch, i) for i in range(0, batch.num_rows)]

    @classmethod
    def convert_parquet_row(cls, dict_for_batch, row_idx):
        # assumption that each row has entries for all columns, I think that's fair with parquet's columnar format?
        return [cls.convert_parquet_attr(name, dict_for_batch[name][row_idx]) for name in dict_for_batch.keys()]

    @classmethod
    def convert_parquet_attr(cls, name, vale):
        return AddUpdateAttribute(name, vale)
