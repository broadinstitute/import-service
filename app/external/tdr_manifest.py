from dataclasses import dataclass
from typing import Dict, List

from app.external import JSON
from app.external.rawls_entity_model import EntityReference


@dataclass
class TDRTable:
    name: str
    primary_key: str
    parquet_files: List[str]
    reference_attrs: Dict[str, EntityReference] # column name -> reference


class TDRManifestParser:
    def __init__(self, jso: JSON):
        self._tables = self._parse(jso)
        self._snapshotid = jso['snapshot']['id']
        self._snapshotname = jso['snapshot']['name']

    def get_tables(self) -> List[TDRTable]:
        return self._tables

    def get_snapshot_id(self) -> str:
        return self._snapshotid

    def get_snapshot_name(self) -> str:
        return self._snapshotname

    def _parse(self, jso: JSON) -> List[TDRTable]:
        # the snapshot model: table names, primary keys, relationships
        snapshot = jso['snapshot']
        # the parquet export files
        parquet_location = jso['format']['parquet']['location']

        # build dict of table->parquet files for the exports
        exports = dict(map(lambda e: (e['name'], e['paths']), parquet_location['tables']))

        # TODO AS-1036: build relationship graph, determine proper ordering for tables.
        # graphlib.TopologicalSorter should do it, based on snapshot relationships

        tables = []

        # for each table in the snapshot model, extract the table name and primary key
        # TODO AS-1044: loop through tables in the order determined by AS-1036, so we
        # import dependants before the tables that depend on them
        for t in snapshot['tables']:
            table_name = t['name']

            # don't bother dealing with this table if it has no exported parquet files
            if table_name in exports:
                # extract primary key
                tdr_pk = t['primaryKey']
                if tdr_pk is None:
                    pk = 'datarepo_row_id'
                elif not isinstance(tdr_pk, list):
                    pk = tdr_pk
                elif isinstance(tdr_pk, list) and len(tdr_pk) == 1:
                    pk = tdr_pk[0]
                else:
                    pk = 'datarepo_row_id'

                # TODO AS-1036: from the "relationships" key in the manifest, save the valid reference attributes
                # to reference_attrs. A reference is valid iif the "to" column is the primary key of the "to" table.

                table = TDRTable(name=table_name, primary_key=pk, parquet_files=exports[table_name], reference_attrs={})
                tables.append(table)
        
        return tables

