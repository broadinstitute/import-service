from collections import defaultdict
from dataclasses import dataclass
from graphlib import TopologicalSorter
from typing import Dict, List

from pydantic import main
from sqlalchemy.sql.sqltypes import String

from app.external import JSON
from app.external.rawls_entity_model import EntityReference
from app.external.tdr_model import Relationship, TDRManifest


@dataclass
class TDRTable:
    name: str
    primary_key: str
    parquet_files: List[str]
    reference_attrs: Dict[str, EntityReference] # column name -> reference


class TDRManifestParser:
    def __init__(self, jso: JSON):
        manifest = TDRManifest(**jso)
        self._tables = self._parse(manifest)
        self._snapshotid = manifest.snapshot.id
        self._snapshotname = manifest.snapshot.name

    def get_tables(self) -> List[TDRTable]:
        return self._tables

    def get_snapshot_id(self) -> str:
        return self._snapshotid

    def get_snapshot_name(self) -> str:
        return self._snapshotname

    def _parse(self, manifest: TDRManifest) -> List[TDRTable]:
        # the snapshot model: table names, primary keys, relationships
        snapshot = manifest.snapshot
        # the parquet export files
        parquet_location = manifest.format.parquet.location

        # build dict of table->parquet files for the exports
        exports = dict(map(lambda e: (e.name, e.paths), parquet_location.tables))

        # get the topological ordering of the tables, so add tables to rawls in the correct order to resolve references
        ordering = TDRManifestParser.get_ordering(manifest.snapshot.relationships)
        ordering_key_fn = lambda table: ordering.index(table) if table in ordering else -1
        ordered_tables = sorted(snapshot.tables, key=ordering_key_fn)

        tdr_tables = []

        # for each table in the snapshot model, extract the table name and primary key
        # TODO AS-1044: loop through tables in the order determined by AS-1036, so we
        # import dependants before the tables that depend on them
        for t in ordered_tables:
            table_name = t.name

            # don't bother dealing with this table if it has no exported parquet files
            if table_name in exports:
                # extract primary key
                tdr_pk = t.primaryKey
                if tdr_pk is None:
                    pk = 'datarepo_row_id'
                elif not isinstance(tdr_pk, list):
                    pk = tdr_pk
                elif isinstance(tdr_pk, list) and len(tdr_pk) == 1:
                    pk = tdr_pk[0]
                else:
                    pk = 'datarepo_row_id'

                # TODO AS-1036: from the "relationships" key in the manifest, save the valid reference attributes
                # to reference_attrs. A reference is valid iff the "to" column is the primary key of the "to" table.
                

                tdr_table = TDRTable(name=table_name, primary_key=pk, parquet_files=exports[table_name], reference_attrs={})
                tdr_tables.append(tdr_table)
        
        return tdr_tables


    def get_ordering(relationships: List[Relationship]) -> List[String]:
        ## Build a relationship graph of all the relationships
        relationship_graph = defaultdict(lambda : set())
        for relationship in relationships:
            from_table = relationship.from_.table
            to_table = relationship.to.table
            relationship_graph[from_table].add(to_table)

        ## Return a topological ordering of the relationship graph
        ## TODO handle exception
        ts = TopologicalSorter(relationship_graph)
        return list(ts.static_order())

