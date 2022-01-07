from collections import defaultdict
from dataclasses import dataclass
from graphlib import TopologicalSorter
from itertools import groupby
from typing import Dict, List, Optional, Tuple

from pydantic import main

from app.external import JSON
from app.external.rawls_entity_model import EntityReference
from app.external.tdr_model import Relationship, Table, TDRManifest


@dataclass
class TDRTable:
    name: str
    primary_key: str
    parquet_files: List[str]
    reference_attrs: Dict[str, str] # column name -> entity type (for referenced table)


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

        # don't bother dealing with this table if it has no exported parquet files
        tables_for_export: List[Table] = list(filter(lambda t: t.name in exports, snapshot.tables))

        # get the topological ordering of the tables, so we add tables to rawls in the correct order to resolve references
        ordering = TDRManifestParser.get_ordering(manifest.snapshot.relationships)
        ordering_key_fn = lambda table: ordering.index(table.name) if table.name in ordering else -1
        ordered_tables = sorted(tables_for_export, key=ordering_key_fn)

        table_to_primary_key: Dict[str, str] = \
            {t.name: TDRManifestParser.get_primary_key(t) for t in ordered_tables}
        
        table_to_relationships: Dict[str, List[Relationship]] = \
            TDRManifestParser.get_table_to_relationships(manifest.snapshot.relationships)

        # for each table in the snapshot model, extract the table name and primary key
        # import tables in order, so we import dependants before the tables that depend on them
        tdr_tables = list(map(
            lambda table: TDRTable(
                name=table.name,
                primary_key=table_to_primary_key[table.name],
                parquet_files=exports[table.name],
                reference_attrs=TDRManifestParser.get_reference_attrs(table_to_relationships[table.name], table_to_primary_key)
            ), 
            ordered_tables
        ))

        return tdr_tables


    @staticmethod
    def get_table_to_relationships(relationships: List[Relationship]) -> Dict[str, str]:
        table_to_relationships = defaultdict(lambda: [])
        for r in relationships:
            table_to_relationships[r.from_.table].append(r)
        return table_to_relationships


    @staticmethod
    def get_primary_key(table: Table) -> str:
        # extract primary key
        tdr_pk = table.primaryKey
        if tdr_pk is None:
            pk = 'datarepo_row_id'
        elif not isinstance(tdr_pk, list):
            pk = tdr_pk
        elif isinstance(tdr_pk, list) and len(tdr_pk) == 1:
            pk = tdr_pk[0]
        else:
            pk = 'datarepo_row_id'
        return pk
    

    # from the "relationships" key in the manifest, save the valid reference attributes to reference_attrs.
    @staticmethod
    def get_reference_attrs(relationships: List[Relationship], table_to_primary_key: Dict[str, str]) -> Dict[str, str]:
        def relationship_to_reference(r: Relationship) -> Optional[Tuple[str, str]]:
            # A reference is valid iff the "to" column is the primary key of the "to" table.
            primary_key = table_to_primary_key[r.to.table]
            if primary_key != r.to.column:
                return None
            else:
                return (r.from_.column, r.to.table)

        reference_attrs: Dict[str, str] = dict(filter(None, map(relationship_to_reference, relationships)))
        return reference_attrs

    @staticmethod
    def get_ordering(relationships: List[Relationship]) -> List[str]:
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

