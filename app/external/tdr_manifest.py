from collections import defaultdict
from dataclasses import dataclass
from graphlib import TopologicalSorter
from itertools import groupby
import logging
from typing import Dict, List, Optional, Tuple

from pydantic import main

from app.external import JSON
from app.external.rawls_entity_model import EntityReference
from app.external.tdr_model import Relationship, Table, TDRManifest, Column


@dataclass
class TDRTable:
    name: str
    primary_key: str
    parquet_files: List[str]
    reference_attrs: Dict[str, str] # column name -> entity type (for referenced table)
    columns: List[Column]


class TDRManifestParser:
    def __init__(self, jso: JSON, import_id: str):
        manifest = TDRManifest(**jso)
        self._snapshotid = manifest.snapshot.id
        self._snapshotname = manifest.snapshot.name
        self._import_id = import_id
        self._tables = self._parse(manifest)

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



        table_to_primary_key: Dict[str, str] = \
            {t.name: TDRManifestParser.get_primary_key(t) for t in tables_for_export}
        
        table_to_relationships: Dict[str, List[Relationship]] = \
            TDRManifestParser.get_table_to_relationships(manifest.snapshot.relationships)

        # for each table in the snapshot model, extract the table name and primary key
        # import tables in order, so we import dependants before the tables that depend on them
        tdr_tables = list(map(
            lambda table: TDRTable(
                name=table.name,
                primary_key=table_to_primary_key[table.name],
                parquet_files=exports[table.name],
                reference_attrs=self.get_reference_attrs(table_to_relationships[table.name], table_to_primary_key),
                columns = table.columns
            ), 
            tables_for_export
        ))

        # get the topological ordering of the tables, so we add tables to rawls in the correct order to resolve references
        ordering = TDRManifestParser.get_ordering(tdr_tables)
        ordering_key_fn = lambda table: ordering.index(table.name) if table.name in ordering else -1
        ordered_tables = sorted(tdr_tables, key=ordering_key_fn)

        return ordered_tables

    @staticmethod
    def get_table_to_relationships(relationships: List[Relationship]) -> Dict[str, List[Relationship]]:
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
    def get_reference_attrs(self, relationships: List[Relationship], table_to_primary_key: Dict[str, str]) -> Dict[str, str]:
        def relationship_to_reference(r: Relationship) -> Optional[Tuple[str, str]]:
            # A reference is valid iff the "to" column is the primary key of the "to" table.
            primary_key = table_to_primary_key[r.to.table]
            if primary_key != r.to.column:
                logging.info("TDR snapshot contains a relationship from table: %s column: %s to table: %s column: %s but %s \
                    is not the primary key of %s. import-job-id: %s, snapshot-id: %s", r.from_.table, r.from_.column, r.to.table, r.to.column, r.to.column, 
                    r.to.table, self._import_id, self._snapshotid)
                return None
            else:
                return (r.from_.column, r.to.table)

        reference_attrs: Dict[str, str] = dict(filter(None, map(relationship_to_reference, relationships)))
        return reference_attrs

    @staticmethod
    def get_ordering(tdr_tables: List[TDRTable]) -> List[str]:
        """Creates ordering for parsing tables. 
        
        Given a list of relationships between tables, tables that are depended on must be parsed before the tables
        that depend on them. Here an ordering is created to ensure tables are created in the correct order.

        Args:
            relationships - list of relationships
        Returns:
            ordering of tables as a list of tablenames in the order they should be processed
        Raises:
            CyclicError - error thrown if there is cycle created by the relationships
        """
        ## Build a relationship graph of all the relationships
        relationship_graph = defaultdict(lambda : set())
        for table in tdr_tables:
            for reference_attr in table.reference_attrs.values():
                from_table = table.name
                to_table = reference_attr
                relationship_graph[from_table].add(to_table)

        ## Return a topological ordering of the relationship graph
        ## TODO handle exception
        ts = TopologicalSorter(relationship_graph)
        return list(ts.static_order())

