## Pydantic model for TDR Manifest file, generated using https://github.com/koxudaxi/datamodel-code-generator
## TODO: remove in favor of tdr-client swagger definition https://broadworkbench.atlassian.net/browse/AJ-221
from typing import Any, List, Optional

from pydantic import BaseModel, Field

from app.external.cloud_platform import CloudPlatform


class StorageItem(BaseModel):
    region: str
    cloudResource: str
    cloudPlatform: CloudPlatform


class Dataset(BaseModel):
    id: str
    name: str
    description: Optional[str]
    defaultProfileId: str
    createdDate: str
    storage: List[StorageItem]
    # Older TDR manifests may not have this field, so made it optional when constructing a TDRManifest object.
    secureMonitoringEnabled: Optional[bool] = False


class SourceItem(BaseModel):
    dataset: Dataset
    asset: Any


class Column(BaseModel):
    name: str
    datatype: Optional[str]
    array_of: bool


class Table(BaseModel):
    name: str
    columns: List[Column]
    primaryKey: Any
    partitionMode: str
    datePartitionOptions: Any
    intPartitionOptions: Any
    rowCount: int


class TableColumn(BaseModel):
    table: str
    column: str


class Relationship(BaseModel):
    name: str
    from_: TableColumn = Field(..., alias='from')
    to: TableColumn


class Snapshot(BaseModel):
    id: str
    name: str
    description: str
    createdDate: str
    source: List[SourceItem]
    tables: List[Table]
    relationships: List[Relationship]
    profileId: str
    dataProject: Optional[str]
    accessInformation: Any


class TableLocation(BaseModel):
    name: str
    paths: List[str]


class Location(BaseModel):
    tables: List[TableLocation]


class Parquet(BaseModel):
    location: Location
    manifest: str


class Format(BaseModel):
    parquet: Parquet
    workspace: Any


class TDRManifest(BaseModel):
    snapshot: Snapshot
    format: Format
