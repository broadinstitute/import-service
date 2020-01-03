import enum
import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, String
from sqlalchemy.ext.declarative import declarative_base
from app.db import DBSession


Base = declarative_base()  # sqlalchemy magic base class.


class ImportServiceTable(Base):
    """sqlalchemy's declarative_base() function constructs a base class for declarative class definitions -- in this
    case, our database tables. It creates the __table__ attribute on that class, but mypy can't see it.
    This class exists to add a type hint to the __table__ variable so mypy knows about it."""
    __table__: Table


# Mypy gets confused about whether sqlalchemy enum columns are strings or enums, see here:
# https://github.com/dropbox/sqlalchemy-stubs/issues/114
# This is the (gross) workaround. Keep an eye on the issue and get rid of it once it's fixed.
from typing import TYPE_CHECKING, Type, TypeVar
if TYPE_CHECKING:
    from sqlalchemy.sql.type_api import TypeEngine
    T = TypeVar('T')

    class Enum(TypeEngine[T]):
        def __init__(self, enum: Type[T]) -> None: ...
else:
    from sqlalchemy import Enum


@enum.unique
class ImportStatus(enum.Enum):
    Pending = enum.auto()
    Translating = enum.auto()


class Import(ImportServiceTable):
    __tablename__ = 'imports'

    id = Column(String(36), primary_key=True)
    workspace_name = Column(String(100), nullable=False)
    workspace_namespace = Column(String(100), nullable=False)
    workspace_uuid = Column(String(36), nullable=False)
    submitter = Column(String(100), nullable=False)
    import_url = Column(String(2048), nullable=False)  # max url length: https://stackoverflow.com/q/417142/2941784
    submit_time = Column(DateTime, nullable=False)
    status = Column(Enum(ImportStatus), nullable=False)

    def __init__(self, workspace_name: str, workspace_ns: str, workspace_uuid: str, submitter: str, import_url: str):
        self.id = str(uuid.uuid4())
        self.workspace_name = workspace_name
        self.workspace_namespace = workspace_ns
        self.workspace_uuid = workspace_uuid
        self.submitter = submitter
        self.import_url = import_url
        self.submit_time = datetime.now()
        self.status = ImportStatus.Pending

    def __repr__(self):
        # todo: replace with https://github.com/manicmaniac/sqlalchemy-repr
        return f"<Import({self.id}, {self.workspace_name}, {self.workspace_namespace}, {self.submitter}, {self.submit_time}, {self.status})>"

    @classmethod
    def update_status_exclusively(cls, id: str, current_status: ImportStatus, new_status: ImportStatus, sess: DBSession) -> bool:
        update = Import.__table__.update() \
            .where(Import.id == id) \
            .where(Import.status == current_status) \
            .values(status=new_status)
        num_affected_rows = sess.execute(update).rowcount
        return num_affected_rows > 0
