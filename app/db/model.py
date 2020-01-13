import enum
import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, String
from sqlalchemy.schema import Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_repr import RepresentableBase
from app.db import DBSession

Base = declarative_base(cls=RepresentableBase)  # sqlalchemy magic base class.


class ImportServiceTable:
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
    """NOTE: enums are special python classes where all members are enum instances.
    so doing ALL_STATUSES = [foo, bar, baz] will give you a new enum member call ALL_STATUSES,
    which is definitely not what you want! hence these being functions, not members."""
    Pending = enum.auto()
    Translating = enum.auto()
    Error = enum.auto()
    Done = enum.auto()

    @classmethod
    def all_statuses(cls):
        return {cls.Pending, cls.Translating, cls.Error, cls.Done}

    @classmethod
    def terminal_statuses(cls):
        return {cls.Error, cls.Done}

    @classmethod
    def running_statuses(cls):
        return cls.all_statuses() - cls.terminal_statuses()


ImportT = TypeVar('ImportT', bound='Import')


class Import(Base, ImportServiceTable):
    __tablename__ = 'imports'

    id = Column(String(36), primary_key=True)
    workspace_name = Column(String(100), nullable=False)
    workspace_namespace = Column(String(100), nullable=False)
    workspace_uuid = Column(String(36), nullable=False)
    submitter = Column(String(100), nullable=False)
    import_url = Column(String(2048), nullable=False)  # max url length: https://stackoverflow.com/q/417142/2941784
    submit_time = Column(DateTime, nullable=False)
    status = Column(Enum(ImportStatus), nullable=False)
    filetype = Column(String(10), nullable=False)
    error_message = Column(String(2048), nullable=True)

    def __init__(self, workspace_name: str, workspace_ns: str, workspace_uuid: str, submitter: str, import_url: str, filetype: str):
        self.id = str(uuid.uuid4())
        self.workspace_name = workspace_name
        self.workspace_namespace = workspace_ns
        self.workspace_uuid = workspace_uuid
        self.submitter = submitter
        self.import_url = import_url
        self.submit_time = datetime.now()
        self.status = ImportStatus.Pending
        self.filetype = filetype
        self.error_message = None

    @classmethod
    def reacquire(cls, id: str, sess: DBSession) -> ImportT:
        i: ImportT = sess.query(Import).filter(Import.id == id).one()
        return i

    @classmethod
    def update_status_exclusively(cls, id: str, current_status: ImportStatus, new_status: ImportStatus, sess: DBSession) -> bool:
        update = Import.__table__.update() \
            .where(Import.id == id) \
            .where(Import.status == current_status) \
            .values(status=new_status)
        num_affected_rows = sess.execute(update).rowcount
        return num_affected_rows > 0

    def write_error(self, msg: str) -> None:
        self.error_message = msg
        self.status = ImportStatus.Error
