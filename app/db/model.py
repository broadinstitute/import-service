import enum
import logging
import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, String
from sqlalchemy.schema import Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import validates
from sqlalchemy.sql.sqltypes import Boolean
from sqlalchemy_repr import RepresentableBase
from app.db import DBSession

from flask_restx import fields
from typing import Optional, Dict

Base = declarative_base(cls=RepresentableBase)  # sqlalchemy magic base class.


class ImportServiceTable:
    """sqlalchemy's declarative_base() function constructs a base class for declarative class definitions -- in this
    case, our database tables. It creates the __table__ attribute on that class, but mypy can't see it.
    This class exists to add a type hint to the __table__ variable so mypy knows about it."""
    __table__: Table


class EqMixin():
    """If you make a SQLAlchemy row class inherit from this, then == will compare column values, not memory location"""
    def __eq__(self, other):
        if type(other) is type(self):
            return all(self.__dict__[col] == other.__dict__[col] for col in self.__mapper__.attrs.keys())
        else:
            return NotImplemented


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
    Pending = 10  # import request received by the user but we haven't done anything with it yet
    Translating = 20  # in the process of translating to rawls batchUpsert
    ReadyForUpsert = 50  # batchUpsert file has been put in bucket and rawls has been notified
    Upserting = 100  # rawls is actively working on importing the batchUpsert file
    Done = 500  # success
    Error = 99999  # something bad happened, check the error_message column for details

    # NOTE: enums are special python classes where all members are enum instances.
    # so doing ALL_STATUSES = [foo, bar, baz] will give you a new enum member call ALL_STATUSES,
    # which is definitely not what you want! hence these being functions, not members.
    @classmethod
    def all_statuses(cls):
        return {e for e in ImportStatus}

    @classmethod
    def terminal_statuses(cls):
        return {cls.Error, cls.Done}

    @classmethod
    def running_statuses(cls):
        return cls.all_statuses() - cls.terminal_statuses()

    @classmethod
    def from_string(cls, name: str):
        try:
            return ImportStatus[name]
        except KeyError:
            raise NotImplementedError(f"Unknown ImportStatus enum {name}")


# Raw is the flask-restx base class for "a json-serializable field".
ModelDefinition = Dict[str, Type[fields.Raw]]


# Note: this should really be a namedtuple but for https://github.com/noirbizarre/flask-restplus/issues/364
# This is an easy fix in flask-restx if we decide to go this route.
class ImportStatusResponse:
    def __init__(self, jobId: str, status: str, message: Optional[str]):
        self.jobId = jobId
        self.status = status
        self.message = message

    @classmethod
    def get_model(cls) -> ModelDefinition:
        return {
            "jobId": fields.String,
            "status": fields.String,
            "message": fields.String }

# This is mypy shenanigans so functions inside the Import class can return an instance of type Import.
# It's basically a forward declaration of the type.
ImportT = TypeVar('ImportT', bound='Import')


class Import(ImportServiceTable, EqMixin, Base):
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
    is_upsert = Column(Boolean, nullable=False, default=True)

    @validates('error_message')
    def truncate(self, key, value):
        """Truncates the value of any write to the columns named in the decorator."""
        max_len = getattr(self.__class__, key).prop.columns[0].type.length
        if value and len(value) > max_len:
            return value[:max_len]
        return value

    def __init__(self, workspace_name: str, workspace_ns: str, workspace_uuid: str, submitter: str, import_url: str, filetype: str, is_upsert: bool = True):
        """Init method for Import model."""
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
        self.is_upsert = is_upsert

    @classmethod
    def get(cls, id: str, sess: DBSession) -> ImportT:
        """Used for getting a real, active Import object after closing a session."""
        i: ImportT = sess.query(Import).filter(Import.id == id).one()
        return i

    @classmethod
    def update_status_exclusively(cls, id: str, current_status: ImportStatus, new_status: ImportStatus, sess: DBSession) -> bool:
        """Given an object in status current_status, flip it to new_status and return True
        only if someone didn't steal the object meanwhile."""

        logging.info(f"Attempting to update import {id} status from {current_status} to {new_status} ...")

        update = Import.__table__.update() \
            .where(Import.id == id) \
            .where(Import.status == current_status) \
            .values(status=new_status)
        num_affected_rows = sess.execute(update).rowcount
        return num_affected_rows > 0

    def write_error(self, msg: str) -> None:
        self.error_message = msg
        self.status = ImportStatus.Error

    def to_status_response(self) -> ImportStatusResponse:
        return ImportStatusResponse(self.id, self.status.name, self.error_message)
