import enum
import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, String, Enum
from sqlalchemy.ext.declarative import declarative_base


@enum.unique
class ImportStatus(enum.Enum):
    Pending = enum.auto()
    Running = enum.auto()


Base = declarative_base()  # sqlalchemy magic base class.


class Import(Base):
    __tablename__ = 'imports'

    id = Column(String(36), primary_key=True)
    workspace_name = Column(String(100), nullable=False)
    workspace_namespace = Column(String(100), nullable=False)
    submitter = Column(String(100), nullable=False)
    submit_time = Column(DateTime, nullable=False)
    status = Column(Enum(ImportStatus), nullable=False)

    def __init__(self, workspace_name: str, workspace_ns: str, submitter: str):
        self.id = str(uuid.uuid4())
        self.workspace_name = workspace_name
        self.workspace_namespace = workspace_ns
        self.submitter = submitter
        self.submit_time = datetime.now()
        self.status = ImportStatus.Pending.name

    def __repr__(self):
        # todo: replace with https://github.com/manicmaniac/sqlalchemy-repr
        return f"<Import({self.id}, {self.workspace_name}, {self.workspace_namespace}, {self.submitter}, {self.submit_time}, {self.status})>"
