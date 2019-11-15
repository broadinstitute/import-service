from sqlalchemy import Column, DateTime, String, Enum
from sqlalchemy.ext.declarative import declarative_base
import enum


@enum.unique
class ImportStatus(enum.Enum):
    Pending = enum.auto()
    Running = enum.auto()


Base = declarative_base()


class Import(Base):
    __tablename__ = 'imports'

    id = Column(String, primary_key=True)
    workspace_name = Column(String, nullable=False)
    workspace_namespace = Column(String, nullable=False)
    submitter = Column(String, nullable=False)
    submit_time = Column(DateTime, nullable=False)
    status = Column(Enum(ImportStatus), nullable=False)

    def __repr__(self):
        # todo: replace with https://github.com/manicmaniac/sqlalchemy-repr
        return f"<Import({self.id}, {self.workspace_name}, {self.workspace_namespace}, {self.submitter}, {self.submit_time}, {self.status})>"
