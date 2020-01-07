import enum
from typing import TYPE_CHECKING, Type


def register_enum(cls: enum.Enum):
    """
    Mypy gets confused about whether sqlalchemy enum columns are strings or enums, see here:
    https://github.com/dropbox/sqlalchemy-stubs/issues/114
    This is the workaround, but this PR won't be merged until Hussein's comment on the thread
    gets an answer.
    """
    if TYPE_CHECKING:
        from sqlalchemy.sql.type_api import TypeEngine

        class Enum(TypeEngine[cls]):
            def __init__(self, enum: Type[cls]) -> None: ...
    else:
        from sqlalchemy import Enum
