from abc import ABC, abstractmethod
from typing import IO, Iterator

from app.db.model import Import
from app.external.rawls_entity_model import Entity


class Translator(ABC):
    @abstractmethod
    def translate(self, import_details: Import, file_like: IO) -> Iterator[Entity]:
        pass
