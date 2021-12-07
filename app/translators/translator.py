from abc import ABC, abstractmethod
from typing import IO, Iterator

from app.external.rawls_entity_model import Entity


class Translator(ABC):
    @abstractmethod
    def translate(self, file_like: IO, file_type: str) -> Iterator[Entity]:
        pass
