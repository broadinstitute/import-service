from abc import ABC, abstractmethod
from typing import Iterator, Dict, IO, Any


class Translator(ABC):
    @abstractmethod
    def translate(self, file_like: IO) -> Iterator[Dict[str, Any]]:
        pass
