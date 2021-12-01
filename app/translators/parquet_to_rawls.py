import base64
import logging

from app.translators.translator import Translator
from typing import Iterator, Dict, Set, Tuple, Any


class ParquetToRawls(Translator):
    def __init__(self, options=None):
        if options is None:
            options = {}
        defaults = {}
        self.options = {**defaults, **options}

    def translate(self, file_like, file_type) -> Iterator[Dict[str, Any]]:
        logging.info("executing a no-op ParquetToRawls translation")
        return iter([])
