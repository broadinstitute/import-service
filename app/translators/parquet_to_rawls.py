import logging

from app.translators.translator import Translator
from typing import Iterator, Dict, Any


class ParquetToRawls(Translator):
    def __init__(self, options=None):
        """Translator for Parquet files."""
        if options is None:
            options = {}
        defaults = {}
        self.options = {**defaults, **options}

    def translate(self, file_like, file_type) -> Iterator[Dict[str, Any]]:
        logging.info(f"executing a no-op ParquetToRawls translation for {file_type}: {file_like}")
        return iter([])
