from app.translators.translator import Translator
from typing import Iterator, Dict, Any

# this class and its translate method should never be called in practice; it exists solely
# to make the FILETYPE_TRANSLATORS in translate.py correct and make the code pass linting.
# There is probably a better way to do this.

class BatchUpsertNoop(Translator):
    def translate(self, file_like, file_type) -> Iterator[Dict[str, Any]]:
        yield from ()
