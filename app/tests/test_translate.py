from app import translate
from app.translators import Translator
from typing import Iterator, Dict, IO, Any

import os
import memunit


class StreamyNoOpTranslator(Translator):
    """Well-behaved no-op translator: does nothing, while streaming"""
    def translate(self, file_like: IO) -> Iterator[Dict[str, Any]]:
        return ({line: line} for line in file_like)


class BadNoOpTranslator(Translator):
    """Badly-behaved no-op translator: does nothing, using lots of memory"""
    def translate(self, file_like: IO) -> Iterator[Dict[str, Any]]:
        return iter([{line: line} for line in file_like])


def get_memory_usage_mb():
    # return the memory usage in MB
    import psutil
    process = psutil.Process(os.getpid())
    mem = process.memory_info()[0] / float(2 ** 20)
    return mem


def maybe_himem_work(numbers_path: str, translator: Translator):
    with open(numbers_path, 'r') as read_numbers:
        with open(os.devnull, 'w') as dev_null:
            translate._stream_translate(read_numbers, dev_null, translator)


def test_stream_translate(tmp_path):
    """Stream-translate a test file and check that Python memory consumption doesn't increase when streaming"""

    # This only makes a 576K file but the BadNoOpTranslator uses a LOT of memory keeping the full dict in-mem
    with open(tmp_path / "numbers.txt", 'w') as write_numbers:
        for n in range(100000):
            write_numbers.write(f"{n}\n")

    current_memusage_mb = get_memory_usage_mb()

    @memunit.assert_lt_mb(current_memusage_mb + 10)
    def good_noop_translate():
        maybe_himem_work(tmp_path / "numbers.txt", StreamyNoOpTranslator())

    @memunit.assert_gt_mb(current_memusage_mb + 10)
    def bad_noop_translate():
        maybe_himem_work(tmp_path / "numbers.txt", BadNoOpTranslator())

    # Test that a streamy translator stays under the memory limit.
    good_noop_translate()

    # A bad, in-memory translator should go OVER the memory limit. If this test fails, then the numbers-file
    # isn't big enough to accurately test whether our streaming works.
    bad_noop_translate()
