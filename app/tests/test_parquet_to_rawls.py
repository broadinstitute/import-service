from app.translators.parquet_to_rawls import ParquetToRawls

import pytest

# this unit test asserts that the parquet translation results in an empty iterator.
# this test is only valid for the noop end-to-end spike, in which we don't actually look inside the parquet file.
# once we implement real parquet translation, this test must be deleted or updated.
def test_noop_translate_parquet(fake_import_parquet):
    """Proper translation of parquet files to rawls json."""
    translator = ParquetToRawls()
    result_iterator = translator.translate(fake_import_parquet, "tdrexport")
    with pytest.raises(StopIteration):
        item = next(result_iterator)


