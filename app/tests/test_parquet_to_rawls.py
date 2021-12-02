from app.translators.parquet_to_rawls import ParquetToRawls
from app.translators.parquet_to_rawls import SAMPLE_ENTITY

import pytest

# this unit test asserts that the parquet translation results in an iterator with one hardcoded entity in it.
# this test is only valid for the noop end-to-end spike, in which we don't actually look inside the parquet file.
# once we implement real parquet translation, this test must be deleted or updated.
def test_noop_translate_parquet(fake_import_parquet):
    """Proper translation of parquet files to rawls json."""
    translator = ParquetToRawls()
    result_iterator = translator.translate(fake_import_parquet, "tdrexport")
    # result_iterator should have one entity in it:
    assert next(result_iterator) == SAMPLE_ENTITY
    # and since there's only one entity, we should see StopIteration when asking for anything beyond the first
    with pytest.raises(StopIteration):
        next(result_iterator)


