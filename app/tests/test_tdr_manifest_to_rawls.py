from app.translators.tdr_manifest_to_rawls import TDRManifestToRawls

# this unit test asserts that the parquet translation results in an iterator with one entity per snapshot table.
# this test is only valid for the noop end-to-end spike, in which we don't actually look inside the parquet file.
# once we implement real parquet translation, this test must be deleted or updated.
def test_noop_translate_tdr_manifest(fake_tdr_manifest):
    """Proper translation of parquet files to rawls json."""
    translator = TDRManifestToRawls()
    result_iterator = translator.translate(fake_tdr_manifest, "tdrexport")

    reclist = list(result_iterator)

    # test-data manifest has 27 tables
    assert(len(reclist) == 27)
