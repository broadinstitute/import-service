import pyarrow.parquet as pq

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
    assert (len(reclist) == 27)


def test_read_parquet(sample_tdr_parquet_file):
    translator = TDRManifestToRawls()
    attrs_to_add = translator.convert_parquet_file_to_entity_attributes(sample_tdr_parquet_file)
    pq_table = pq.read_table(sample_tdr_parquet_file)
    assert len(attrs_to_add) == pq_table.num_rows
    assert len(attrs_to_add[0]) == pq_table.num_columns
    first_attr_name = attrs_to_add[0][0].attributeName
    assert first_attr_name == pq_table.column_names[0]
    assert pq_table.to_batches()[0].to_pydict()[first_attr_name][0] == attrs_to_add[0][0].addUpdateAttribute
