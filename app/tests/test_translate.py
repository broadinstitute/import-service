import io
import os
import unittest.mock as mock
import urllib.error
from contextlib import contextmanager
from typing import IO, Any, Dict, Iterator

import gcsfs.retry
import memunit
import pytest
from app import db, translate
from app.db import model
from app.external.rawls_entity_model import Entity
from app.server import requestutils
from app.tests import testutils
from app.translators import Translator

# necessary to set this env var for unit tests; at runtime this is set by app.yaml
# if we don't set it here, assertions that compare gs:// paths can fail with
# an error where expected is "unittest-allowed-bucket" but actual is "None"
os.environ.setdefault("BATCH_UPSERT_BUCKET", "unittest-allowed-bucket")

class StreamyNoOpTranslator(Translator):
    """Well-behaved no-op translator: does nothing, while streaming"""
    def translate(self, import_details: model.Import, file_like: IO) -> Iterator[Entity]:
        return (Entity(line, 'line', []) for line in file_like)


class BadNoOpTranslator(Translator):
    """Badly-behaved no-op translator: does nothing, using lots of memory"""
    def translate(self, import_details: model.Import, file_like: IO) -> Iterator[Entity]:
        return iter([Entity(line, 'line', []) for line in file_like])


def get_memory_usage_mb():
    # return the memory usage in MB
    import psutil
    process = psutil.Process(os.getpid())
    mem = process.memory_info()[0] / float(2 ** 20)
    return mem


def maybe_himem_work(numbers_path: str, translator: Translator):
    import_details = model.Import("aa", "aa", "uuid", "project", "aa@aa.aa", "gs://aa/aa", "pfb")
    with open(numbers_path, 'r') as read_numbers:
        with open(os.devnull, 'wb') as dev_null:
            translate._stream_translate(import_details, read_numbers, dev_null, translator)


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


@pytest.fixture(scope="function")
def good_http_pfb(monkeypatch, fake_pfb):
    monkeypatch.setattr(translate.http, "http_as_filelike", mock.MagicMock(return_value=fake_pfb))

@pytest.fixture(scope="function")
def good_http_tdr_manifest(monkeypatch, fake_tdr_manifest):
    monkeypatch.setattr(translate.gcs, "open_file", mock.MagicMock(return_value=fake_tdr_manifest))

# method to patch in for end-to-end tdr manifest tests; returns either the fake_tdr_manifest or the fake_parquet,
# based on input args. Inside the tdr manifest translator, we use gcs.open_file first to open the json manifest,
# then again to open each parquet file. When patching a mock response for gcs_open_file for these tests,
# we need to be dynamic based on the file path being read.
#
# N.B. this copy/pastes the fake_parquet() and fake_tdr_manifest() implementations from conftest.py. I can't get the
# multiple layers of fixtures to work correctly together so a copy/paste seemed an ok solution.
@contextmanager
def open_tdr_manifest_or_parquet_file_gcp(project: str, bucket: str, path: str, submitter: str, pet_key: Dict[str, Any] = None) -> Iterator[IO]:  # type: ignore
    if path.endswith('parquet'):
        with open("app/tests/empty.parquet", 'rb') as out:
            yield out
    else:
        with open("app/tests/resources/test_tdr_response_gcp.json", 'rb') as out:
            yield out

@contextmanager
def open_tdr_manifest_or_parquet_file_azure(url: str) -> Iterator[IO]:
    if url.find(".parquet") > -1:
        with open("app/tests/empty.parquet", 'rb') as out:
            yield out
    else:
        with open("app/tests/resources/test_tdr_response_azure.json", 'rb') as out:
            yield out

@contextmanager
def open_tdr_manifest_or_parquet_file_azure_bad(url: str) -> Iterator[IO]:
    if url.find(".parquet") > -1:
        with open("app/tests/empty.parquet", 'rb') as out:
            yield out
    else:
        with open("app/tests/resources/test_tdr_response_azure_invalid_parquet.json", 'rb') as out:
            yield out

@pytest.fixture(scope="function")
def good_tdr_manifest_or_parquet_file_gcp(monkeypatch):
    monkeypatch.setattr(translate.gcs, "open_file", open_tdr_manifest_or_parquet_file_gcp)

@pytest.fixture(scope="function")
def good_tdr_manifest_or_parquet_file_azure(monkeypatch):
    monkeypatch.setattr(translate.http, "http_as_filelike", open_tdr_manifest_or_parquet_file_azure)

@pytest.fixture(scope="function")
def bad_tdr_manifest_or_parquet_file_azure(monkeypatch):
    monkeypatch.setattr(translate.http, "http_as_filelike", open_tdr_manifest_or_parquet_file_azure_bad)

@pytest.fixture(scope="function")
def forbidden_http_pfb(monkeypatch):
    mm = mock.MagicMock(side_effect=urllib.error.HTTPError("http://bad.pfb", 403, "Forbidden", {}, None))
    monkeypatch.setattr(translate.http, "http_as_filelike", mm)


@pytest.fixture(scope="function")
def junk_http_pfb(monkeypatch):
    junk_bytes = io.BytesIO(b"this is junk")
    junk_bytes.seek(0)
    monkeypatch.setattr(translate.http, "http_as_filelike", mock.MagicMock(return_value=junk_bytes))


@pytest.fixture(scope="function")
def good_gcs_dest(monkeypatch, pubsub_fake_env):
    monkeypatch.setattr(translate.service_auth, "get_isvc_credential", mock.MagicMock())
    gcsfs_mock = mock.MagicMock()
    gcsfs_mock.open = mock.mock_open()
    monkeypatch.setattr(translate, "GCSFileSystem", gcsfs_mock)


@pytest.fixture(scope="function")
def bad_gcs_dest(monkeypatch, pubsub_fake_env):
    monkeypatch.setattr(translate.service_auth, "get_isvc_credential", mock.MagicMock())

    # This is a bit bonkers. Here's an explanation.
    # Our goal is to make the __exit__ function of the "with gcs_project.open()" context manager throw an exception.
    # This mimics the behaviour of gcsfs when it doesn't have permission to write to the bucket.

    # To get there, we have to mock:
    # - the result of the GCSFileSystem constructor, giving us a fake constructor.
    # - the result of the fake constructor's open() call giving us a fake file-like object that's also a context manager.
    # - the result of the fake context manager's __exit__() function, to throw the exception we want.

    # This would require multiple lines of creating mock objects and assigning them, were it not for the fact that
    # when you call any function or access any variable on a MagicMock object, it returns another MagicMock object.
    # So you can just dereference your way to the exact thing you want to override and do that.

    gcsfs_mock = mock.MagicMock()
    monkeypatch.setattr(translate, "GCSFileSystem", gcsfs_mock)
    error_msg = {"message":"Anonymous caller does not have storage.objects.create access", "code":403}
    gcsfs_mock.return_value.open.return_value.__exit__ = mock.MagicMock(side_effect = gcsfs.retry.HttpError(error_msg))


@pytest.fixture(scope="function")
def fake_publish_rawls(monkeypatch, pubsub_fake_env):
    mm = mock.MagicMock()
    monkeypatch.setattr(translate.pubsub, "publish_rawls", mm)
    yield mm


@pytest.mark.usefixtures("good_http_pfb", "good_gcs_dest", "incoming_valid_pubsub")
def test_golden_path_pfb(fake_import, fake_publish_rawls, client):
    """Everything is fine: the pfb is valid and retrievable, and we can write to the destination."""
    with db.session_ctx() as sess:
        sess.add(fake_import)

    resp = client.post("/_ah/push-handlers/receive_messages",
                       json=testutils.pubsub_json_body({"action":"translate", "import_id":fake_import.id}))

    # result should be OK
    assert resp.status_code == 200

    # import should be updated to next step
    with db.session_ctx() as sess:
        imp: model.Import = model.Import.get(fake_import.id, sess)
        assert imp.status == model.ImportStatus.ReadyForUpsert

    # rawls should have been told to do something
    fake_publish_rawls.assert_called_once()


@pytest.mark.usefixtures("good_tdr_manifest_or_parquet_file_gcp", "good_gcs_dest", "incoming_valid_pubsub", "sam_valid_pet_key")
def test_golden_path_tdr_manifest_gcp(fake_import_tdr_manifest_gcp, fake_publish_rawls, client):
    """Everything is fine: the tdr manifest file is valid and retrievable, and we can write to the destination."""
    with db.session_ctx() as sess:
        sess.add(fake_import_tdr_manifest_gcp)

    resp = client.post("/_ah/push-handlers/receive_messages",
                       json=testutils.pubsub_json_body({"action":"translate", "import_id":fake_import_tdr_manifest_gcp.id}))

    # result should be OK
    assert resp.status_code == 200

    # import should be updated to next step
    with db.session_ctx() as sess:
        imp: model.Import = model.Import.get(fake_import_tdr_manifest_gcp.id, sess)
        assert imp.status == model.ImportStatus.ReadyForUpsert
        assert imp.snapshot_id == "9516afec-583f-11ec-bf63-0242ac130002"

    # rawls should have been told to do something
    fake_publish_rawls.assert_called_once()


@pytest.mark.usefixtures("good_tdr_manifest_or_parquet_file_azure", "good_gcs_dest", "incoming_valid_pubsub", "sam_valid_pet_key")
def test_golden_path_tdr_manifest_azure(fake_import_tdr_manifest_azure, fake_publish_rawls, client):
    """Everything is fine: the tdr manifest file is valid and retrievable, and we can write to the destination."""
    with db.session_ctx() as sess:
        sess.add(fake_import_tdr_manifest_azure)

    resp = client.post("/_ah/push-handlers/receive_messages",
                       json=testutils.pubsub_json_body({"action":"translate", "import_id":fake_import_tdr_manifest_azure.id}))

    # result should be OK
    assert resp.status_code == 200

    # import should be updated to next step
    with db.session_ctx() as sess:
        imp: model.Import = model.Import.get(fake_import_tdr_manifest_azure.id, sess)
        assert imp.status == model.ImportStatus.ReadyForUpsert
        assert imp.snapshot_id == "9516afec-583f-11ec-bf63-0242ac130002"

    # rawls should have been told to do something
    fake_publish_rawls.assert_called_once()

@pytest.mark.usefixtures("bad_tdr_manifest_or_parquet_file_azure", "good_gcs_dest", "incoming_valid_pubsub", "sam_valid_pet_key")
def test_bad_actor_path_tdr_manifest_azure(fake_import_tdr_manifest_azure, fake_publish_rawls, client):
    """Everything is fine: the tdr manifest file is valid and retrievable, and we can write to the destination."""
    with db.session_ctx() as sess:
        sess.add(fake_import_tdr_manifest_azure)

    resp = client.post("/_ah/push-handlers/receive_messages",
                       json=testutils.pubsub_json_body({"action":"translate", "import_id":fake_import_tdr_manifest_azure.id}))

    # result should be not-OK
    assert resp.status_code == requestutils.PUBSUB_STATUS_NOTOK


@pytest.mark.parametrize("is_upsert", [True, False])
@pytest.mark.usefixtures("good_http_pfb", "good_gcs_dest", "incoming_valid_pubsub")
def test_publish_rawls_is_upsert_passed_on(is_upsert, fake_publish_rawls, client):
    """is_upsert value from the database is sent along to Rawls in the pubsub message."""
    test_import = model.Import("bb", "bb", "uuid", "project", "bb@bb.bb", "gs://bb/bb", "pfb", is_upsert=is_upsert)

    with db.session_ctx() as sess:
        sess.add(test_import)

    resp = client.post("/_ah/push-handlers/receive_messages",
                       json=testutils.pubsub_json_body({"action":"translate", "import_id":test_import.id}))

    # result should be OK
    assert resp.status_code == 200

    # import should be updated to next step
    with db.session_ctx() as sess:
        imp: model.Import = model.Import.get(test_import.id, sess)
        assert imp.status == model.ImportStatus.ReadyForUpsert

        # rawls should have been told to do something
        fake_publish_rawls.assert_called_once_with({
            "workspaceNamespace": "bb",
            "workspaceName": "bb",
            "userEmail": "bb@bb.bb",
            "jobId": imp.id,
            "upsertFile": f"unittest-allowed-bucket/{imp.id}.rawlsUpsert",
            "isUpsert": str(is_upsert)
        })

@pytest.mark.usefixtures("forbidden_http_pfb", "good_gcs_dest", "incoming_valid_pubsub")
def test_forbidden_pfb(fake_import, fake_publish_rawls, client):
    """PFB retrieval fails with a 403 Forbidden."""
    with db.session_ctx() as sess:
        sess.add(fake_import)

    resp = client.post("/_ah/push-handlers/receive_messages",
                       json=testutils.pubsub_json_body({"action":"translate", "import_id":fake_import.id}))

    # result should be not-OK
    assert resp.status_code == requestutils.PUBSUB_STATUS_NOTOK

    # import should be set to error
    with db.session_ctx() as sess:
        imp: model.Import = model.Import.get(fake_import.id, sess)
        assert imp.status == model.ImportStatus.Error
        assert "Forbidden" in imp.error_message

    # no pubsub message should have been sent
    fake_publish_rawls.assert_not_called()


@pytest.mark.usefixtures("junk_http_pfb", "good_gcs_dest", "incoming_valid_pubsub")
def test_junk_pfb(fake_import, fake_publish_rawls, client):
    with db.session_ctx() as sess:
            sess.add(fake_import)

    resp = client.post("/_ah/push-handlers/receive_messages",
                       json=testutils.pubsub_json_body({"action":"translate", "import_id":fake_import.id}))

    # result should be not-OK
    assert resp.status_code == requestutils.PUBSUB_STATUS_NOTOK

    # import should be set to error
    with db.session_ctx() as sess:
        imp: model.Import = model.Import.get(fake_import.id, sess)
        assert imp.status == model.ImportStatus.Error
        assert "Error translating file" in imp.error_message

    # no pubsub message should have been sent
    fake_publish_rawls.assert_not_called()


@pytest.mark.usefixtures("good_http_pfb", "bad_gcs_dest", "incoming_valid_pubsub")
def test_bad_gcs(fake_import, fake_publish_rawls, client):
    """The PFB is fine, but import service doesn't have permission to write to the bucket.
    This is a programmer error."""
    with db.session_ctx() as sess:
        sess.add(fake_import)

    resp = client.post("/_ah/push-handlers/receive_messages",
                       json=testutils.pubsub_json_body({"action":"translate", "import_id":fake_import.id}))

    # result should be not-OK
    assert resp.status_code == requestutils.PUBSUB_STATUS_NOTOK

    # import should be set to error
    with db.session_ctx() as sess:
        imp: model.Import = model.Import.get(fake_import.id, sess)
        assert imp.status == model.ImportStatus.Error
        assert "System error" in imp.error_message

    # no pubsub message should have been sent
    fake_publish_rawls.assert_not_called()
