from app import translate, db
from app.db import model
from app.translators import Translator
from app.server import requestutils
from app.tests import testutils
from typing import Iterator, Dict, IO, Any

import io
import os
import gcsfs.utils
import memunit
import urllib.error
import unittest.mock as mock
import pytest


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
        with open(os.devnull, 'wb') as dev_null:
            translate._stream_translate("unittest", read_numbers, dev_null, translator)


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
    gcsfs_mock.return_value.open.return_value.__exit__ = mock.MagicMock(side_effect = gcsfs.utils.HttpError(error_msg))


@pytest.fixture(scope="function")
def fake_publish_rawls(monkeypatch, pubsub_fake_env):
    mm = mock.MagicMock()
    monkeypatch.setattr(translate.pubsub, "publish_rawls", mm)
    yield mm


@pytest.mark.usefixtures("good_http_pfb", "good_gcs_dest", "incoming_valid_pubsub")
def test_golden_path(fake_import, fake_publish_rawls, client):
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
