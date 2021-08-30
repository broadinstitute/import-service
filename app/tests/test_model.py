import pytest

from app.db import db, model
from app.db.model import ImportStatus
from typing import Iterator, Dict, IO, Any
import copy


def test_eq(fake_import: model.Import):
    copied = copy.deepcopy(fake_import)
    assert fake_import == copied


def test_reacquire(fake_import: model.Import):
    with db.session_ctx() as sess:
        sess.add(fake_import)

    with db.session_ctx() as sess2:
        reacquired: model.Import = model.Import.get(fake_import.id, sess2)
        assert fake_import == reacquired


def test_truncate(fake_import: model.Import):
    with db.session_ctx() as sess:
        fake_import.error_message = "a" * 3000
        sess.add(fake_import)
        assert len(fake_import.error_message) == 2048


def test_update_status_exclusively(fake_import: model.Import):
    # NOTE: ideally we'd like to test this in a multithreaded context, with different transactions modifying
    # the same row at the same time. but we've overridden the db session in tests to only ever return a single
    # one, so that's not gonna work.

    with db.session_ctx() as sess:
        sess.add(fake_import)

    # check we find the import
    with db.session_ctx() as sess2:
        updated = model.Import.update_status_exclusively(fake_import.id, model.ImportStatus.Pending, model.ImportStatus.Translating, sess2)
        assert updated

    # status has now been flipped to Translating, so this will fail
    with db.session_ctx() as sess3:
        updated = model.Import.update_status_exclusively(fake_import.id, model.ImportStatus.Pending, model.ImportStatus.Translating, sess3)
        assert not updated

def test_importstatus_enum_fromstring():
    """ImportStatus enum from_string() works as expected"""

    # don't need to test every enum member
    p = ImportStatus.from_string("Pending")
    assert p == ImportStatus.Pending

    d = ImportStatus.from_string("Done")
    assert d == ImportStatus.Done

    r = ImportStatus.from_string("ReadyForUpsert")
    assert r == ImportStatus.ReadyForUpsert

    with pytest.raises(NotImplementedError):
        ImportStatus.from_string("not a valid enum string")

def test_import_is_upsert_default():
    """Import model sets is_upsert to True when omitted"""

    new_import = model.Import(
        workspace_name="ws_name",
        workspace_ns="ws_ns",
        workspace_uuid="workspace_uuid",
        submitter="user_info.user_email",
        import_url="import_url",
        filetype="filetype")

    assert new_import.is_upsert == True

@pytest.mark.parametrize("is_upsert", [True, False])
def test_import_is_upsert_allows_setting_false(is_upsert):
    """Import model sets is_upsert appropriately when specified"""

    new_import = model.Import(
        workspace_name="ws_name",
        workspace_ns="ws_ns",
        workspace_uuid="workspace_uuid",
        submitter="user_info.user_email",
        import_url="import_url",
        filetype="filetype",
        is_upsert=is_upsert)

    assert new_import.is_upsert == is_upsert
