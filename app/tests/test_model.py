from app.db import db, model
from typing import Iterator, Dict, IO, Any
import copy


def test_eq(fake_import: model.Import):
    copied = copy.deepcopy(fake_import)
    assert fake_import == copied


def test_reacquire(fake_import: model.Import):
    with db.session_ctx() as sess:
        sess.add(fake_import)

    with db.session_ctx() as sess2:
        reacquired: model.Import = model.Import.reacquire(fake_import.id, sess2)
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
