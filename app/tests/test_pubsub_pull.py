from app.db import model
from unittest import mock
import pytest

from app import db
from app.external import pubsub_pull


def test_pubsub_pull(fake_import, client, monkeypatch):
    """Test the pubsub pull mechanism by faking a message updating the status of an import."""

    # Add a fake import to the db.
    with db.session_ctx() as sess:
        sess.add(fake_import)

    def fake_pull_self(_howmany: int):
        """Fake version of pubsub.pull_self.
        First call yields something that looks like a message.
        Second call raises an exception so that loop() doesn't loop forever."""
        fake_message = mock.MagicMock()
        fake_message.message = mock.MagicMock()
        fake_message.message.attributes = {"action": "status", "import_id": fake_import.id,
                                           "current_status": "Pending",
                                           "new_status": "Upserting"}
        return [fake_message]

    # Overwrite the pubsub module to use fake_pull_self.
    monkeypatch.setattr("app.external.pubsub.pull_self", fake_pull_self)
    monkeypatch.setattr("app.external.pubsub.acknowledge_self_messages", mock.MagicMock())

    # Now, calling poll should get a message asking to flip the fake_import to Upserting.
    pubsub_pull.poll(client.application)

    with db.session_ctx() as sess2:
        imp: model.Import = model.Import.get(fake_import.id, sess2)
        assert imp.status == model.ImportStatus.Upserting
