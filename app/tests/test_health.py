import flask.testing
import jsonschema
import pytest
from unittest import mock

from app import new_import, translate
from app.util import exceptions
from app.db import db
from app.db.model import *


@pytest.fixture(scope="function")
def sam_ok(monkeypatch):
    """Makes us think that Sam is fine."""
    monkeypatch.setattr("app.external.sam.check_health",
                        mock.MagicMock(return_value=True))


@pytest.fixture(scope="function")
def rawls_ok(monkeypatch):
    """Makes us think that Rawls is fine."""
    monkeypatch.setattr("app.external.rawls.check_health",
                        mock.MagicMock(return_value=True))


@pytest.fixture(scope="function")
def db_ok(monkeypatch):
    """Makes us think that our db is fine."""
    monkeypatch.setattr("app.health.check_health",
                        mock.MagicMock(return_value=True))


@pytest.fixture(scope="function")
def rawls_bad(monkeypatch):
    """Makes us think that Rawls is dead."""
    monkeypatch.setattr("app.external.rawls.check_health",
                        mock.MagicMock(return_value=False))


@pytest.mark.usefixtures("sam_ok", "rawls_ok", "db_ok")
def test_everything_ok(client):
    resp = client.get('/health')
    assert resp.status_code == 200

    assert resp.json["ok"]
    assert resp.json["subsystems"]["rawls"]
    assert resp.json["subsystems"]["sam"]
    assert resp.json["subsystems"]["db"]


@pytest.mark.usefixtures("sam_ok", "rawls_bad", "db_ok")
def test_one_subsystem_died(client):
    resp = client.get('/health')
    assert resp.status_code == 200

    assert not resp.json["ok"]
    assert not resp.json["subsystems"]["rawls"]
    assert resp.json["subsystems"]["sam"]
    assert resp.json["subsystems"]["db"]
