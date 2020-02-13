import flask
import json
import logging
from sqlalchemy.orm.exc import NoResultFound
from typing import Dict

from app.auth import user_auth
from app.db import db, model
from app.db.model import ImportStatus
from app.external import sam, rawls
from app.util import exceptions


def handle_health_check() -> flask.Response:

    sam_health = sam.check_health()
    rawls_health = rawls.check_health()
    db_health = check_health()

    isvc_health = all([sam_health, rawls_health, db_health])

    return flask.make_response((json.dumps({"ok": isvc_health, "subsystems": {"db": db_health, "rawls": rawls_health, "sam": sam_health}}), 200))


def check_health() -> bool:
    with db.session_ctx() as sess:

        res = sess.execute("select true").rowcount
        return bool(res)