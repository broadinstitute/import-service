from flask_restx import fields

from app.db import db, model
from app.external import sam, rawls


class HealthResponse:
    def __init__(self, db_health: bool, rawls_health: bool, sam_health: bool):
        self.ok = all([db_health, rawls_health, sam])
        self.subsystems = {
            "db": db_health,
            "rawls": rawls_health,
            "sam": sam_health
        }

    @classmethod
    def get_model(cls, api) -> model.ModelDefinition:
        return {
            "ok": fields.Boolean,
            "subsystems": fields.Nested(api.model('SubsystemModel', {
                "db": fields.Boolean,
                "rawls": fields.Boolean,
                "sam": fields.Boolean
            }))
        }


def handle_health_check() -> HealthResponse:
    sam_health = sam.check_health()
    rawls_health = rawls.check_health()
    db_health = check_health()

    return HealthResponse(db_health, rawls_health, sam_health)


def check_health() -> bool:
    with db.session_ctx() as sess:

        res = sess.execute("select true").rowcount
        return bool(res)
