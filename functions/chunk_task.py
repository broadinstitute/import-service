from .common import db
from .common.model import *

schema = {
    "task_id": "uuid",
    "job_id": "uuid",
    "path": "string"
}


def handle(message: dict) -> None:
    if "job_id" in message:
        sess = db.get_session()
        result = sess.query(Import).filter(Import.id == message["job_id"]).fetchall()
        print(f"db results {result}")
        return result
    else:
        raise RuntimeError("task is missing job id!")
