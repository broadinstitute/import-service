from .common import db
from .common.model import *
from typing import List

schema = {
    "task_id": "uuid",
    "job_id": "uuid",
    "path": "string"
}

# TODO: This is all still really placeholder.


def handle(message: dict) -> List[Import]:
    if "job_id" in message:
        print(f'received message for job id {message["job_id"]}')
        with db.session_ctx() as sess:
            result = sess.query(Import).filter(Import.id == message["job_id"]).all()
            print(f"db results {result}")
            return result
    else:
        raise RuntimeError("task is missing job id!")
