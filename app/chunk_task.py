import logging
from typing import List

from app.db import db
from app.db.model import *

schema = {
    "task_id": "uuid",
    "job_id": "uuid",
    "path": "string"
}

# TODO: This is all still really placeholder.


def handle(message: dict) -> List[Import]:
    if "job_id" in message:
        logging.info(f'received message for job id {message["job_id"]}')
        with db.session_ctx() as sess:
            result = sess.query(Import).filter(Import.id == message["job_id"]).all()
            logging.info(f"db results {result}")
            return result
    else:
        raise RuntimeError("task is missing job id!")
