import logging
from typing import List

from .common import db
from .common.model import *

schema = {
    "task_id": "uuid",
    "job_id": "uuid",
    "path": "string"
}

# TODO: This is all still really placeholder.


def handle(message: dict) -> List[Import]:
    print(message)
    if "job_id" in message:
        logging.info(f'chunk_task: received message for job id {message["job_id"]}')
        print(f'chunk_task: received message for job id {message["job_id"]}')
        with db.session_ctx() as sess:
            result = sess.query(Import).filter(Import.id == message["job_id"]).all()
            logging.info(f"chunk_task: db results {result}")
            print(f"chunk_task: db results {result}")
            return result
    else:
        print("something went bad in chunk_task.handle")
        raise RuntimeError("task is missing job id!")
