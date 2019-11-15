import sqlalchemy.engine
from functions.common import db

schema = {
    "task_id" : "uuid",
    "job_id" : "uuid",
    "path" : "string"
}


def handle(message: dict, db: db.DBSession):
    if "job_id" in message:
        print(f"task has job id {message['job_id']}")
        result: sqlalchemy.engine.ResultProxy = db.execute("select * from imports")
        return result.fetchall()
    else:
        raise RuntimeError("task is missing job id!")
    return None
