from app.db import db, model
import logging

from app.db.model import Import, ImportStatus


def clean_up_stale_imports(job_age_hours: int):
    with db.session_ctx() as sess:
        stuck_jobs = model.Import.get_stalled_imports(sess, job_age_hours)
        for job in stuck_jobs:
            logging.warning(f"Job id {job.id} has status {job.status} but is more than {job_age_hours} hours old")
            Import.update_status_exclusively(job.id, job.status, ImportStatus.TimedOut, sess)
