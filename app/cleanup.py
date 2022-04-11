from app.db import db, model
import logging

from app.db.model import Import, ImportStatus


def clean_up_stale_imports(job_age_hours: int):
    with db.session_ctx() as sess:
        stuck_jobs = model.Import.get_stalled_imports(sess, job_age_hours)
        if len(stuck_jobs) > 0:
            logging.warning(f"We have {len(stuck_jobs)} imports that aren't in a terminal state "
                            f"after {job_age_hours} hours")
        for job in stuck_jobs:
            logging.warning(f"Job id {job.id} was submitted {job.submit_time}, has status {job.status} "
                            f"and is more than {job_age_hours} hours old")
            Import.update_status_exclusively(job.id, job.status, ImportStatus.TimedOut, sess)
