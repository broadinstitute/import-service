from app.db import db, model
import logging

from app.db.model import Import, ImportStatus


def clean_up_stale_imports():
    with db.session_ctx() as sess:
        stuck_jobs = model.Import.get_stalled_imports(sess)
        for job in stuck_jobs:
            logging.warning(f"Job id {job.get('id')} has status {job.get('status')} but is more than 36 hours old")
            Import.update_status_exclusively(job.get('id'), ImportStatus.from_string(job.get('status')),
                                             ImportStatus.TimedOut, sess)
