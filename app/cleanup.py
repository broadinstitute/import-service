from app.db import db
import logging

from app.db.model import Import, ImportStatus


def clean_up_stale_imports():
    with db.session_ctx() as sess:
        stuck_jobs = sess.execute("""select id, status from imports where status NOT IN ('Error', 'Done', 'TimedOut') 
        and HOUR(TIMEDIFF(NOW(), submit_time)) > 36""")
        for job in stuck_jobs:
            logging.warning(f"Job id {job['id']} has status {job['status']} but is more than 36 hours old")
            Import.update_status_exclusively(job['id'], ImportStatus.from_string(job['status']), ImportStatus.TimedOut,
                                             sess)
