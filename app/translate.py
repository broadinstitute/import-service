import flask
import logging

from app.db import db
from app.db.model import *
from app.util import http
from app.auth.userinfo import UserInfo
from app.util.exceptions import InvalidPathException

from typing import Dict, Optional, IO
from urllib.parse import urlparse


VALID_FILETYPES = ["pfb"]

VALID_NETLOCS = ["gen3-pfb-export.s3.amazonaws.com", "storage.googleapis.com"]

def translate(msg: Dict[str, str]) -> flask.Response:
    import_id = msg["import_id"]
    with db.session_ctx() as sess:
        # flip the status to Translating, and then get the row
        update_successful = Import.update_status_exclusively(import_id, ImportStatus.Pending, ImportStatus.Translating, sess)
        import_details: Import = sess.query(Import).filter(Import.id == import_id).first()

    if not update_successful:
        # this import wasn't in pending. most likely this means that the pubsub message we got was delivered twice,
        # and some other GAE instance has picked it up and is happily processing it. happy translating, friendo!
        return flask.make_response("ok")

    with http.http_as_filelike(import_details.import_url) as pfb_file:
        pass

    # TODO:
    # - determine destination (gs:// somewhere? a bucket we control?)
    import_details.workspace_namespace
    import_details.import_url

    # at some point in the future we'll be able to handle other filetypes.
    # note that the filetype attribute has been validated on ingest, so we don't need to revalidate it here.
    # for now, it's always pfb.
    return pfb_to_rawls(import_details)


def pfb_to_rawls(import_details: Import) -> flask.Response:
    # otherwise: actually do the translate.
    # if there's some error, flip the status to Error and put the message in a new column.

    return flask.make_response("ok")

def validate_import_url(import_url: Optional[str], user_info: UserInfo) -> bool:
    """Inspects the URI from which the user wants to import data. Because our service will make an
    outbound request to the user-supplied URI, we want to make sure that our service only visits
    safe and acceptable domains. Especially if we were to add authentication tokens to these outbound
    requests in the future, visiting arbitrary domains would allow malicious users to collect sensitive
    data. Therefore, we whitelist the domains we are willing to visit."""
    # json schema validation ensures that "import_url" exists, but we'll be safe
    if import_url is None:
        logging.info(f"Missing path from inbound translate request:")
        raise InvalidPathException(import_url, user_info, "Missing path to PFB")

    try:
        parsedurl = urlparse(import_url)
    except Exception as e:
        # catch any/all exceptions here so we can ensure audit logging
        raise InvalidPathException(import_url, user_info, f"{e}")

    # parse path into url parts, verify the netloc is one that we allow
    # we may want to validate netloc suffixes ("s3.amazonaws.com") instead of entire string matches someday.
    if parsedurl.netloc in VALID_NETLOCS:
        return True
    else:
        logging.warning(f"Unrecognized netloc for PFB import: [{parsedurl.netloc}] from [{import_url}]")
        raise InvalidPathException(import_url, user_info, "PFB cannot be imported from this domain.")
