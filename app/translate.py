import flask
import logging

from typing import Dict, Optional
from urllib.parse import urlparse

from app.auth.userinfo import UserInfo
from app.util.exceptions import InvalidPathException

VALID_FILETYPES = ["pfb"]

VALID_NETLOCS = ["gen3-pfb-export.s3.amazonaws.com", "storage.googleapis.com"]

def translate(attributes: Dict[str, str]) -> flask.Response:
    # the filetype attribute has been validated on ingest, so we don't need to revalidate it here.
    # at some point in the future this function will flesh out to ingest other filetypes.
    return pfb_to_rawls(attributes)


def pfb_to_rawls(attributes: Dict[str, str]) -> flask.Response:
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
