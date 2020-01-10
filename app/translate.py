import flask
import logging

from typing import Dict, Optional
from urllib.parse import urlparse

from app.util.exceptions import ISvcException

VALID_FILETYPES = ["pfb"]

VALID_NETLOCS = ["gen3-pfb-export.s3.amazonaws.com", "storage.googleapis.com"]

def translate(attributes: Dict[str, str]) -> flask.Response:
    # the filetype attribute has been validated on ingest, so we don't need to revalidate it here.
    # at some point in the future this function will flesh out to ingest other filetypes.
    return pfb_to_rawls(attributes)


def pfb_to_rawls(attributes: Dict[str, str]) -> flask.Response:
    return flask.make_response("ok")

def validate_path(path: Optional[str]) -> bool:
    """Inspects the URI from which the user wants to import data. Because our service will make an
    outbound request to the user-supplied URI, we want to make sure that our service only visits
    safe and acceptable domains. Especially if we were to add authentication tokens to these outbound
    requests in the future, visiting arbitrary domains would allow malicious users to collect sensitive
    data. Therefore, we whitelist the domains we are willing to visit."""
    # json schema validation ensures that "path" exists, but we'll be safe
    if path is None:
        logging.info(f"Missing path from inbound translate request:")
        raise ISvcException("Missing path to PFB", 400)

    # parse path into url parts, verify the netloc is one that we allow
    # we may want to validate netloc suffixes ("s3.amazonaws.com") instead of entire string matches someday.
    parsedurl = urlparse(path)
    if parsedurl.netloc in VALID_NETLOCS:
        return True
    else:
        logging.warning(f"Unrecognized netloc for PFB import: {parsedurl.netloc} from {path}")
        raise ISvcException("PFB cannot be imported from this domain.", 400)
