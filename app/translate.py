import flask
import logging

from typing import Dict, Optional
from urllib.parse import urlparse

from app.util.exceptions import ISvcException

VALID_FILETYPES = ["pfb"]

VALID_NETLOCS = ["gen3-pfb-export.s3.amazonaws.com", "storage.googleapis.com"]



# TODO: head request to get size of import file, abort on extreme size?  Check if content-size is even returned;
# the signed url may use chunked transfer and therefore make this check impossible


def translate(attributes: Dict[str, str]) -> flask.Response:
    # the filetype attribute has been validated on ingest, so we don't need to revalidate it here.
    # at some point in the future this function will flesh out to ingest other filetypes.
    return pfb_to_rawls(attributes)


def pfb_to_rawls(attributes: Dict[str, str]) -> flask.Response:
    # extract path from attributes. json schema validation ensures that "path" exists, but we'll be safe
    pfbpath: Optional[str] = attributes.get("path")

    if pfbpath is None:
        logging.info(f"Missing path from inbound translate request: {attributes}")
        raise ISvcException("Missing path to PFB")

    # parse path into url parts, verify the netloc is one that we allow
    # TODO: should we validate netloc suffixes ("s3.amazonaws.com") instead of entire string matches?
    parsedurl = urlparse(pfbpath)
    if parsedurl.netloc not in VALID_NETLOCS:
        logging.warn(f"Unrecognized netloc for PFB import: {parsedurl.netloc} from {pfbpath}")
        raise ISvcException("PFB cannot be imported from this domain.")

    return flask.make_response("ok")

def validate_path(path: Optional[str]) -> bool:
    if path is None:
        logging.info(f"Missing path from inbound translate request: {attributes}")
        raise ISvcException("Missing path to PFB")

