import flask
from typing import Dict


VALID_FILETYPES = ["pfb"]


def translate(attributes: Dict[str, str]) -> flask.Response:
    # the filetype attribute has been validated on ingest, so we don't need to revalidate it here.
    # at some point in the future this function will flesh out to ingest other filetypes.
    return pfb_to_rawls(attributes)


def pfb_to_rawls(attributes: Dict[str, str]) -> flask.Response:
    return flask.make_response("ok")
