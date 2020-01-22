import flask
import logging

from app.auth import service_auth
from app.auth.userinfo import UserInfo
from app.db import db
from app.db.model import *
from app.external import pubsub
from app.translators import Translator, PFBToRawls
from app.util import http, exceptions
from app.util.json import StreamArray

from typing import Dict, Optional, IO

from urllib.parse import urlparse
import os
import traceback

from json import JSONEncoder

from gcsfs.core import GCSFileSystem

FILETYPE_TRANSLATORS = {"pfb": PFBToRawls}

VALID_NETLOCS = ["gen3-pfb-export.s3.amazonaws.com", "storage.googleapis.com"]


def translate(msg: Dict[str, str]) -> flask.Response:
    import_id = msg["import_id"]
    with db.session_ctx() as sess:
        # flip the status to Translating, and then get the row
        update_successful = Import.update_status_exclusively(import_id, ImportStatus.Pending, ImportStatus.Translating, sess)
        import_details: Import = Import.reacquire(import_id, sess)

    if not update_successful:
        # this import wasn't in pending. most likely this means that the pubsub message we got was delivered twice,
        # and some other GAE instance has picked it up and is happily processing it. happy translating, friendo!
        return flask.make_response("ok")

    dest_file = f'{os.environ.get("BATCH_UPSERT_BUCKET")}/{import_details.id}.rawlsUpsert'

    with http.http_as_filelike(import_details.import_url) as pfb_file:

        gcsfs = GCSFileSystem(os.environ.get("PUBSUB_PROJECT"), token=service_auth.get_isvc_credential())
        with gcsfs.open(dest_file, 'wb') as dest_upsert:

            try:
                _stream_translate(pfb_file, dest_upsert, translator = FILETYPE_TRANSLATORS[import_details.filetype]())
            except Exception as e:
                # Something went wrong with the translate. Raising an exception will fail the import.
                # Over time we should be able to narrow down the kinds of exception we might get, and perhaps
                # give users clearer messaging instead of logging them all.
                eid = uuid.uuid4()
                logging.warn(f"eid {eid}: \n{traceback.format_exc()}")
                raise exceptions.ISvcException(f"Error translating file: {import_details.import_url}\n" + \
                                               f"{e.__class__.__name__}\n" + \
                                               f"eid: {str(eid)}")

    # Tell Rawls to import the result.
    pubsub.publish_rawls({
        "workspaceNamespace": import_details.workspace_namespace,
        "workspaceName": import_details.workspace_name,
        "userEmail": import_details.submitter,
        # "userSubjectId": do_we_really_though?,
        "jobId": import_details.id,
        "upsertFile": dest_file
    })

    with db.session_ctx() as sess:
        # This should always succeed as we started this function by getting an exclusive lock on the import row.
        Import.update_status_exclusively(import_id, ImportStatus.Translating, ImportStatus.Upserting, sess)

    return flask.make_response("ok")


def _stream_translate(source: IO, dest: IO, translator: Translator) -> None:
    translated_gen = translator.translate(source)  # doesn't actually translate, just returns a generator

    for chunk in JSONEncoder(indent=0).iterencode(StreamArray(translated_gen)):
        dest.write(chunk.encode())  # encodes as utf-8 by default


def validate_import_url(import_url: Optional[str], user_info: UserInfo) -> bool:
    """Inspects the URI from which the user wants to import data. Because our service will make an
    outbound request to the user-supplied URI, we want to make sure that our service only visits
    safe and acceptable domains. Especially if we were to add authentication tokens to these outbound
    requests in the future, visiting arbitrary domains would allow malicious users to collect sensitive
    data. Therefore, we whitelist the domains we are willing to visit."""
    # json schema validation ensures that "import_url" exists, but we'll be safe
    if import_url is None:
        logging.info(f"Missing path from inbound translate request:")
        raise exceptions.InvalidPathException(import_url, user_info, "Missing path to PFB")

    try:
        parsedurl = urlparse(import_url)
    except Exception as e:
        # catch any/all exceptions here so we can ensure audit logging
        raise exceptions.InvalidPathException(import_url, user_info, f"{e}")

    # parse path into url parts, verify the netloc is one that we allow
    # we may want to validate netloc suffixes ("s3.amazonaws.com") instead of entire string matches someday.
    if parsedurl.netloc in VALID_NETLOCS:
        return True
    else:
        logging.warning(f"Unrecognized netloc for PFB import: [{parsedurl.netloc}] from [{import_url}]")
        raise exceptions.InvalidPathException(import_url, user_info, "PFB cannot be imported from this domain.")
