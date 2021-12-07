import logging
import os
import traceback
from json import JSONEncoder
from time import time
from typing import IO, Dict, Optional
from urllib.parse import urlparse

import flask
import gcsfs.utils
import requests.exceptions
from gcsfs.core import GCSFileSystem

from app.auth import service_auth
from app.auth.userinfo import UserInfo
from app.db import db
from app.db.model import *
from app.external import gcs, pubsub
from app.translators import TDRManifestToRawls, PFBToRawls, Translator
from app.util import exceptions, http
from app.util.json import StreamArray

# these filetypes get stream-translated
FILETYPE_TRANSLATORS = {"pfb": PFBToRawls, "tdrexport": TDRManifestToRawls}

# this filetype is accepted as-is
FILETYPE_NOTRANSLATION = "rawlsjson"

VALID_NETLOCS = ["s3.amazonaws.com", "storage.googleapis.com", "service.azul.data.humancellatlas.org", "dev.singlecell.gi.ucsc.edu"]


def handle(msg: Dict[str, str]) -> ImportStatusResponse:
    import_id = msg["import_id"]
    with db.session_ctx() as sess:
        # flip the status to Translating, and then get the row
        update_successful = Import.update_status_exclusively(import_id, ImportStatus.Pending, ImportStatus.Translating, sess)
        import_details: Import = Import.get(import_id, sess)

    if not update_successful:
        # this import wasn't in pending. most likely this means that the pubsub message we got was delivered twice,
        # and some other GAE instance has picked it up and is happily processing it. happy translating, friendo!
        logging.info(f"Failed to update status exclusively for translating import {import_id}: expected Pending, got {import_details.status}. PubSub probably delivered this message twice.")
        return flask.make_response(f"Failed to update status exclusively for translating import {import_id}: expected Pending, got {import_details.status}. PubSub probably delivered this message twice.", 409) # type: ignore

    dest_file = f'{os.environ.get("BATCH_UPSERT_BUCKET")}/{import_details.id}.rawlsUpsert'

    logging.info(f"Starting translation for import {import_id} from {import_details.import_url} to {dest_file} ...")
    try:
        gcs_project = GCSFileSystem(os.environ.get("PUBSUB_PROJECT"), token=service_auth.get_isvc_credential())

        if import_details.filetype == FILETYPE_NOTRANSLATION:
            logging.info(f"import {import_id} is of type {import_details.filetype}; attempting copy from {import_details.import_url} to {dest_file} ...")
            # no need to stream-translate, we just move the file from its incoming location to
            # its final destination; the final destination includes the job id
            gcs_project.mv(import_details.import_url, dest_file)
        else:
            logging.info(f"import {import_id} is of type {import_details.filetype}; attempting stream-translate ...")

            parsedurl = urlparse(import_details.import_url)
            if import_details.filetype == "tdrexport" and parsedurl.scheme == "gs":
                filereader = gcs.open_file(import_details.workspace_google_project, parsedurl.netloc, parsedurl.path, import_details.submitter)
            else:
                filereader = http.http_as_filelike(import_details.import_url)

            with filereader as pfb_file:
                with gcs_project.open(dest_file, 'wb') as dest_upsert:
                    _stream_translate(import_id, pfb_file, dest_upsert, import_details.filetype, translator = FILETYPE_TRANSLATORS[import_details.filetype]())

    except (FileNotFoundError, IOError, gcsfs.utils.HttpError, requests.exceptions.ProxyError) as e:
        # These are errors thrown by the gcsfs library, see here:
        #   https://github.com/dask/gcsfs/blob/d7b832e13de6b5b0df00eeb7454c6547bf30d7b9/gcsfs/core.py#L151
        # Any of these indicate programmer error: import-service can't write to the batchUpsert json bucket, which
        # is probably a service account permissions issue.
        # Note that we open the import URL using urllib's urlopen, which raises subclasses of URLError,
        # so we're not at risk of confusing import failures with bucket write failures.
        logging.error(f"Read/write error during translation for import {import_id}: {traceback.format_exc()}")
        raise exceptions.SystemException([import_details], e)
    except Exception as e:
        # Something went wrong with the translate. Raising an exception will fail the import.
        # Over time we should be able to narrow down the kinds of exception we might get, and perhaps
        # give users clearer messaging instead of logging them all.
        # For now, this is a last-ditch catch-all.
        logging.error(f"Unexpected error during translation for import {import_id}: {traceback.format_exc()}")
        raise exceptions.FileTranslationException(import_details, e)

    with db.session_ctx() as sess:
        # This should always succeed as we started this function by getting an exclusive lock on the import row.
        Import.update_status_exclusively(import_id, ImportStatus.Translating, ImportStatus.ReadyForUpsert, sess)

    logging.info(f"Completed translation for import {import_id} from {import_details.import_url} to {dest_file}")
    logging.info(f"Requesting Rawls upsert for import {import_id}...")

    # Tell Rawls to import the result.
    pubsub.publish_rawls({
        "workspaceNamespace": import_details.workspace_namespace,
        "workspaceName": import_details.workspace_name,
        "userEmail": import_details.submitter,
        "jobId": import_details.id,
        "upsertFile": dest_file,
        "isUpsert": str(import_details.is_upsert)
    })

    return ImportStatusResponse(import_id, ImportStatus.ReadyForUpsert.name, None)


def _stream_translate(import_id: str, source: IO, dest: IO, file_type: str, translator: Translator) -> None:
    translated_gen = translator.translate(source, file_type)  # doesn't actually translate, just returns a generator

    start_time = time()
    last_log_time = time()
    num_chunks = 0

    for chunk in JSONEncoder(indent=0).iterencode(StreamArray(translated_gen)):
        chunk_time = time()
        num_chunks = num_chunks + 1
        if (chunk_time - last_log_time >= 30):
            elapsed = chunk_time - start_time
            logging.info(f"still translating for import {import_id}: total time {elapsed}s, chunks processed {num_chunks}")
            last_log_time = chunk_time

        dest.write(chunk.encode())  # encodes as utf-8 by default

def validate_import_url(import_url: Optional[str], import_filetype: Optional[str], user_info: UserInfo) -> bool:
    """Inspects the URI from which the user wants to import data. Because our service will make an
    outbound request to the user-supplied URI, we want to make sure that our service only visits
    safe and acceptable domains. Especially if we were to add authentication tokens to these outbound
    requests in the future, visiting arbitrary domains would allow malicious users to collect sensitive
    data. Therefore, we whitelist the domains we are willing to visit."""
    # json schema validation ensures that "import_url" exists, but we'll be safe
    if import_url is None:
        logging.info(f"Missing path from inbound translate request:")
        raise exceptions.InvalidPathException(import_url, user_info, "Missing path to file to import")

    # json schema validation ensures that "import_filetype" exists, but we'll be safe
    if import_filetype is None:
        logging.info(f"Missing filetype from inbound translate request:")
        raise exceptions.InvalidFiletypeException(import_filetype, user_info, "Missing filetype")

    try:
        parsedurl = urlparse(import_url)
    except Exception as e:
        # catch any/all exceptions here so we can ensure audit logging
        raise exceptions.InvalidPathException(import_url, user_info, f"{e}")

    # parse path into url parts, verify the netloc is one that we allow
    # we validate netloc suffixes ("s3.amazonaws.com") instead of entire string matches; this allows
    # for subdomains of the netlocs we deem safe.
    # for "rawlsjson" requests, we validate that the file-to-be-imported is already in our
    # dedicated bucket.
    actual_netloc = parsedurl.netloc
    if import_filetype == FILETYPE_NOTRANSLATION and actual_netloc == os.environ.get("BATCH_UPSERT_BUCKET"):
        return True
    elif import_filetype in FILETYPE_TRANSLATORS.keys() and any(actual_netloc.endswith(s) for s in VALID_NETLOCS):
        return True
    else:
        logging.warning(f"Unrecognized netloc or bucket for import: [{parsedurl.netloc}] from [{import_url}]")
        raise exceptions.InvalidPathException(import_url, user_info, "File cannot be imported from this URL.")
