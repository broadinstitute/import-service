import logging
import os
import traceback
from dataclasses import asdict
from json import JSONEncoder
from time import time
from typing import IO, Dict, Optional
from urllib.parse import urlparse

import flask
import gcsfs.retry
import requests.exceptions
from gcsfs.core import GCSFileSystem

from app.auth import service_auth
from app.auth.userinfo import UserInfo
from app.db import db
from app.db.model import Import, ImportStatus, ImportStatusResponse
from app.external import gcs, pubsub
from app.translators import PFBToRawls, TDRManifestToRawls, Translator
from app.util import exceptions, http
from app.util.json import StreamArray

# these filetypes get stream-translated
FILETYPE_TRANSLATORS = {"pfb": PFBToRawls, "tdrexport": TDRManifestToRawls}

# this filetype is accepted as-is
FILETYPE_NOTRANSLATION = "rawlsjson"

VALID_TDR_SCHEMES = ["gs", "https"]


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
            if import_details.filetype == "tdrexport" and parsedurl.scheme in VALID_TDR_SCHEMES:
                if parsedurl.scheme == "gs":
                    filereader = gcs.open_file(import_details.workspace_google_project, parsedurl.netloc, parsedurl.path, import_details.submitter)
                elif parsedurl.scheme == "https":
                    filereader = http.http_as_filelike(import_details.import_url)
                else:
                    # This state should never be reached since the request is validated when it is first submitted
                    logging.error(f"unsupported scheme {parsedurl.scheme} provided")
                    raise exceptions.InvalidPathException(import_details.import_url,
                                                          UserInfo("---", import_details.submitter, True),
                                                          "File cannot be imported from this URL.")

            else:
                filereader = http.http_as_filelike(import_details.import_url)

            with filereader as pfb_file:
                with gcs_project.open(dest_file, 'wb') as dest_upsert:
                    _stream_translate(import_details, pfb_file, dest_upsert, translator = FILETYPE_TRANSLATORS[import_details.filetype]())

    except (FileNotFoundError, IOError, gcsfs.retry.HttpError, requests.exceptions.ProxyError) as e:
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

    return ImportStatusResponse(import_id, ImportStatus.ReadyForUpsert.name, import_details.filetype, None)


def _stream_translate(import_details: Import, source: IO, dest: IO, translator: Translator) -> None:
    translated_entity_gen = translator.translate(import_details, source)  # doesn't actually translate, just returns a generator
    # translated_entity_gen returns an Iterator[Entity]. Turn those Entity objects into dicts so they can be json-encoded
    translated_gen = (asdict(e) for e in translated_entity_gen)

    start_time = time()
    last_log_time = time()
    num_chunks = 0

    for chunk in JSONEncoder(indent=0).iterencode(StreamArray(translated_gen)):
        chunk_time = time()
        num_chunks = num_chunks + 1
        if (chunk_time - last_log_time >= 30):
            elapsed = chunk_time - start_time
            logging.info(f"still translating for import {import_details.id}: total time {elapsed}s, chunks processed {num_chunks}")
            last_log_time = chunk_time

        dest.write(chunk.encode())  # encodes as utf-8 by default
