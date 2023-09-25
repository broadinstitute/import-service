import flask
import json
import logging

from app.translate import FILETYPE_TRANSLATORS, FILETYPE_NOTRANSLATION
from app.db import db, model
from app.external import gcs, sam, pubsub
from app.external.tdr_model import TDRManifest
from app.auth import user_auth
from app.util import exceptions, http

from pydantic import AnyUrl, validate_arguments
from typing import Optional, Set
from urllib.parse import ParseResult, urlparse
import os

from app.auth.userinfo import UserInfo
from app import protected_data

# Allow downloads from any GCS bucket, Azure storage container, or S3 bucket
VALID_NETLOCS = [
    "storage.googleapis.com",
    "*.core.windows.net",
    # S3 allows multiple URL formats
    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html
    "s3.amazonaws.com", # path style legacy global endpoint
    "*.s3.amazonaws.com", # virtual host style legacy global endpoint
]

# Allow configuration to specify additional netlocs from which imports are allowed.
additional_valid_netlocs = os.getenv("IMPORT_ALLOWED_NETLOCS")
if additional_valid_netlocs:
    VALID_NETLOCS += [s.strip() for s in additional_valid_netlocs.split(",")]

def is_valid_netloc(parsed_url: ParseResult) -> bool:
    for valid_netloc in VALID_NETLOCS:
        if valid_netloc[0] == "*" and parsed_url.netloc.endswith(valid_netloc[1:]):
            return True
        elif parsed_url.netloc == valid_netloc:
            return True

    return False

def handle(request: flask.Request, ws_ns: str, ws_name: str) -> model.ImportStatusResponse:
    access_token = user_auth.extract_auth_token(request)
    user_info = sam.validate_user(access_token)

    # force parsing as json regardless of application/content-type, return None if errors
    request_json_opt = request.get_json(force=True, silent=True)

    if not isinstance(request_json_opt, dict):
        raise exceptions.BadJsonException("Input payload is not valid", audit_log = True)

    request_json: dict = request_json_opt

    # make sure the user is allowed to import to this workspace
    uuid_and_project = user_auth.workspace_uuid_and_project_with_auth(ws_ns, ws_name, access_token, "write")
    workspace_uuid = uuid_and_project.workspace_id
    google_project = uuid_and_project.google_project
    authorization_domain = uuid_and_project.authorization_domain
    bucket_name = uuid_and_project.bucket_name

    import_url = request_json["path"]
    import_filetype = request_json["filetype"]
    import_is_upsert = request_json.get("isUpsert", "true") # default to true if missing, to support legacy imports
    options = request_json.get("options",{})
    is_tdr_sync_required = options.get("tdrSyncPermissions", False) # default to not sync permissions

    logging.info(f"New import received for {import_url}, {import_filetype}, is_upsert: {import_is_upsert}, \
        options: {options}, tdrSyncFlag: {is_tdr_sync_required}")

    # and validate the input's path
    validate_import_url(import_url, import_filetype, user_info)

    # Refuse imports from restricted sources
    if protected_data.is_restricted_import(import_url):
        raise exceptions.AuthorizationException("Unable to import data from this source into this Terra environment")

    # Refuse to import protected data into unprotected workspace
    if is_protected_data(import_url, import_filetype, google_project=google_project, user_info=user_info):
        if not is_protected_workspace(authorization_domain, bucket_name):
            raise exceptions.AuthorizationException("Unable to import protected data into an unprotected workspace")

    # parse is_upsert from a str into a bool
    is_upsert = str(import_is_upsert).strip().lower() == "true"

    new_import = model.Import(
        workspace_name=ws_name,
        workspace_ns=ws_ns,
        workspace_uuid=workspace_uuid,
        workspace_google_project=google_project,
        submitter=user_info.user_email,
        import_url=import_url,
        filetype=import_filetype,
        is_upsert=is_upsert,
        is_tdr_sync_required=is_tdr_sync_required)

    with db.session_ctx() as sess:
        sess.add(new_import)
        new_import_id = new_import.id

    pubsub.publish_self({"action": "translate", "import_id": new_import_id})

    return new_import.to_status_response()

def is_protected_workspace(authorization_domain: Optional[Set[str]], bucket_name: Optional[str]):
    if authorization_domain and len(authorization_domain) > 0:
        return True
    return bucket_name and bucket_name.startswith("fc-secure")

@validate_arguments
def _validate_url(url: AnyUrl) -> str:
    return str(url)

def validate_and_parse_url(url: str) -> ParseResult:
    """Validates the input URL using pydantic and parses it with urlparse."""
    # Mypy expects _validate_url to take an AnyUrl argument, but it actually can accept a string, which pydantic
    # will coerce into an AnyUrl. Breaking this up into two functions keeps the type: ignore confined here.
    return urlparse(_validate_url(url)) # type: ignore

def validate_import_url(import_url: Optional[str], import_filetype: Optional[str], user_info: UserInfo) -> str:
    """Inspects the URI from which the user wants to import data. Because our service will make an
    outbound request to the user-supplied URI, we want to make sure that our service only visits
    safe and acceptable domains. Especially if we were to add authentication tokens to these outbound
    requests in the future, visiting arbitrary domains would allow malicious users to collect sensitive
    data. Therefore, we whitelist the domains we are willing to visit.  If no error found with import url,
    return the url host for further validation"""
    # json schema validation ensures that "import_url" exists, but we'll be safe
    if import_url is None:
        logging.info("Missing path from inbound translate request:")
        raise exceptions.InvalidPathException(import_url, user_info, "Missing path to file to import")

    # json schema validation ensures that "import_filetype" exists, but we'll be safe
    if import_filetype is None:
        logging.info("Missing filetype from inbound translate request:")
        raise exceptions.InvalidFiletypeException(import_filetype, user_info, "Missing filetype")

    try:
        parsedurl = validate_and_parse_url(import_url)
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
        return actual_netloc
    elif import_filetype in FILETYPE_TRANSLATORS.keys() and is_valid_netloc(parsedurl):
        return actual_netloc
    elif import_filetype == "tdrexport" and parsedurl.scheme == "gs":
        return actual_netloc
    else:
        logging.warning(f"Unrecognized netloc or bucket for import: [{parsedurl.netloc}] from [{import_url}]")
        raise exceptions.InvalidPathException(import_url, user_info, "File cannot be imported from this URL.")

def is_protected_data(import_url: str, import_filetype: str, *, google_project: str, user_info: UserInfo) -> bool:
    """Determines whether an import is protected data based on where it's imported from
    and its filetype."""
    
    if import_filetype == "pfb":
        return any(pattern.match(import_url) for pattern in protected_data.PROTECTED_URL_PATTERNS)

    elif import_filetype == "tdrexport":
        parsed_url = urlparse(import_url)
        if parsed_url.scheme == "gs":
            filereader = gcs.open_file(google_project, parsed_url.netloc, parsed_url.path, user_info.user_email)
        elif parsed_url.scheme == "https":
            filereader = http.http_as_filelike(import_url)
        else:
            # This case should never be reached since the request is validated before this function is called
            logging.error(f"unsupported scheme {parsed_url.scheme} provided for TDR export")
            raise exceptions.InvalidPathException(import_url, user_info, "File cannot be imported from this URL.")
        
        with filereader as manifest_file:
            manifest_json = json.load(manifest_file)

        manifest = TDRManifest(**manifest_json)
        return any(source.dataset.secureMonitoringEnabled for source in manifest.snapshot.source)


    return False
