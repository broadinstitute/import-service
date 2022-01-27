import logging
import traceback
from contextlib import contextmanager
from typing import IO, Any, Dict, Iterator

from pyarrow.filesystem import FileSystem

from app.external import sam
from gcsfs.core import GCSFileSystem

# convenience function to read a GCS file as a user's pet SA.
# this method is broken out from translate.py to make it easy to mock in unit tests.

@contextmanager
def open_file(project: str, bucket: str, path: str, submitter: str, auth_key: Dict[str, Any] = None) -> Iterator[IO]:
    try:
        manifest_fs = get_gcs_filesystem(project, submitter, auth_key)
        manifest_fs.expand_path
        manifest_fs.split_path
        with manifest_fs.open(f"{bucket}{path}") as response:
            yield response
    except Exception as e:
        # log and rethrow
        logging.error(f"Error reading {bucket}{path} from GCS : {traceback.format_exc()}")
        raise e

def get_gcs_filesystem(project: str, submitter: str, auth_key: Dict[str, Any] = None) -> FileSystem:
    if auth_key:
        logging.debug(f'using supplied auth key')
    else:
        logging.debug(f'retrieving auth key from Sam')
        auth_key = sam.admin_get_pet_key(project, submitter)

    try:
        return GCSFileSystem(project=project, token=auth_key)
    except Exception as e:
        # log and rethrow
        logging.error(f"Error reading filesystem for project {project} from GCS : {traceback.format_exc()}")
        raise e
