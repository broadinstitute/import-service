import logging
import traceback
from contextlib import contextmanager
from typing import IO, Any, Dict, Iterator

from app.external import sam
from gcsfs.core import GCSFileSystem

from app.util.exceptions import GcsFileTooLargeException


@contextmanager
def open_file(project: str, bucket: str, path: str, submitter: str, auth_key: Dict[str, Any] = None,
              file_limit_bytes: int = 2147483648, gcsfs: GCSFileSystem = None) -> Iterator[IO]:
    """
    convenience function to read a GCS file as a user's pet SA
    this method is broken out from translate.py to make it easy to mock in unit tests
    Args:
        project: google project
        bucket: gcs bucket e.g.  gs://<bucket-name>
        path: file name within the bucket
        submitter: Sam user
        auth_key: one of those Google auth key dicts
        file_limit_bytes: if the file's bytes exceed this value, throw an exception and cease processing
        gcsfs: Google bucket api--for passing a mock Gcsfs for testing

    Returns:

    """
    if auth_key:
        logging.debug(f'using supplied auth key to read {path}')
    else:
        logging.debug(f'retrieving auth key from Sam to read {path}')
        auth_key = sam.admin_get_pet_key(project, submitter)

    try:
        fs = GCSFileSystem(project=project, token=auth_key) if (gcsfs is None) else gcsfs
        if fs.du(f"{bucket}{path}") > file_limit_bytes:
            raise GcsFileTooLargeException
        with fs.open(f"{bucket}{path}") as response:
            yield response
    except Exception as e:
        # log and rethrow
        logging.error(f"Error reading {bucket}{path} from GCS : {traceback.format_exc()}")
        raise e
