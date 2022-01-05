import logging
from contextlib import contextmanager
from typing import IO, Any, Dict, Iterator

from app.external import sam
from gcsfs.core import GCSFileSystem

# convenience function to read a GCS file as a user's pet SA.
# this method is broken out from translate.py to make it easy to mock in unit tests.

@contextmanager
def open_file(project: str, bucket: str, path: str, submitter: str, auth_key: Dict[str, Any] = None) -> Iterator[IO]:
    if auth_key:
        logging.debug(f'using supplied auth key to read {path}')
    else:
        logging.debug(f'retrieving auth key from Sam to read {path}')
        auth_key = sam.admin_get_pet_key(project, submitter)

    manifest_fs = GCSFileSystem(project=project, token=auth_key)
    with manifest_fs.open(f"{bucket}{path}") as response:
        yield response
