from contextlib import contextmanager
from typing import IO, Iterator

from app.external import sam
from gcsfs.core import GCSFileSystem

# convenience function to read a GCS file as a user's pet SA.
# this method is broken out from translate.py to make it easy to mock in unit tests.

@contextmanager
def open_file(project: str, bucket: str, path: str, submitter: str) -> Iterator[IO]:
    pet_token = sam.admin_get_pet_token(project, submitter)
    manifest_fs = GCSFileSystem(project, token=pet_token)
    return manifest_fs.open(f"{bucket}{path}")
