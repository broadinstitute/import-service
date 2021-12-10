from contextlib import contextmanager
from typing import IO, Iterator

from app.external import sam
from gcsfs.core import GCSFileSystem

# convenience function to read a GCS file as a user's pet SA.
# this method is broken out from translate.py to make it easy to mock in unit tests.

@contextmanager
def open_file(project: str, bucket: str, path: str, submitter: str) -> Iterator[IO]:
    pet_key = sam.admin_get_pet_key(project, submitter)
    manifest_fs = GCSFileSystem(project=project, token=pet_key)
    with manifest_fs.open(f"{bucket}{path}") as response:
        yield response
