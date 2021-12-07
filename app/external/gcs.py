from typing import Iterator, IO
from app.external import sam

from gcsfs.core import GCSFileSystem


def open_file(project: str, bucket: str, path: str, submitter: str) -> Iterator[IO]:
    pet_token = sam.admin_get_pet_token(project, submitter)
    manifest_fs = GCSFileSystem(project, token=pet_token)
    return manifest_fs.open(f"{bucket}{path}")