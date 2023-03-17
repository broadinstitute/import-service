import logging
import traceback
from contextlib import contextmanager
from typing import IO, Any, Dict, Iterator

from app.constants import TWO_GB_IN_BYTES
from app.external import sam
from gcsfs.core import GCSFileSystem

from app.util.exceptions import FileTooBigToDownload


# convenience function to read a GCS file as a user's pet SA
# this method is broken out from translate.py to make it easy to mock in unit tests
@contextmanager
def open_file(project: str, bucket: str, path: str, submitter: str, auth_key: Dict[str, Any] = None,  # type: ignore
              file_limit_bytes: int = TWO_GB_IN_BYTES, gcsfs: GCSFileSystem = None) -> Iterator[IO]:

    if auth_key:
        logging.debug(f'using supplied auth key to read {path}')
    else:
        logging.debug(f'retrieving auth key from Sam to read {path}')
        auth_key = sam.admin_get_pet_key(project, submitter)

    try:
        fs = GCSFileSystem(project=project, token=auth_key) if (gcsfs is None) else gcsfs
        # du() would seem to be a more straightforward option, but it requires permissions that TDR doesn't grant,
        # so we use info, if size metadata is not present don't download
        if fs.info(f"{bucket}{path}").get('size', file_limit_bytes) >= file_limit_bytes:
            raise FileTooBigToDownload
        with fs.open(f"{bucket}{path}") as response:
            yield response
    except Exception as e:
        # log and rethrow
        logging.error(f"Error reading {bucket}{path} from GCS : {traceback.format_exc()}")
        raise e
