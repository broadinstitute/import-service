import urllib.request
import requests
from contextlib import contextmanager
from typing import IO, Iterator

from app.util.exceptions import FileTooBigToDownlod


@contextmanager
def http_as_filelike(url: str, file_limit_bytes: int = 2147483648) -> Iterator[IO]:
    """Open a file over HTTP and return it as a file-like object."""
    http_response = requests.head(url)
    if int(http_response.headers.get('content-length', file_limit_bytes)) >= file_limit_bytes:
        raise FileTooBigToDownlod
    rq = urllib.request.Request(url)
    with urllib.request.urlopen(rq) as response:
        yield response
