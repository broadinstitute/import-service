import logging
import re
import urllib.request
import requests
from contextlib import contextmanager
from typing import IO, Iterator

from app.constants import TWO_GB_IN_BYTES
from app.util.exceptions import FileTooBigToDownload

BYTE_RANGE = "0-0"


def extractBytes(content_range: str) -> int:
    # we expect values of the form `bytes <start-end bytes>/<total-bytes>`
    return int(content_range.replace(f"bytes {BYTE_RANGE}/", ''))


@contextmanager
def http_as_filelike(url: str, file_limit_bytes: int = TWO_GB_IN_BYTES) -> Iterator[IO]:
    """Open a file over HTTP and return it as a file-like object."""
    headers = {"Range": f"bytes={BYTE_RANGE}"}
    http_response = requests.get(url, headers=headers)
    content_range = http_response.headers.get('Content-Range')
    if http_response.status_code != 206:
        logging.error(f"Content-Range header unexpectedly returned response code {http_response.status_code} for {url}")
        raise FileTooBigToDownload
    if content_range is None:
        logging.error(f"No Content-Range header provided from {url} we won't download")
        raise FileTooBigToDownload
    if extractBytes(content_range) >= file_limit_bytes:
        logging.error(f"Content-Range value {extractBytes(content_range)} exceeds {file_limit_bytes}")
        raise FileTooBigToDownload
    rq = urllib.request.Request(url)
    with urllib.request.urlopen(rq) as response:
        yield response
