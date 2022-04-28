import logging
import re
import urllib.request
import requests
from contextlib import contextmanager
from typing import IO, Iterator

from app.constants import TWO_GB_IN_BYTES
from app.util.exceptions import FileTooBigToDownlod


def extractBytes(content_range):
    # we expect value of the form `bytes 0-0/3268794`
    # if for some reason we change the Range request header to pull additional bytes this pattern needs to change too
    return int(re.sub(r'bytes 0-0/', '', content_range))


@contextmanager
def http_as_filelike(url: str, file_limit_bytes: int = TWO_GB_IN_BYTES) -> Iterator[IO]:
    """Open a file over HTTP and return it as a file-like object."""
    headers = {"Range": "bytes=0-0"}
    http_response = requests.get(url, headers=headers)
    content_range = http_response.headers.get('Content-Range')
    if content_range is None:
        logging.error(f"Content-Range header not returned by server for {url} we won't download")
        raise FileTooBigToDownlod
    if extractBytes(content_range) >= file_limit_bytes:
        raise FileTooBigToDownlod
    rq = urllib.request.Request(url)
    with urllib.request.urlopen(rq) as response:
        yield response
