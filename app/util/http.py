import urllib.request
from contextlib import contextmanager
from typing import IO


@contextmanager
def http_as_filelike(url: str) -> IO:
    """Open a file over HTTP and return it as a file-like object."""
    rq = urllib.request.Request(url)
    with urllib.request.urlopen(rq) as response:
        yield response
