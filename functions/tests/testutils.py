from contextlib import contextmanager
from typing import Optional, Iterator, Any
import unittest.mock as mock

@contextmanager
def patch_request(
        module_path: str,
        http_method: str,
        status_code: int,
        text: str = "",
        json: Optional[Any] = None) -> Iterator[mock.MagicMock]:
    """Wrapper for mock.patch over a python requests call."""
    fn_to_patch = f"{module_path}.requests.{http_method.lower()}"
    with mock.patch(fn_to_patch) as mocked_fn:
        mocked_fn.return_value.ok = status_code / 100 == 2
        mocked_fn.return_value.status_code = status_code
        mocked_fn.return_value.text = text
        if json is not None:
            mocked_fn.return_value.json.return_value = json
        yield mocked_fn
