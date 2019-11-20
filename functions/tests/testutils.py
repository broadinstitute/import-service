from contextlib import contextmanager
from typing import Optional, Iterator, Any
import pytest
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


def fxpatch(target: str, **kwargs) -> str:
    """Dynamically generates a pytest fixture that monkeypatches the target.
    The contents of **kwargs are as for mock.MagicMock.
    The intention here is that you use this in a @pytest.mark.usefixtures() decorator, e.g.:
    @pytest.mark.usefixtures(
        testutils.fxpatch("foo.bar.baz", return_value=42),
        testutils.fxpatch("foo.bar.qux", side_effect=KeyError))
    def test_function():
        import bar
        assert bar.baz() == 42
        with pytest.raises(KeyError):
            bar.qux()"""
    mm = mock.MagicMock(**kwargs)
    @pytest.fixture
    def hx_fixture(monkeypatch):
        monkeypatch.setattr(target, mm)
    fxname = f"hx_fixture_{id(mm)}"
    # In order for pytest to find this newly generated fixture, it has to be added to the module-level globals
    # in the test file: https://github.com/pytest-dev/pytest/issues/2424#issuecomment-333387206
    # But the test file is not this function -- it is the caller. So we have to peek up the stack to poke it in.
    import inspect
    inspect.stack()[1][0].f_globals[fxname] = hx_fixture
    return fxname
