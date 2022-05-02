from unittest.mock import patch

import pytest
from _pytest.outcomes import fail

from app.external import gcs
from app.util import exceptions


@patch('gcsfs.core.GCSFileSystem')
def test_we_throw_exception_if_file_too_big(mock_gcs):
    mock_gcs.info.return_value = {'size': 101}
    with pytest.raises(exceptions.FileTooBigToDownload):
        with gcs.open_file('foo', 'bar', 'path', 'user', file_limit_bytes=100, auth_key={'key': 'val'}, gcsfs=mock_gcs):
            fail("Should have thrown exception just before this")


@patch('gcsfs.core.GCSFileSystem')
def test_no_exception_if_file_small_enough(mock_gcs):
    mock_gcs.info.return_value = {'size': 99}
    with gcs.open_file('foo', 'bucket', 'path', 'user', file_limit_bytes=100, auth_key={'key': 'val'}, gcsfs=mock_gcs):
        mock_gcs.open.assert_called_with('bucketpath')