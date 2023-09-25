import os
from unittest import mock

import pytest

from app import protected_data

@mock.patch.dict(os.environ, {"IMPORT_RESTRICTED_SOURCES": "s3://test-bucket-1,s3://test-bucket-2"})
def test_get_restricted_url_patterns():
  assert protected_data.get_restricted_url_patterns() == [
    *protected_data.url_patterns_for_s3_bucket("test-bucket-1"),
    *protected_data.url_patterns_for_s3_bucket("test-bucket-2"),
  ]

@mock.patch.object(protected_data, "RESTRICTED_URL_PATTERNS", protected_data.url_patterns_for_s3_bucket("restricted-bucket"))
@pytest.mark.parametrize("import_url,expected_is_restricted", [
  ("https://s3.amazonaws.com/restricted-bucket/path/to/file.pfb", True),
  ("https://s3.amazonaws.com/other-bucket/path/to/file.pfb", False),
])
def test_is_restricted_import(import_url, expected_is_restricted):
  assert protected_data.is_restricted_import(import_url) == expected_is_restricted
