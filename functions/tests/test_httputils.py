import pytest

from ..common import exceptions
from ..common.httputils import _part_to_regex, _pattern_to_regex, expect_urlshape


def test_part_to_regex():
    assert _part_to_regex("foo") == "foo"
    assert _part_to_regex("<boo>") == r"(?P<boo>[\w-]+)"


def test_pattern_to_regex():
    assert _pattern_to_regex("/foo/boo") == r"/foo/boo"
    assert _pattern_to_regex("/foo/<boo>/woo") == r"/foo/(?P<boo>[\w-]+)/woo"


def test_expect_urlshape():
    assert expect_urlshape("/foo", "/foo") == {}

    with pytest.raises(exceptions.NotFoundException):
        expect_urlshape("/foo", "/boo")

    assert expect_urlshape("/foo/<boo>/zoo", "/foo/woo-woo/zoo") == {"boo": "woo-woo"}

    with pytest.raises(exceptions.NotFoundException):
        expect_urlshape("/foo/<boo>/zoo", "foo/woo/")
