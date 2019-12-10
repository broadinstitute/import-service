"""Dummy module for testing mocks and patches."""
import requests


def dummy(d: str) -> str:
    return f"dummy {d}"


def request() -> requests.Response:
    return requests.get("www.example.com")
