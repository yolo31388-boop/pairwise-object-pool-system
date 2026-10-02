"""Pytest shim: plain (non-unittest.TestCase) test classes in this repo use
unittest-style assertions on ``self``. Inject them on plain collected classes
so the existing tests run unmodified."""

import unittest


def pytest_collection_modifyitems(items):
    for item in items:
        cls = getattr(item, "cls", None)
        if cls is None or issubclass(cls, unittest.TestCase):
            continue
        for name in ("assertIsNotNone", "assertIsNone", "assertTrue",
                     "assertFalse", "assertEqual"):
            if not hasattr(cls, name):
                setattr(cls, name, getattr(unittest.TestCase, name))
