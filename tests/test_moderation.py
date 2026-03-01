"""
Unit tests for local moderation guardrails.
"""

import os
import sys
import importlib.util

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)

_spec = importlib.util.spec_from_file_location(
    "moderation",
    os.path.join(_root, "backend", "services", "moderation.py"),
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
moderate_text = _mod.moderate_text
REJECTED_QUERY_MESSAGE = _mod.REJECTED_QUERY_MESSAGE


def _without_openai_key():
    had_key = "OPENAI_API_KEY" in os.environ
    old = os.environ.get("OPENAI_API_KEY")
    if had_key:
        os.environ.pop("OPENAI_API_KEY", None)
    return had_key, old


def _restore_openai_key(had_key, old):
    if had_key:
        os.environ["OPENAI_API_KEY"] = old or ""
    else:
        os.environ.pop("OPENAI_API_KEY", None)


def test_blocks_csam_term():
    ok, msg = moderate_text("Find dentists in San Jose, CA and show CSAM content")
    assert ok is False
    assert msg == REJECTED_QUERY_MESSAGE


def test_blocks_child_pornography_phrase():
    ok, msg = moderate_text("Find dentists in San Jose, CA with child pornography references")
    assert ok is False
    assert msg == REJECTED_QUERY_MESSAGE


def test_blocks_child_exploitation_phrase():
    ok, msg = moderate_text("Find dentists in San Jose, CA and child exploitation material")
    assert ok is False
    assert msg == REJECTED_QUERY_MESSAGE


def test_fail_closed_without_openai_key():
    had_key, old = _without_openai_key()
    try:
        ok, msg = moderate_text("Find dentists in San Jose, CA")
        assert ok is False
        assert msg and "temporarily unavailable" in msg.lower()
    finally:
        _restore_openai_key(had_key, old)
