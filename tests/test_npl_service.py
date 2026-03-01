"""
Coverage tests for Ask query parsing.
"""

import os
import sys
import importlib.util

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)

_spec = importlib.util.spec_from_file_location(
    "npl_service",
    os.path.join(_root, "backend", "services", "npl_service.py"),
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
parse_npl_query = _mod.parse_npl_query


def _types(intent):
    return [c.get("type") for c in intent.get("criteria", [])]


def test_missing_location_near_me():
    intent = parse_npl_query("Find dentists near me")
    assert not intent.get("city")
    assert not intent.get("state")


def test_missing_location_city_no_state():
    intent = parse_npl_query("Dentists in San Jose")
    assert intent.get("city") in (None, "San Jose")
    assert not intent.get("state")


def test_location_odd_phrase_city_state_no_in():
    intent = parse_npl_query("10 dentists San Jose CA")
    assert intent["city"] == "San Jose"
    assert intent["state"] == "CA"


def test_location_with_full_state_name():
    intent = parse_npl_query("dentist offices, Austin Texas")
    assert intent["city"] == "Austin"
    assert intent["state"] == "TX"


def test_location_parenthesized_state():
    intent = parse_npl_query("In San Francisco (CA) need dentists")
    assert intent["city"] == "San Francisco"
    assert intent["state"] == "CA"


def test_two_word_city_lowercase():
    intent = parse_npl_query("dentists in new york ny")
    assert intent["city"].lower() == "new york"
    assert intent["state"] == "NY"


def test_limit_capped_20():
    intent = parse_npl_query("Find 50 dentists in Houston TX")
    assert intent["limit"] == 20


def test_limit_zero_becomes_one():
    intent = parse_npl_query("Find 0 dentists in San Jose CA")
    assert intent["limit"] == 1


def test_limit_default_when_missing():
    intent = parse_npl_query("Dentists in Seattle WA")
    assert intent["limit"] == 10


def test_below_review_avg_phrases():
    intent = parse_npl_query("Dentists in Dallas TX that have bad reviews")
    assert "below_review_avg" in _types(intent)


def test_website_phrases_no_website():
    intent = parse_npl_query("Dental offices in Columbus OH without a site")
    assert "no_website" in _types(intent)


def test_website_phrases_has_website():
    intent = parse_npl_query("Dentists in San Jose CA that have a website")
    assert "has_website" in _types(intent)


def test_missing_service_page_mapping():
    intent = parse_npl_query("Find 8 dentists in Phoenix AZ that don't have an invisalign page")
    crit = intent.get("criteria", [])
    assert any(c.get("type") == "missing_service_page" and c.get("service") == "invisalign" for c in crit)


def test_combined_supported_criteria():
    intent = parse_npl_query("Find like 12 dentists in San Jose CA with bad reviews and no website")
    t = _types(intent)
    assert "below_review_avg" in t
    assert "no_website" in t
    assert intent["limit"] == 12


def test_combined_with_missing_service():
    intent = parse_npl_query("Dentists in Austin TX missing implants page, below review avg")
    t = _types(intent)
    assert "below_review_avg" in t
    assert any(c.get("type") == "missing_service_page" and c.get("service") == "implants" for c in intent["criteria"])


def test_unsupported_phrases_captured():
    intent = parse_npl_query("Dentists in Chicago IL that take Medicaid and are open on weekends")
    unsupported = set(intent.get("unsupported_parts") or [])
    assert "medicaid" in unsupported
    assert "weekend" in unsupported or "open weekends" in unsupported


def test_vertical_orthodontist():
    intent = parse_npl_query("Find 10 orthodontists in Austin TX")
    assert intent["vertical"] == "orthodontist"


def test_unknown_specific_request_keeps_supported_only():
    intent = parse_npl_query("Find dentists that need SEO help in San Jose CA")
    assert intent["city"] == "San Jose"
    assert intent["state"] == "CA"
    assert "seo" not in _types(intent)


def test_semantic_map_poor_visibility():
    intent = parse_npl_query("Find 10 dentists in Austin TX with poor visibility")
    assert "below_review_avg" in _types(intent)


def test_primary_constraint_mapping():
    intent = parse_npl_query("Find dentists in Austin TX whose primary constraint is visibility")
    assert "primary_constraint_visibility" in _types(intent)
