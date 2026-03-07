import os
import sys

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)

from pipeline.practice_classifier import classify_practice_type


def test_classify_practice_type_domain_orthodontics():
    place = {"signal_website_url": "https://bartonorthodontics.com"}
    out = classify_practice_type(place, "")
    assert out["practice_type"] == "orthodontist"
    assert out["confidence"] > 0


def test_classify_practice_type_general_fallback():
    place = {"types": ["dentist", "health"], "signal_website_url": "https://exampledental.com"}
    website_text = "We offer exams, cleanings, fillings, and family dental care."
    out = classify_practice_type(place, website_text)
    assert out["practice_type"] == "general_dentist"


def test_invisalign_heavy_general_defaults_to_general_when_close_score():
    place = {"types": ["Dentist"], "signal_website_url": "https://calsmiledental.com"}
    website_text = (
        ("invisalign braces aligners " * 8)
        + ("implants crowns root canal veneers whitening " * 5)
    )
    out = classify_practice_type(place, website_text)
    assert out["practice_type"] == "general_dentist"
