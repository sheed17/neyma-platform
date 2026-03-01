"""Unit tests for ServicePageQualificationAgent_v2 deterministic rules."""

import os
import sys

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)

from pipeline.service_page_qualification_agent_v2 import qualify_service_page_candidate_v2


def test_v2_strong_when_all_core_rules_pass():
    out = qualify_service_page_candidate_v2(
        {
            "service_slug": "implants",
            "service_display": "Dental Implants",
            "root_domain": "example.com",
            "page": {
                "url": "/services/dental-implants",
                "canonical_url": "/services/dental-implants",
                "word_count": 900,
                "h1": "Dental Implants in Dallas, TX",
                "title": "Dental Implants | Example",
                "keyword_density": 0.031,
                "faq_present": True,
                "h2_sections": 4,
                "financing_section": False,
                "before_after_section": False,
                "internal_links_to_page": 2,
                "schema_types": ["Dentist", "Service"],
            },
        }
    )
    assert out["qualified"] is True
    assert out["prediction_status"] == "strong"


def test_v2_rejects_homepage():
    out = qualify_service_page_candidate_v2(
        {
            "service_slug": "implants",
            "service_display": "Dental Implants",
            "root_domain": "example.com",
            "page": {
                "url": "/",
                "canonical_url": "/",
                "word_count": 1400,
                "h1": "Dental Implants",
                "title": "Dental Implants",
                "keyword_density": 0.05,
                "faq_present": True,
                "h2_sections": 3,
                "financing_section": False,
                "before_after_section": False,
                "internal_links_to_page": 6,
                "schema_types": ["Service"],
            },
        }
    )
    assert out["qualified"] is False
    assert out["prediction_status"] == "rejected_non_service"
    assert out["core_rules_passed"]["homepage_excluded"] is False


def test_v2_rejects_blog_url():
    out = qualify_service_page_candidate_v2(
        {
            "service_slug": "implants",
            "service_display": "Dental Implants",
            "root_domain": "example.com",
            "page": {
                "url": "/blog/dental-implants-cost-guide",
                "canonical_url": "/blog/dental-implants-cost-guide",
                "word_count": 2000,
                "h1": "Dental Implants",
                "title": "Dental Implants",
                "keyword_density": 0.04,
                "faq_present": True,
                "h2_sections": 5,
                "financing_section": True,
                "before_after_section": False,
                "internal_links_to_page": 12,
                "schema_types": ["Article"],
            },
        }
    )
    assert out["qualified"] is False
    assert out["prediction_status"] == "rejected_non_service"
    assert out["core_rules_passed"]["blog_excluded"] is False


def test_v2_umbrella_only_when_marked_umbrella():
    out = qualify_service_page_candidate_v2(
        {
            "service_slug": "veneers",
            "service_display": "Porcelain Veneers",
            "root_domain": "example.com",
            "page": {
                "url": "/services",
                "canonical_url": "/services",
                "word_count": 1200,
                "h1": "Our Services",
                "title": "Services",
                "keyword_density": 0.01,
                "faq_present": False,
                "h2_sections": 2,
                "financing_section": False,
                "before_after_section": False,
                "internal_links_to_page": 10,
                "schema_types": ["Dentist"],
                "umbrella_page": True,
                "service_mentioned": True,
            },
        }
    )
    assert out["qualified"] is False
    assert out["prediction_status"] == "umbrella_only"


def test_v2_weak_stub_when_slug_matches_but_rules_fail():
    out = qualify_service_page_candidate_v2(
        {
            "service_slug": "invisalign",
            "service_display": "Invisalign",
            "root_domain": "example.com",
            "page": {
                "url": "/services/invisalign",
                "canonical_url": "/services/invisalign",
                "word_count": 280,
                "h1": "Invisalign",
                "title": "Invisalign",
                "keyword_density": 0.01,
                "faq_present": False,
                "h2_sections": 1,
                "financing_section": False,
                "before_after_section": False,
                "internal_links_to_page": 0,
                "schema_types": ["Service"],
            },
        }
    )
    assert out["qualified"] is False
    assert out["prediction_status"] == "weak_stub_page"

