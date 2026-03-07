import os
import sys

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)


def test_brief_signal_verification_low_crawl_not_evaluated(monkeypatch):
    from backend.services import enrichment_service as es

    monkeypatch.setenv("BRIEF_AI_VERIFY_ENABLED", "0")
    out = es._build_brief_signal_verification(
        website="https://example.com",
        service_intel={
            "crawl_confidence": "low",
            "high_value_services": [
                {
                    "service": "implants",
                    "display_name": "Implants",
                    "service_status": "missing",
                    "detection_reason": "No dedicated page found in crawled HTML.",
                    "confidence_level": "high",
                }
            ],
        },
    )

    assert out["summary"]["total"] == 1
    row = out["services"][0]
    assert row["final_verdict"] == "not_evaluated"
    assert row["final_confidence"] == "low"
    assert row["ai_validation"]["enabled"] is False


def test_brief_signal_verification_tracks_ai_disagreement(monkeypatch):
    from backend.services import enrichment_service as es
    from backend.services import npl_service

    monkeypatch.setenv("BRIEF_AI_VERIFY_ENABLED", "1")
    monkeypatch.setenv("BRIEF_AI_VERIFY_MAX_ITEMS", "8")
    monkeypatch.setattr(
        npl_service,
        "ai_validate_brief_service_rows",
        lambda **_kwargs: {
            "implants": {
                "verdict": "likely_present",
                "confidence": "high",
                "reason": "Found strong implants page signal.",
                "model": "gpt-4o-mini",
            }
        },
    )

    out = es._build_brief_signal_verification(
        website="https://example.com",
        service_intel={
            "crawl_confidence": "high",
            "high_value_services": [
                {
                    "service": "implants",
                    "display_name": "Implants",
                    "service_status": "missing",
                    "detection_reason": "No dedicated page found in crawled HTML.",
                    "confidence_level": "high",
                }
            ],
        },
    )

    assert out["summary"]["disagreements"] == 1
    row = out["services"][0]
    assert row["final_verdict"] == "missing"
    assert row["final_confidence"] == "medium"
    assert row["ai_validation"]["enabled"] is True
    assert row["ai_validation"]["verdict"] == "likely_present"
