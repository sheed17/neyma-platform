import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from backend.services.territory_service import _compute_tier1_rank_key
from pipeline.evidence_registry import EVID_BOOKING_ABSENT, EVID_BOOKING_PRESENT, collect_evidence_ids
from pipeline.signals import _analyze_html_content, _merge_capture_verification


def test_capture_followup_verifies_online_self_scheduling():
    homepage_html = """
    <html>
      <body>
        <a href="/make-appointment">Schedule with Us</a>
      </body>
    </html>
    """
    booking_html = """
    <html>
      <body>
        <h1>Schedule your Appointment</h1>
        <div>Appointment Type</div>
        <div>Pick a Time</div>
        <button>Complete your booking</button>
      </body>
    </html>
    """

    homepage = _analyze_html_content(homepage_html, base_url="https://edisonfamilydentalcare.com")
    booking = _analyze_html_content(
        booking_html,
        base_url="https://edisonfamilydentalcare.com/make-appointment",
    )

    merged = _merge_capture_verification(
        homepage_url="https://edisonfamilydentalcare.com",
        homepage_analysis=homepage,
        followups=[
            {
                "page": "/make-appointment",
                "url": "https://edisonfamilydentalcare.com/make-appointment",
                "source": "followup",
                "method": "playwright",
                "analysis": booking,
            }
        ],
        extraction_method="headless",
    )

    assert homepage["scheduling_cta_detected"] is True
    assert homepage["has_automated_scheduling"] is None
    assert booking["has_automated_scheduling"] is True
    assert booking["booking_flow_type"] == "online_self_scheduling"
    assert merged["booking_flow"]["value"] == "online_self_scheduling"
    assert "/make-appointment" in (merged["booking_flow"]["observed_pages"] or [])


def test_unknown_contact_form_is_not_ranked_like_verified_absence():
    base_row = {
        "user_ratings_total": 42,
        "rating": 4.2,
        "has_website": True,
        "ssl": True,
        "has_phone": True,
        "has_viewport": True,
        "has_schema": True,
    }
    unknown_row = dict(base_row, has_contact_form=None)
    absent_row = dict(base_row, has_contact_form=False)

    unknown_score = _compute_tier1_rank_key(unknown_row, avg_reviews=118.0)
    absent_score = _compute_tier1_rank_key(absent_row, avg_reviews=118.0)

    assert absent_score > unknown_score
    assert round(absent_score - unknown_score, 2) == 8.0


def test_unknown_booking_does_not_emit_booking_absent_evidence():
    ids = collect_evidence_ids(
        signals={
            "signal_review_count": 30,
            "signal_booking_conversion_path": None,
            "signal_has_automated_scheduling": None,
            "signal_has_schema_microdata": True,
            "signal_runs_paid_ads": False,
            "signal_has_website": True,
        },
        competitive_snapshot={},
        service_intelligence={},
        revenue_intelligence={},
        objective_layer={},
    )

    assert EVID_BOOKING_PRESENT not in ids
    assert EVID_BOOKING_ABSENT not in ids
