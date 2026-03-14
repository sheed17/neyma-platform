import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pipeline.db import (
    create_territory_scan,
    init_db,
    link_territory_prospect_diagnostic,
    list_territory_prospects,
    save_diagnostic,
    save_territory_prospects,
)
from scripts.build_lead_quality_dataset import build_dataset_rows
from scripts.run_market_training_batch import select_prospect_ids


def _response(place_id: str, business_name: str, city: str, state: str) -> dict:
    return {
        "place_id": place_id,
        "business_name": business_name,
        "city": city,
        "state": state,
        "website": "https://example.com",
        "phone": "+1-555-555-1212",
        "market_density": "medium",
        "paid_status": "Inactive",
        "service_intelligence": {
            "missing_services": [],
            "detected_services": [],
            "pages_crawled": 0,
            "crawl_confidence": "low",
        },
        "conversion_infrastructure": {
            "online_booking": None,
            "contact_form": None,
            "phone_prominent": True,
            "mobile_optimized": True,
            "page_load_ms": None,
        },
        "brief": {
            "market_position": {
                "reviews": "25",
                "local_avg": "80",
                "market_density": "medium",
            }
        },
        "competitors": [{"isYou": True, "reviews": 25, "rating": 4.1}],
        "local_avg_rating": 4.4,
    }


def _prospect(place_id: str, business_name: str, city: str, state: str) -> dict:
    return {
        "place_id": place_id,
        "business_name": business_name,
        "city": city,
        "state": state,
        "website": "https://example.com",
        "rating": 4.1,
        "user_ratings_total": 25,
        "has_website": True,
        "ssl": True,
        "has_contact_form": True,
        "has_phone": True,
        "has_viewport": True,
        "has_schema": False,
        "rank_key": 1.0,
        "rank": 1,
        "review_position_summary": "Below average",
    }


def test_build_dataset_rows_prefers_scan_market(monkeypatch, tmp_path):
    db_path = tmp_path / "opportunity.sqlite"
    monkeypatch.setenv("OPPORTUNITY_DB_PATH", str(db_path))
    init_db()

    response = _response("place-1", "Quincy Dental", "Quincy", "MA")
    diagnostic_id = save_diagnostic(
        user_id=1,
        job_id=None,
        place_id="place-1",
        business_name="Quincy Dental",
        city="Quincy",
        state="MA",
        brief=response["brief"],
        response=response,
    )
    create_territory_scan(
        scan_id="scan-1",
        user_id=1,
        job_id=None,
        city="Boston",
        state="MA",
        vertical="dentist",
        limit_count=20,
    )
    save_territory_prospects("scan-1", 1, [_prospect("place-1", "Quincy Dental", "Quincy", "MA")])
    prospect = list_territory_prospects("scan-1", 1)[0]
    link_territory_prospect_diagnostic(int(prospect["id"]), diagnostic_id, full_brief_ready=True)

    _, rows, summary = build_dataset_rows(market_values=["Boston, MA"])

    assert len(rows) == 1
    row = rows[0]
    assert row["city"] == "Quincy"
    assert row["state"] == "MA"
    assert row["market_city"] == "Boston"
    assert row["market_state"] == "MA"
    assert row["market_key"] == "boston|MA"
    assert row["market_source"] == "territory_scan"
    assert row["source_scan_id"] == "scan-1"
    assert summary["market_counts"] == {"boston|MA": 1}
    assert summary["market_source_counts"] == {"territory_scan": 1}

    _, nonmatching_rows, _ = build_dataset_rows(market_values=["Quincy, MA"])
    assert nonmatching_rows == []


def test_build_dataset_rows_falls_back_to_business_location(monkeypatch, tmp_path):
    db_path = tmp_path / "opportunity.sqlite"
    monkeypatch.setenv("OPPORTUNITY_DB_PATH", str(db_path))
    init_db()

    response = _response("place-2", "Seattle Dental", "Seattle", "WA")
    save_diagnostic(
        user_id=1,
        job_id=None,
        place_id="place-2",
        business_name="Seattle Dental",
        city="Seattle",
        state="WA",
        brief=response["brief"],
        response=response,
    )

    _, rows, summary = build_dataset_rows(market_values=["Seattle, WA"])

    assert len(rows) == 1
    row = rows[0]
    assert row["market_city"] == "Seattle"
    assert row["market_state"] == "WA"
    assert row["market_key"] == "seattle|WA"
    assert row["market_source"] == "business_location"
    assert row["source_scan_id"] == ""
    assert summary["market_counts"] == {"seattle|WA": 1}
    assert summary["market_source_counts"] == {"business_location": 1}


def test_select_prospect_ids_spreads_across_rank_curve():
    rows = [{"id": idx} for idx in range(1, 11)]

    assert select_prospect_ids(rows, 4, "stratified_rank") == [1, 4, 7, 10]
    assert select_prospect_ids(rows, 4, "top") == [1, 2, 3, 4]
