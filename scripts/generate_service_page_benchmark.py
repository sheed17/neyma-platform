#!/usr/bin/env python3
"""
Generate a human-label benchmark CSV for strict service-page detection.

Creates one row per (site, service) with current detector outputs plus empty
label columns for manual truthing.
"""

from __future__ import annotations

import argparse
import csv
import random
import signal
import sqlite3
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from pipeline.service_depth import build_service_intelligence


VALID_STATUSES = ["strong", "moderate", "weak", "weak_stub_page", "missing"]


class SiteTimeout(Exception):
    pass


def _alarm_handler(_signum, _frame):
    raise SiteTimeout()


def _load_sites(db_path: str, max_rows: int) -> List[Dict[str, Any]]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, website, business_name, city, state, updated_at
        FROM territory_prospects
        WHERE website IS NOT NULL AND website != ''
        ORDER BY COALESCE(updated_at, created_at) DESC, id DESC
        LIMIT ?
        """,
        (max_rows,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    # Also source recent websites from diagnostics payloads.
    cur.execute(
        """
        SELECT id, business_name, city, state, response_json, created_at
        FROM diagnostics
        ORDER BY id DESC
        LIMIT ?
        """,
        (max_rows,),
    )
    diag_rows = []
    for r in cur.fetchall():
        rd = dict(r)
        try:
            import json
            payload = json.loads(rd.get("response_json") or "{}")
        except Exception:
            payload = {}
        website = str(payload.get("website") or "").strip()
        if not website:
            continue
        diag_rows.append(
            {
                "id": f"diag-{rd.get('id')}",
                "website": website,
                "business_name": rd.get("business_name") or payload.get("business_name"),
                "city": rd.get("city") or payload.get("city"),
                "state": rd.get("state") or payload.get("state"),
                "updated_at": rd.get("created_at"),
            }
        )
    rows.extend(diag_rows)
    conn.close()

    seen = set()
    out: List[Dict[str, Any]] = []
    for r in rows:
        w = str(r.get("website") or "").strip().lower()
        if not w or w in seen:
            continue
        seen.add(w)
        out.append(r)
    return out


def _pick_services(service_rows: List[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
    if not service_rows:
        return []
    ordered = sorted(
        service_rows,
        key=lambda x: (-int(x.get("revenue_weight") or 0), str(x.get("service") or "")),
    )
    return ordered[:n]


def _to_float(v: Any) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate service-page detection benchmark CSV.")
    parser.add_argument("--db-path", default="data/opportunity_intelligence.db")
    parser.add_argument("--sites", type=int, default=50)
    parser.add_argument("--services-per-site", type=int, default=5)
    parser.add_argument("--candidate-pool", type=int, default=400)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--site-timeout-sec", type=int, default=40)
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    rng = random.Random(args.seed)
    candidates = _load_sites(args.db_path, max_rows=max(args.candidate_pool, args.sites))
    if not candidates:
        raise SystemExit("No websites found in territory_prospects.")

    rng.shuffle(candidates)
    chosen = candidates[: args.sites]

    signal.signal(signal.SIGALRM, _alarm_handler)

    rows_out: List[Dict[str, Any]] = []
    for site in chosen:
        website = str(site.get("website") or "").strip()
        if not website:
            continue
        try:
            signal.alarm(max(5, int(args.site_timeout_sec)))
            intel = build_service_intelligence(
                website_url=website,
                website_html=None,
                procedure_mentions_from_reviews=None,
                city=site.get("city"),
                state=site.get("state"),
                vertical="dentist",
            )
            signal.alarm(0)
        except SiteTimeout:
            signal.alarm(0)
            rows_out.append(
                {
                    "site_id": site.get("id"),
                    "business_name": site.get("business_name"),
                    "city": site.get("city"),
                    "state": site.get("state"),
                    "website": website,
                    "service_slug": "",
                    "service_display": "",
                    "prediction_status": "",
                    "prediction_page_detected": "",
                    "prediction_reason": f"timeout>{args.site_timeout_sec}s",
                    "prediction_url": "",
                    "pred_word_count": "",
                    "pred_keyword_density": "",
                    "pred_h1_match": "",
                    "pred_faq_present": "",
                    "pred_service_h2_sections": "",
                    "pred_financing_section": "",
                    "pred_before_after_section": "",
                    "pred_internal_links_to_page": "",
                    "pred_umbrella_triggered": "",
                    "pred_pages_crawled_n": "",
                    "pred_rejected_n": "",
                    "label_status": "",
                    "label_dedicated_page_exists": "",
                    "label_notes": "",
                }
            )
            continue
        except Exception as exc:
            signal.alarm(0)
            rows_out.append(
                {
                    "site_id": site.get("id"),
                    "business_name": site.get("business_name"),
                    "city": site.get("city"),
                    "state": site.get("state"),
                    "website": website,
                    "service_slug": "",
                    "service_display": "",
                    "prediction_status": "",
                    "prediction_page_detected": "",
                    "prediction_reason": f"error:{exc}",
                    "prediction_url": "",
                    "pred_word_count": "",
                    "pred_keyword_density": "",
                    "pred_h1_match": "",
                    "pred_faq_present": "",
                    "pred_service_h2_sections": "",
                    "pred_financing_section": "",
                    "pred_before_after_section": "",
                    "pred_internal_links_to_page": "",
                    "pred_umbrella_triggered": "",
                    "pred_pages_crawled_n": "",
                    "pred_rejected_n": "",
                    "label_status": "",
                    "label_dedicated_page_exists": "",
                    "label_notes": "",
                }
            )
            continue

        hv_rows = intel.get("high_value_services") if isinstance(intel.get("high_value_services"), list) else []
        picked = _pick_services(hv_rows, args.services_per_site)
        debug = intel.get("service_page_detection_debug") if isinstance(intel.get("service_page_detection_debug"), dict) else {}
        umbrella = bool(debug.get("umbrella_detection_triggered"))
        pages_crawled_n = len(debug.get("pages_crawled") or [])
        rejected_n = len(debug.get("pages_rejected") or [])

        for svc in picked:
            structural = svc.get("structural_signals") if isinstance(svc.get("structural_signals"), dict) else {}
            status = str(svc.get("qualification_status") or svc.get("optimization_tier") or "missing")
            if status not in VALID_STATUSES:
                status = "missing"
            rows_out.append(
                {
                    "site_id": site.get("id"),
                    "business_name": site.get("business_name"),
                    "city": site.get("city"),
                    "state": site.get("state"),
                    "website": website,
                    "service_slug": svc.get("service"),
                    "service_display": svc.get("display_name"),
                    "prediction_status": status,
                    "prediction_page_detected": bool(svc.get("page_detected")),
                    "prediction_reason": svc.get("detection_reason"),
                    "prediction_url": svc.get("url"),
                    "pred_word_count": int(svc.get("word_count") or 0),
                    "pred_keyword_density": f"{_to_float(svc.get('keyword_density')) or 0.0:.4f}",
                    "pred_h1_match": bool(svc.get("h1_match")),
                    "pred_faq_present": bool(structural.get("faq_present")),
                    "pred_service_h2_sections": int(structural.get("service_h2_sections") or 0),
                    "pred_financing_section": bool(structural.get("financing_section")),
                    "pred_before_after_section": bool(structural.get("before_after_section")),
                    "pred_internal_links_to_page": int(svc.get("internal_links_to_page") or 0),
                    "pred_umbrella_triggered": umbrella,
                    "pred_pages_crawled_n": pages_crawled_n,
                    "pred_rejected_n": rejected_n,
                    "label_status": "",
                    "label_dedicated_page_exists": "",
                    "label_notes": "",
                }
            )

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output = args.output.strip() or f"output/service_page_benchmark_labels_{ts}.csv"

    fieldnames = [
        "site_id",
        "business_name",
        "city",
        "state",
        "website",
        "service_slug",
        "service_display",
        "prediction_status",
        "prediction_page_detected",
        "prediction_reason",
        "prediction_url",
        "pred_word_count",
        "pred_keyword_density",
        "pred_h1_match",
        "pred_faq_present",
        "pred_service_h2_sections",
        "pred_financing_section",
        "pred_before_after_section",
        "pred_internal_links_to_page",
        "pred_umbrella_triggered",
        "pred_pages_crawled_n",
        "pred_rejected_n",
        "label_status",
        "label_dedicated_page_exists",
        "label_notes",
    ]
    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_out)

    print(f"Wrote {len(rows_out)} rows to {output}")
    print("Label guidance:")
    print(f"- label_status must be one of: {', '.join(VALID_STATUSES)}")
    print("- label_dedicated_page_exists should be true/false")


if __name__ == "__main__":
    main()
