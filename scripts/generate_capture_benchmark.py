#!/usr/bin/env python3
"""
Generate a capture-signal benchmark CSV with model predictions.

Input CSV columns required:
- business_name
- city

Optional input columns:
- state
- website
- any label_* fields (preserved in output for annotation/eval)

Output adds prediction columns:
- pred_online_booking
- pred_contact_form
- pred_phone_prominent
- pred_phone_clickable
- pred_has_cta
- pred_cta_count
- pred_form_structure
- pred_crawl_method
- pred_crawl_confidence
- pred_risk_flags
- pred_error
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from backend.services.enrichment_service import run_diagnostic  # noqa: E402


def _to_bool_text(value: Any) -> str:
    if value is True:
        return "true"
    if value is False:
        return "false"
    return ""


def _extract_predictions(diag: Dict[str, Any]) -> Dict[str, str]:
    brief = diag.get("brief") if isinstance(diag.get("brief"), dict) else {}
    conv = brief.get("conversion_infrastructure") if isinstance(brief.get("conversion_infrastructure"), dict) else {}
    if not conv:
        conv = diag.get("conversion_infrastructure") if isinstance(diag.get("conversion_infrastructure"), dict) else {}
    convs = brief.get("conversion_structure") if isinstance(brief.get("conversion_structure"), dict) else {}
    svc = diag.get("service_intelligence") if isinstance(diag.get("service_intelligence"), dict) else {}
    risk_flags = diag.get("risk_flags") if isinstance(diag.get("risk_flags"), list) else []

    cta_count = convs.get("cta_count")
    has_cta: Optional[bool] = None
    try:
        if cta_count is not None:
            has_cta = int(cta_count) > 0
    except (TypeError, ValueError):
        has_cta = None

    out = {
        "pred_online_booking": _to_bool_text(conv.get("online_booking")),
        "pred_contact_form": _to_bool_text(conv.get("contact_form")),
        "pred_phone_prominent": _to_bool_text(conv.get("phone_prominent")),
        "pred_phone_clickable": _to_bool_text(convs.get("phone_clickable")),
        "pred_has_cta": _to_bool_text(has_cta),
        "pred_cta_count": str(cta_count) if cta_count is not None else "",
        "pred_form_structure": str(convs.get("form_single_or_multi_step") or ""),
        "pred_crawl_method": str(svc.get("crawl_method") or ""),
        "pred_crawl_confidence": str(svc.get("crawl_confidence") or ""),
        "pred_risk_flags": " | ".join(str(x) for x in risk_flags if str(x).strip()),
        "pred_error": "",
    }
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate capture-signal benchmark predictions.")
    parser.add_argument("--csv", required=True, help="Input CSV path with business rows.")
    parser.add_argument("--output", default="", help="Output CSV path.")
    parser.add_argument("--limit", type=int, default=0, help="Optional row limit.")
    parser.add_argument(
        "--deep-audit",
        action="store_true",
        default=False,
        help="Pass deep_audit=True into run_diagnostic (optional).",
    )
    args = parser.parse_args()

    with open(args.csv, "r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        print("No rows found in input CSV.")
        return 1

    if args.limit and args.limit > 0:
        rows = rows[: args.limit]

    out_rows: List[Dict[str, str]] = []
    for idx, row in enumerate(rows, start=1):
        business_name = str(row.get("business_name") or "").strip()
        city = str(row.get("city") or "").strip()
        state = str(row.get("state") or "").strip()
        website = str(row.get("website") or "").strip()
        if not business_name or not city:
            rec = dict(row)
            rec["pred_error"] = "missing business_name/city"
            out_rows.append(rec)
            print(f"[{idx}/{len(rows)}] skipped missing required fields")
            continue
        try:
            diag = run_diagnostic(
                business_name=business_name,
                city=city,
                state=state or None,
                website=website or None,
                deep_audit=bool(args.deep_audit),
            )
            pred = _extract_predictions(diag)
            rec = dict(row)
            rec.update(pred)
            out_rows.append(rec)
            print(f"[{idx}/{len(rows)}] ok: {business_name} ({city})")
        except Exception as exc:  # noqa: BLE001
            rec = dict(row)
            rec["pred_error"] = f"{type(exc).__name__}: {exc}"
            out_rows.append(rec)
            print(f"[{idx}/{len(rows)}] error: {business_name} ({city}) -> {type(exc).__name__}")

    now = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output = args.output.strip() or f"output/capture_benchmark_predictions_{now}.csv"
    os.makedirs(os.path.dirname(output), exist_ok=True)

    # Stabilize header order: original + known prediction fields.
    pred_fields = [
        "pred_online_booking",
        "pred_contact_form",
        "pred_phone_prominent",
        "pred_phone_clickable",
        "pred_has_cta",
        "pred_cta_count",
        "pred_form_structure",
        "pred_crawl_method",
        "pred_crawl_confidence",
        "pred_risk_flags",
        "pred_error",
    ]
    fieldnames: List[str] = list(rows[0].keys())
    for pf in pred_fields:
        if pf not in fieldnames:
            fieldnames.append(pf)
    # Also include any extra keys created from error rows.
    for r in out_rows:
        for k in r.keys():
            if k not in fieldnames:
                fieldnames.append(k)

    with open(output, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in out_rows:
            w.writerow(r)

    print(f"\nWrote: {output}")
    print(f"Rows: {len(out_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

