"""
Validation and sanity checks for signals and context.

Surfaces impossible combos and odd states as warnings (no hard failures).
"""

import re
from typing import Dict, List, Any


def check_lead_signals(signals: Dict[str, Any]) -> List[str]:
    """
    Check for inconsistent or impossible signal combinations.
    Returns list of warning strings (empty if none).
    """
    warnings = []
    # Normalize: accept signal_ prefix
    s = {k.replace("signal_", "") if k.startswith("signal_") else k: v for k, v in signals.items()}

    has_website = s.get("has_website")
    website_accessible = s.get("website_accessible")
    if has_website is False and website_accessible is True:
        warnings.append("has_website=false but website_accessible=true (impossible)")
    if has_website is True and website_accessible is False:
        # This is valid (site exists but down)
        pass
    if has_website is False and s.get("has_contact_form") is True:
        warnings.append("has_website=false but has_contact_form=true (unusual)")
    if has_website is False and s.get("mobile_friendly") is not None:
        warnings.append("has_website=false but mobile_friendly is set (should be unknown)")

    return warnings


def check_context(context: Dict[str, Any]) -> List[str]:
    """
    Check for odd context states (e.g. all Unknown but high confidence).
    Returns list of warning strings.
    """
    warnings = []
    dimensions = context.get("context_dimensions") or []
    confidence = context.get("confidence") or 0
    statuses = [d.get("status") for d in dimensions if d.get("status")]

    if confidence >= 0.7 and all(s == "Unknown" for s in statuses):
        warnings.append("High confidence but all dimensions Unknown (review signals)")
    if not dimensions and confidence > 0:
        warnings.append("No dimensions but confidence > 0")
    return warnings


def _first_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    m = re.search(r"-?\d+(?:\.\d+)?", str(value))
    if not m:
        return None
    try:
        return float(m.group(0))
    except (TypeError, ValueError):
        return None


def _review_position_label(lead_reviews: float, avg_reviews: float) -> str:
    if avg_reviews <= 0:
        return "In line with sample average"
    ratio = lead_reviews / avg_reviews
    if ratio >= 1.2:
        return "Above sample average"
    if ratio >= 0.8:
        return "In line with sample average"
    return "Below sample average"


def _vs_competitor_text(lead_reviews: float, competitor_reviews: float) -> str:
    if lead_reviews < competitor_reviews:
        return "below that competitor"
    if lead_reviews > competitor_reviews:
        return "above that competitor"
    return "in line with that competitor"


def enforce_diagnostic_consistency(out: Dict[str, Any], merged: Dict[str, Any] | None = None) -> List[str]:
    """
    Apply lightweight deterministic consistency checks/fixes to diagnostic output.
    Mutates `out` in place and returns warning list describing what was corrected.
    """
    warnings: List[str] = []
    if not isinstance(out, dict):
        return warnings

    brief = out.get("brief")
    if not isinstance(brief, dict):
        return warnings

    # 1) Normalize review_position to match observed review/local average ratio.
    mp = brief.get("market_position") if isinstance(brief.get("market_position"), dict) else {}
    lead_reviews = _first_number(mp.get("reviews"))
    avg_reviews = _first_number(mp.get("local_avg"))
    if lead_reviews is None and isinstance(merged, dict):
        lead_reviews = _first_number(merged.get("signal_review_count") or merged.get("user_ratings_total"))
    if avg_reviews is None and isinstance(merged, dict):
        comp = merged.get("competitive_snapshot") if isinstance(merged.get("competitive_snapshot"), dict) else {}
        avg_reviews = _first_number(comp.get("avg_review_count"))
    if lead_reviews is not None and avg_reviews is not None and avg_reviews > 0:
        expected_review_position = _review_position_label(float(lead_reviews), float(avg_reviews))
        current = str(out.get("review_position") or "").strip()
        if current and current != expected_review_position:
            warnings.append(
                f"Adjusted review_position from '{current}' to '{expected_review_position}' based on reviews/local avg."
            )
            out["review_position"] = expected_review_position
        elif not current:
            out["review_position"] = expected_review_position

    # 2) Ensure strategic_gap review-position narrative is data-driven.
    sg = brief.get("strategic_gap") if isinstance(brief.get("strategic_gap"), dict) else {}
    if sg:
        c_reviews = _first_number(sg.get("competitor_reviews"))
        l_reviews = _first_number(sg.get("lead_reviews"))
        if c_reviews is not None and l_reviews is not None:
            relation = _vs_competitor_text(float(l_reviews), float(c_reviews))
            sg["review_position_vs_competitor"] = relation
            sg["review_position_sentence"] = f"This practice's review position is {relation}."

    # 3) Keep opportunity-profile review_deficit driver aligned to observed values when present.
    ed = brief.get("executive_diagnosis") if isinstance(brief.get("executive_diagnosis"), dict) else {}
    opp = ed.get("opportunity_profile") if isinstance(ed.get("opportunity_profile"), dict) else {}
    drivers = opp.get("leverage_drivers") if isinstance(opp.get("leverage_drivers"), dict) else {}
    if drivers and lead_reviews is not None and avg_reviews is not None and avg_reviews > 0:
        expected_deficit = float(lead_reviews) < (0.5 * float(avg_reviews))
        current_deficit = drivers.get("review_deficit")
        if isinstance(current_deficit, bool) and current_deficit != expected_deficit:
            drivers["review_deficit"] = expected_deficit
            warnings.append("Adjusted leverage_drivers.review_deficit to match observed reviews/local average.")

    # 4) Guard against paid contradiction when an explicit paid signal exists.
    ds = brief.get("demand_signals") if isinstance(brief.get("demand_signals"), dict) else {}
    if isinstance(merged, dict):
        paid_signal = merged.get("signal_runs_paid_ads")
        ads_line = str(ds.get("google_ads_line") or "").strip().lower()
        if paid_signal is True and ads_line in {"not detected", "inactive"}:
            ds["google_ads_line"] = "Active (signal detected)"
            warnings.append("Adjusted demand_signals.google_ads_line to reflect observed paid signal.")

    if warnings:
        brief["consistency_warnings"] = warnings
    else:
        brief.pop("consistency_warnings", None)
    return warnings
