"""
Territory scan orchestration.

Tier 1: lightweight market scan + deterministic ranking.
Tier 2: full diagnostic on-demand (handled by ensure brief endpoint + existing job worker).
"""

from __future__ import annotations

import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from statistics import mean
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlsplit, urlunsplit

import requests

from backend.services.enrichment_service import run_diagnostic
from backend.ml.store import persist_saved_diagnostic_response, persist_scored_entity
from backend.ml.runtime import score_territory_row
from backend.services.npl_service import ai_batch_explain_matches, ai_batch_rerank_candidates
from backend.services.place_resolver import _geocode_city
from pipeline.db import (
    add_scan_diagnostic,
    get_tier1_cache,
    list_territory_prospects,
    list_members_for_list,
    save_diagnostic,
    save_territory_prospects,
    upsert_tier1_cache,
    upsert_list_member,
    update_territory_scan_status,
)
from pipeline.enrich import PlaceDetailsEnricher
from pipeline.fetch import PlacesFetcher, get_keywords_for_niche
from pipeline.geo import generate_geo_grid
from pipeline.normalize import deduplicate_places, filter_practices_only, normalize_place

logger = logging.getLogger(__name__)

PLACE_DETAILS_FIELDS = [
    "place_id",
    "name",
    "formatted_address",
    "address_components",
    "website",
    "international_phone_number",
    "rating",
    "user_ratings_total",
]

PHONE_RE = re.compile(r"(\+?\d[\d\s().-]{7,}\d)")
EMAIL_RE = re.compile(r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})")
CACHE_TTL_SECONDS = 24 * 60 * 60
MAX_CONSECUTIVE_EMPTY_PLACE_QUERIES = int(os.getenv("NEYMA_MAX_CONSECUTIVE_EMPTY_PLACE_QUERIES", "20"))
MAX_ZERO_PLACE_QUERIES_WITH_NO_TOTAL = int(os.getenv("NEYMA_MAX_ZERO_PLACE_QUERIES_WITH_NO_TOTAL", "25"))
TERRITORY_AI_ENABLED = str(os.getenv("TERRITORY_AI_ENABLED", "1")).strip().lower() in {"1", "true", "yes", "on"}
TERRITORY_AI_RERANK_TOP_N = max(0, min(int(os.getenv("TERRITORY_AI_RERANK_TOP_N", "25")), 100))
TERRITORY_AI_EXPLAIN_TOP_N = max(0, min(int(os.getenv("TERRITORY_AI_EXPLAIN_TOP_N", "20")), 80))


def run_territory_scan_job(job: Dict[str, Any]) -> Dict[str, Any]:
    """Execute one Tier 1 territory scan job and return summary payload."""
    inp = job.get("input", {})
    user_id = int(job.get("user_id", 1))
    scan_id = str(inp["scan_id"])
    city = str(inp["city"]).strip()
    state = str(inp.get("state") or "").strip() or None
    vertical = str(inp["vertical"]).strip()
    limit = max(1, min(int(inp.get("limit") or 20), 100))
    filters = inp.get("filters") or {}

    update_territory_scan_status(
        scan_id,
        "running",
        summary={"processed": 0, "accepted": 0, "failed": 0, "total_candidates": 0},
    )

    def _candidate_progress_cb(queries_done: int, queries_total: int, raw_collected: int) -> None:
        update_territory_scan_status(
            scan_id,
            "running",
            summary={
                "phase": "candidate_fetch",
                "candidate_queries_done": queries_done,
                "candidate_queries_total": queries_total,
                "raw_candidates_collected": raw_collected,
                "processed": 0,
                "accepted": 0,
                "failed": 0,
                "total_candidates": 0,
                "scored_candidates": 0,
            },
        )

    candidates = _fetch_territory_candidates(
        city=city,
        state=state,
        vertical=vertical,
        limit=limit,
        progress_cb=_candidate_progress_cb,
    )
    total_candidates = len(candidates)
    scored_cap = min(max(limit * 2, limit), 50, total_candidates)
    candidates = candidates[:scored_cap]

    update_territory_scan_status(
        scan_id,
        "running",
        summary={
            "phase": "tier1_scoring",
            "processed": 0,
            "accepted": 0,
            "failed": 0,
            "total_candidates": total_candidates,
            "scored_candidates": scored_cap,
        },
    )

    def _progress_cb(processed: int, failed_count: int) -> None:
        update_territory_scan_status(
            scan_id,
            "running",
            summary={
                "phase": "tier1_scoring",
                "processed": processed,
                "accepted": 0,
                "failed": failed_count,
                "total_candidates": total_candidates,
                "scored_candidates": scored_cap,
            },
        )

    tier1_rows, failed = _build_tier1_rows(
        candidates,
        city=city,
        state=state,
        filters=filters,
        progress_cb=_progress_cb,
    )

    if TERRITORY_AI_ENABLED and tier1_rows:
        update_territory_scan_status(
            scan_id,
            "running",
            summary={
                "phase": "ai_rerank",
                "processed": 0,
                "accepted": 0,
                "failed": failed,
                "total_candidates": total_candidates,
                "scored_candidates": scored_cap,
            },
        )
        adjustments = ai_batch_rerank_candidates(
            rows=tier1_rows,
            criteria=[{"type": "territory_opportunity", "service": None}],
            purpose="territory",
            max_items=TERRITORY_AI_RERANK_TOP_N,
        )
        if adjustments:
            for row in tier1_rows:
                pid = str(row.get("place_id") or "")
                adj = adjustments.get(pid)
                if not adj:
                    continue
                row["rank_key"] = round(float(row.get("rank_key") or 0.0) + float(adj.get("delta") or 0.0), 3)
                row["ai_rerank"] = adj
            tier1_rows.sort(key=lambda x: float(x.get("rank_key") or 0), reverse=True)

        explanations = ai_batch_explain_matches(
            rows=tier1_rows,
            criteria=[{"type": "territory_opportunity", "service": None}],
            max_items=TERRITORY_AI_EXPLAIN_TOP_N,
        )
        if explanations:
            for row in tier1_rows:
                pid = str(row.get("place_id") or "")
                if pid in explanations:
                    row["ai_explanation"] = explanations[pid]

    for idx, row in enumerate(tier1_rows, start=1):
        row["rank"] = idx
    top_rows = tier1_rows[:limit]
    save_territory_prospects(scan_id=scan_id, user_id=user_id, prospects=top_rows)
    try:
        persisted_rows = list_territory_prospects(scan_id, user_id)
        by_place = {str((r.get("place_id") or "")): r for r in persisted_rows}
        for row in top_rows:
            place_id = str(row.get("place_id") or "")
            persisted = by_place.get(place_id)
            scored = row.get("lead_quality")
            if not persisted or not isinstance(scored, dict):
                continue
            persist_scored_entity(
                entity_type="territory_prospect",
                entity_id=int(persisted["id"]),
                place_id=place_id,
                feature_scope="tier1",
                feature_version=str(scored.get("feature_version") or ""),
                score_payload=scored,
                feature_payload=dict(scored.get("features") or {}),
            )
    except Exception:
        logger.exception("Failed to persist territory lead-quality predictions for scan %s", scan_id)

    summary = {
        "phase": "completed",
        "processed": scored_cap,
        "accepted": len(top_rows),
        "failed": failed,
        "total_candidates": total_candidates,
        "scored_candidates": scored_cap,
    }
    update_territory_scan_status(scan_id, "completed", summary=summary)
    return {"scan_id": scan_id, **summary}


def run_list_rescan_job(job: Dict[str, Any]) -> Dict[str, Any]:
    """Re-run diagnostics for all members of a list and capture diffs."""
    inp = job.get("input", {})
    user_id = int(job.get("user_id", 1))
    scan_id = str(inp["scan_id"])
    list_id = int(inp["list_id"])

    members = list_members_for_list(list_id)
    total = len(members)
    processed = 0
    changed = 0
    failed = 0

    update_territory_scan_status(
        scan_id,
        "running",
        summary={"processed": 0, "accepted": 0, "failed": 0, "total_candidates": total, "changed": 0},
    )

    for member in members:
        prev_diag_id = int(member["diagnostic_id"])
        prev_resp = member.get("response") or {}
        city = str(member.get("city") or prev_resp.get("city") or "").strip()
        state = str(member.get("state") or prev_resp.get("state") or "").strip() or None
        business_name = str(member.get("business_name") or prev_resp.get("business_name") or "").strip()

        if not business_name or not city:
            failed += 1
            continue

        try:
            result = run_diagnostic(business_name=business_name, city=city, state=state)
            resolved_place_id = result.get("place_id") or member.get("place_id")
            diag_id = save_diagnostic(
                user_id=user_id,
                job_id=job["id"],
                place_id=resolved_place_id,
                business_name=result.get("business_name") or business_name,
                city=result.get("city") or city,
                state=result.get("state") or state,
                brief=result.get("brief"),
                response=result,
            )
            persist_saved_diagnostic_response(
                diagnostic_id=int(diag_id),
                place_id=str(resolved_place_id or ""),
                response=result,
            )
            change = _build_change_summary(previous=prev_resp, current=result)
            if change.get("changed"):
                changed += 1
            add_scan_diagnostic(
                scan_id=scan_id,
                diagnostic_id=diag_id,
                place_id=resolved_place_id,
                business_name=result.get("business_name") or business_name,
                city=result.get("city") or city,
                state=result.get("state") or state,
                previous_diagnostic_id=prev_diag_id,
                change=change,
            )
            upsert_list_member(
                list_id=list_id,
                diagnostic_id=diag_id,
                place_id=resolved_place_id,
                business_name=result.get("business_name") or business_name,
                city=result.get("city") or city,
                state=result.get("state") or state,
            )
            processed += 1
        except Exception as exc:
            failed += 1
            logger.warning("list rescan %s member failed: %s", scan_id, exc)

        update_territory_scan_status(
            scan_id,
            "running",
            summary={
                "processed": processed + failed,
                "accepted": processed,
                "failed": failed,
                "total_candidates": total,
                "changed": changed,
            },
        )

    summary = {
        "processed": processed + failed,
        "accepted": processed,
        "failed": failed,
        "total_candidates": total,
        "changed": changed,
    }
    update_territory_scan_status(scan_id, "completed", summary=summary)
    return {"scan_id": scan_id, "rescanned": processed, "failed": failed, "changed": changed}


def _fetch_territory_candidates(
    city: str,
    state: Optional[str],
    vertical: str,
    limit: int,
    radius_miles: Optional[float] = None,
    progress_cb: Optional[Callable[[int, int, int], None]] = None,
) -> List[Dict[str, Any]]:
    coords = _geocode_city(city, state=state)
    if not coords:
        raise RuntimeError(f"Could not geocode city '{city}'")

    city_radius_km = 12.0
    if radius_miles is not None:
        try:
            city_radius_km = max(1.0, min(float(radius_miles) * 1.60934, 96.0))
        except (TypeError, ValueError):
            city_radius_km = 12.0
    search_radius_km = 3.0
    max_pages = 2
    grid_points = generate_geo_grid(coords[0], coords[1], city_radius_km, search_radius_km)
    keywords = get_keywords_for_niche(vertical)

    fetcher = PlacesFetcher()
    raw_places: List[Dict[str, Any]] = []
    seen_place_ids: set[str] = set()
    total_queries = max(1, len(grid_points) * len(keywords))
    query_count = 0
    consecutive_empty_queries = 0
    zero_place_queries_with_no_total = 0
    scored_cap_target = min(max(limit * 2, limit), 50)
    target_unique = min(max(scored_cap_target * 2, scored_cap_target + 10), 140)
    stop_early = False
    stop_reason: str | None = None
    for lat, lng, radius_m in grid_points:
        for keyword in keywords:
            query_count += 1
            places_this_query = 0
            for place in fetcher.fetch_all_pages_for_query(lat, lng, radius_m, keyword, max_pages=max_pages):
                places_this_query += 1
                raw_places.append(place)
                pid = str(place.get("place_id") or "").strip()
                if pid:
                    seen_place_ids.add(pid)
                    if len(seen_place_ids) >= target_unique:
                        stop_early = True
                        stop_reason = "target_unique_reached"
                        break

            if places_this_query == 0:
                consecutive_empty_queries += 1
                if len(raw_places) == 0:
                    zero_place_queries_with_no_total += 1
            else:
                consecutive_empty_queries = 0

            if not stop_early and (
                consecutive_empty_queries >= MAX_CONSECUTIVE_EMPTY_PLACE_QUERIES
                or zero_place_queries_with_no_total >= MAX_ZERO_PLACE_QUERIES_WITH_NO_TOTAL
            ):
                stop_early = True
                stop_reason = "no_results_after_bounded_queries"
                logger.warning(
                    "Stopping candidate fetch early for %s, %s (%s): %s after %d queries "
                    "(consecutive_empty=%d, zero_with_no_total=%d).",
                    city,
                    state,
                    vertical,
                    stop_reason,
                    query_count,
                    consecutive_empty_queries,
                    zero_place_queries_with_no_total,
                )

            if stop_early:
                if progress_cb:
                    progress_cb(query_count, total_queries, len(raw_places))
                break
            if progress_cb and (query_count % 5 == 0 or query_count == total_queries):
                progress_cb(query_count, total_queries, len(raw_places))
        if stop_early:
            break

    normalized: List[Dict[str, Any]] = []
    for place in raw_places:
        try:
            normalized.append(normalize_place(place))
        except Exception:
            continue

    unique = deduplicate_places(normalized)
    vertical_lower = (vertical or "").strip().lower()
    if "dent" in vertical_lower:
        unique = filter_practices_only(unique)
    cap = min(max(limit * 5, limit), 140)
    return unique[:cap]


def _build_tier1_rows(
    candidates: List[Dict[str, Any]],
    city: str,
    state: Optional[str],
    filters: Dict[str, Any],
    progress_cb: Optional[Callable[[int, int], None]] = None,
) -> Tuple[List[Dict[str, Any]], int]:
    details = PlaceDetailsEnricher()

    failed = 0
    rows: List[Dict[str, Any]] = []

    for idx, candidate in enumerate(candidates, start=1):
        place_id = str(candidate.get("place_id") or "").strip()
        if not place_id:
            failed += 1
            if progress_cb and (idx % 5 == 0 or idx == len(candidates)):
                progress_cb(idx, failed)
            continue

        det: Dict[str, Any] = {}
        cache = get_tier1_cache(place_id)
        if cache and _is_cache_fresh(cache.get("updated_at")) and isinstance(cache.get("details"), dict):
            det = cache.get("details") or {}
        else:
            try:
                det = details.get_place_details(place_id, fields=PLACE_DETAILS_FIELDS) or {}
                if det:
                    upsert_tier1_cache(place_id, details=det, website_signals=None)
            except Exception:
                failed += 1
                if progress_cb and (idx % 5 == 0 or idx == len(candidates)):
                    progress_cb(idx, failed)
                continue

        resolved_city, resolved_state = _extract_city_state_from_components(
            det.get("address_components") or [],
            fallback_city=city,
            fallback_state=state,
        )
        website = _normalize_url(det.get("website"))
        phone = str(det.get("international_phone_number") or "").strip()
        rating = _to_float(det.get("rating") if det.get("rating") is not None else candidate.get("rating"))
        reviews = _to_int(
            det.get("user_ratings_total")
            if det.get("user_ratings_total") is not None
            else candidate.get("user_ratings_total")
        )

        rows.append(
            {
                "place_id": place_id,
                "business_name": str(det.get("name") or candidate.get("name") or "Unknown"),
                "city": resolved_city,
                "state": resolved_state,
                "website": website,
                "rating": rating,
                "user_ratings_total": reviews,
                "has_website": bool(website),
                "ssl": False,
                "has_contact_form": False,
                "has_phone": bool(phone),
                "has_viewport": False,
                "has_schema": False,
                "phone": phone or None,
                "email": None,
            }
        )
        if progress_cb and (idx % 5 == 0 or idx == len(candidates)):
            progress_cb(idx, failed)

    website_rows = []
    for r in rows:
        if not r.get("website"):
            continue
        cache = get_tier1_cache(str(r["place_id"]))
        ws = cache.get("website_signals") if cache else None
        if cache and _is_cache_fresh(cache.get("updated_at")) and isinstance(ws, dict):
            r["ssl"] = bool(ws.get("ssl"))
            r["has_contact_form"] = bool(ws.get("has_contact_form"))
            r["has_phone"] = bool(r.get("has_phone") or ws.get("has_phone"))
            r["has_viewport"] = bool(ws.get("has_viewport"))
            r["has_schema"] = bool(ws.get("has_schema"))
            r["email"] = ws.get("email")
        else:
            website_rows.append(r)

    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(_fetch_lightweight_website_signals, str(r["website"])): r for r in website_rows}
        for fut in as_completed(futures):
            row = futures[fut]
            try:
                sig = fut.result()
            except Exception:
                sig = {}
            row["ssl"] = bool(sig.get("ssl"))
            row["has_contact_form"] = bool(sig.get("has_contact_form"))
            row["has_phone"] = bool(row.get("has_phone") or sig.get("has_phone"))
            row["has_viewport"] = bool(sig.get("has_viewport"))
            row["has_schema"] = bool(sig.get("has_schema"))
            row["email"] = sig.get("email")
            upsert_tier1_cache(str(row["place_id"]), details=None, website_signals=sig)

    review_values = [int(r["user_ratings_total"]) for r in rows if int(r.get("user_ratings_total") or 0) > 0]
    avg_reviews = float(mean(review_values)) if review_values else 0.0
    rating_values = [float(r["rating"]) for r in rows if r.get("rating") is not None]
    avg_rating = float(mean(rating_values)) if rating_values else 0.0

    ranked_rows: List[Dict[str, Any]] = []
    for row in rows:
        row["below_review_avg"] = bool(avg_reviews > 0 and (row.get("user_ratings_total") or 0) < avg_reviews)
        row["below_rating_avg"] = bool(avg_rating > 0 and (row.get("rating") or 0) < avg_rating)
        if not _matches_tier1_filters(row, filters):
            continue
        row["avg_market_reviews"] = round(avg_reviews, 2) if avg_reviews > 0 else 0.0
        row["avg_market_rating"] = round(avg_rating, 2) if avg_rating > 0 else 0.0
        row["market_candidate_count"] = len(rows)
        if avg_reviews >= 120:
            row["market_density"] = "high"
        elif avg_reviews >= 50:
            row["market_density"] = "medium"
        else:
            row["market_density"] = "low"
        row["rank_key"] = _compute_tier1_rank_key(row, avg_reviews)
        rv = int(row.get("user_ratings_total") or 0)
        rating = row.get("rating")
        row["review_position_summary"] = f"{rv} reviews, rating {rating if rating is not None else 'N/A'}"
        scored = score_territory_row(row)
        row["lead_quality"] = scored
        ranked_rows.append(row)

    ranked_rows.sort(key=lambda x: float(x.get("rank_key") or 0), reverse=True)
    return ranked_rows, failed


def _compute_tier1_rank_key(row: Dict[str, Any], avg_reviews: float) -> float:
    score = 45.0

    reviews = float(row.get("user_ratings_total") or 0)
    rating = _to_float(row.get("rating"))

    if avg_reviews > 0:
        review_gap = max(0.0, min(1.0, (avg_reviews - reviews) / avg_reviews))
        score += review_gap * 24.0

    if row.get("has_website"):
        score += 4.0
    else:
        score += 12.0

    if not row.get("ssl") and row.get("has_website"):
        score += 8.0
    if not row.get("has_contact_form") and row.get("has_website"):
        score += 8.0
    if not row.get("has_phone"):
        score += 6.0
    if not row.get("has_viewport") and row.get("has_website"):
        score += 3.0
    if not row.get("has_schema") and row.get("has_website"):
        score += 3.0

    if rating is not None:
        if rating >= 4.5:
            score += 4.0
        elif rating < 3.5:
            score -= 4.0

    return round(max(0.0, min(100.0, score)), 2)


def _fetch_lightweight_website_signals(url: str) -> Dict[str, Any]:
    request_url = url
    if not request_url.startswith(("http://", "https://")):
        request_url = f"https://{request_url}"

    headers = {"User-Agent": "Mozilla/5.0 (Neyma Tier1 Scan)"}
    resp = requests.get(request_url, headers=headers, timeout=(3, 8), allow_redirects=True)
    html = (resp.text or "")[:300000]
    lower = html.lower()
    final_url = str(resp.url or request_url)

    has_contact_form = ("<form" in lower) or ("contact" in lower)
    has_phone = bool(PHONE_RE.search(html))
    has_viewport = "name=\"viewport\"" in lower or "name='viewport'" in lower
    has_schema = ("application/ld+json" in lower) or ("itemscope" in lower)
    email = None
    mailto_match = re.search(r"mailto:([^\s\"'>]+)", html, flags=re.IGNORECASE)
    if mailto_match:
        email = mailto_match.group(1).strip()
    else:
        m = EMAIL_RE.search(html)
        if m:
            email = m.group(1).strip()

    return {
        "ssl": final_url.startswith("https://"),
        "has_contact_form": has_contact_form,
        "has_phone": has_phone,
        "has_viewport": has_viewport,
        "has_schema": has_schema,
        "email": email,
    }


def _matches_tier1_filters(row: Dict[str, Any], filters: Dict[str, Any]) -> bool:
    if not filters:
        return True
    if filters.get("below_review_avg") and not row.get("below_review_avg"):
        return False
    # Tier 1 has no implant-gap detection by design.
    return True


def _extract_city_state_from_components(
    components: List[Dict[str, Any]],
    fallback_city: str,
    fallback_state: Optional[str],
) -> Tuple[str, Optional[str]]:
    city = fallback_city
    state = fallback_state
    for comp in components:
        types = comp.get("types") or []
        if "locality" in types:
            city = comp.get("long_name") or city
        elif "administrative_area_level_1" in types:
            state = comp.get("short_name") or comp.get("long_name") or state
    return city, state


def _normalize_url(raw: Any) -> Optional[str]:
    if not raw:
        return None
    val = str(raw).strip()
    if not val:
        return None
    if not val.startswith(("http://", "https://")):
        val = f"https://{val}"
    try:
        parts = urlsplit(val)
        scheme = parts.scheme or "https"
        netloc = (parts.netloc or "").lower()
        path = parts.path or ""
        if path.endswith("/") and path != "/":
            path = path.rstrip("/")
        # Strip query/fragment to remove UTM and tracking noise.
        normalized = urlunsplit((scheme, netloc, path, "", ""))
        return normalized
    except Exception:
        return val


def _to_int(v: Any) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _build_change_summary(previous: Dict[str, Any], current: Dict[str, Any]) -> Dict[str, Any]:
    prev_brief = previous.get("brief") or {}
    curr_brief = current.get("brief") or {}

    fields = [
        ("constraint", str(previous.get("constraint") or ""), str(current.get("constraint") or "")),
        ("primary_leverage", str(previous.get("primary_leverage") or ""), str(current.get("primary_leverage") or "")),
        ("opportunity_profile", str(previous.get("opportunity_profile") or ""), str(current.get("opportunity_profile") or "")),
        (
            "modeled_revenue_upside",
            str((prev_brief.get("executive_diagnosis") or {}).get("modeled_revenue_upside") or ""),
            str((curr_brief.get("executive_diagnosis") or {}).get("modeled_revenue_upside") or ""),
        ),
    ]

    deltas = []
    for key, old, new in fields:
        if old != new:
            deltas.append({"field": key, "before": old or None, "after": new or None})

    return {"changed": bool(deltas), "deltas": deltas}


def _is_cache_fresh(updated_at: Any, ttl_seconds: int = CACHE_TTL_SECONDS) -> bool:
    if not updated_at:
        return False
    try:
        ts = datetime.fromisoformat(str(updated_at).replace("Z", "+00:00"))
        age = (datetime.now(timezone.utc) - ts).total_seconds()
        return age <= ttl_seconds
    except Exception:
        return False
