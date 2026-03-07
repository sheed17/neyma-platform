"""
Diagnostics CRUD — list / detail / delete saved diagnostics.
"""

import io
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response

from backend.models.schemas import (
    DiagnosticListItem,
    DiagnosticListResponse,
    DiagnosticResponse,
    InterventionPlanItem,
    ServiceIntelligence,
    RevenueBreakdown,
    ConversionInfrastructure,
    EvidenceItem,
    LeadQualityPrediction,
)
from pipeline.db import (
    count_diagnostics,
    create_brief_share_token,
    delete_diagnostic,
    get_diagnostic,
    get_outcome_summary_for_user,
    get_territory_contact_for_diagnostic,
    list_diagnostics,
    list_outcomes_for_user,
)

router = APIRouter(prefix="/diagnostics", tags=["diagnostics"])


def _pick_string(*values: Any) -> str | None:
    for val in values:
        if val is None:
            continue
        txt = str(val).strip()
        if txt:
            return txt
    return None


def _response_from_saved(resp: dict) -> DiagnosticResponse:
    """Build a DiagnosticResponse from the stored response_json dict."""
    plan = [
        InterventionPlanItem(step=p["step"], category=p["category"], action=p["action"])
        for p in resp.get("intervention_plan", [])
        if isinstance(p, dict) and "step" in p
    ]

    si_raw = resp.get("service_intelligence")
    si = ServiceIntelligence(**si_raw) if isinstance(si_raw, dict) else None

    rbs = [
        RevenueBreakdown(
            service=rb.get("service", ""),
            consults_per_month=rb.get("consults_per_month", ""),
            revenue_per_case=rb.get("revenue_per_case", ""),
            annual_revenue_range=rb.get("annual_revenue_range", ""),
        )
        for rb in resp.get("revenue_breakdowns", [])
        if isinstance(rb, dict)
    ]

    ci_raw = resp.get("conversion_infrastructure")
    ci = ConversionInfrastructure(**ci_raw) if isinstance(ci_raw, dict) else None
    signals = resp.get("signals") if isinstance(resp.get("signals"), dict) else {}

    phone = _pick_string(
        resp.get("phone"),
        resp.get("signal_phone"),
        resp.get("international_phone_number"),
        signals.get("signal_phone") if isinstance(signals, dict) else None,
        signals.get("international_phone_number") if isinstance(signals, dict) else None,
        signals.get("phone") if isinstance(signals, dict) else None,
    )
    website = _pick_string(
        resp.get("website"),
        resp.get("signal_website_url"),
        resp.get("website_url"),
        signals.get("signal_website_url") if isinstance(signals, dict) else None,
        signals.get("website") if isinstance(signals, dict) else None,
        signals.get("website_url") if isinstance(signals, dict) else None,
    )

    evidence = [
        EvidenceItem(label=e.get("label", ""), value=e.get("value", ""))
        for e in resp.get("evidence", [])
        if isinstance(e, dict)
    ]
    lead_quality_raw = resp.get("lead_quality")
    lead_quality = LeadQualityPrediction(**lead_quality_raw) if isinstance(lead_quality_raw, dict) else None

    return DiagnosticResponse(
        lead_id=resp.get("lead_id", 0),
        business_name=resp.get("business_name", ""),
        city=resp.get("city", ""),
        state=resp.get("state"),
        phone=phone,
        website=website,
        opportunity_profile=resp.get("opportunity_profile", ""),
        constraint=resp.get("constraint", ""),
        primary_leverage=resp.get("primary_leverage", ""),
        market_density=resp.get("market_density", ""),
        review_position=resp.get("review_position", ""),
        paid_status=resp.get("paid_status", ""),
        intervention_plan=plan,
        brief=resp.get("brief"),
        service_intelligence=si,
        revenue_breakdowns=rbs,
        conversion_infrastructure=ci,
        risk_flags=resp.get("risk_flags", []),
        evidence=evidence,
        lead_quality=lead_quality,
    )


def _render_pdf_from_lines(title: str, lines: List[str]) -> bytes:
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="PDF export dependency missing (reportlab). Install with: pip install reportlab",
        ) from exc

    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter
    y = height - 48
    c.setFont("Helvetica-Bold", 14)
    c.drawString(48, y, title[:110])
    y -= 28
    c.setFont("Helvetica", 10)
    for line in lines:
        text = (line or "").strip()
        if not text:
            y -= 8
            continue
        wrapped: List[str] = []
        while len(text) > 112:
            cut = text.rfind(" ", 0, 112)
            if cut <= 0:
                cut = 112
            wrapped.append(text[:cut])
            text = text[cut:].lstrip()
        wrapped.append(text)
        for part in wrapped:
            if y < 56:
                c.showPage()
                c.setFont("Helvetica", 10)
                y = height - 48
            c.drawString(48, y, part)
            y -= 14
    c.save()
    return buffer.getvalue()


def _brief_pdf_lines(resp: Dict[str, Any]) -> List[str]:
    brief = resp.get("brief") or {}
    ed = brief.get("executive_diagnosis") or {}
    mp = brief.get("market_position") or {}
    cc = brief.get("competitive_context") or {}
    csg = brief.get("competitive_service_gap") or {}
    cd = brief.get("competitive_delta") or {}
    ds = brief.get("demand_signals") or {}
    review_intel = brief.get("review_intelligence") or {}
    ht = brief.get("high_ticket_gaps") or {}
    rucg = brief.get("revenue_upside_capture_gap") or {}
    sg = brief.get("strategic_gap") or {}
    ci = brief.get("conversion_infrastructure") or {}
    convs = brief.get("conversion_structure") or {}
    market_sat = brief.get("market_saturation") or {}
    geo = brief.get("geo_coverage") or {}
    intervention_structured = brief.get("intervention_plan_structured") or []
    plan = brief.get("intervention_plan") or []
    risks = brief.get("risk_flags") or []
    evidence = brief.get("evidence_bullets") or []

    def _opp_label() -> str:
        opp = ed.get("opportunity_profile")
        if isinstance(opp, dict):
            lbl = opp.get("label") or "—"
            why = opp.get("why")
            return f"{lbl} ({why})" if why else str(lbl)
        return str(opp or resp.get("opportunity_profile") or "—")

    def _first_demand_line() -> str:
        if ds.get("google_ads_line"):
            return f"Demand signal: Google Ads {ds.get('google_ads_line')}."
        if ds.get("organic_visibility_tier"):
            return f"Demand signal: organic visibility is {ds.get('organic_visibility_tier')}."
        if ds.get("review_velocity_30d") is not None:
            return f"Demand signal: review velocity ~{ds.get('review_velocity_30d')} in 30 days."
        return "Demand signal: —"

    missing_pages = ht.get("missing_landing_pages") or []
    lines: List[str] = [
        f"Business: {resp.get('business_name', '')}",
        f"Location: {resp.get('city', '')}{', ' + str(resp.get('state')) if resp.get('state') else ''}",
        "",
        "Problem Headline",
        f"{ed.get('constraint') or resp.get('constraint') or '—'}, {ed.get('primary_leverage') or resp.get('primary_leverage') or '—'}.",
        f"Opportunity Profile: {_opp_label()}",
        f"Market density: {mp.get('market_density') or resp.get('market_density') or '—'}.",
        _first_demand_line(),
        "",
        "Top KPI Row",
        f"Reviews: {mp.get('reviews') or '—'} (local avg {mp.get('local_avg') or '—'})",
        f"Conversion: {'No data'}",
        f"Ad status: {ds.get('google_ads_line') or resp.get('paid_status') or '—'}",
        f"Market density: {mp.get('market_density') or resp.get('market_density') or '—'}",
        f"Last review: {('~' + str(ds.get('last_review_days_ago')) + ' days ago') if ds.get('last_review_days_ago') is not None else '—'}",
        f"Review velocity: {('~' + str(ds.get('review_velocity_30d')) + ' in 30d') if ds.get('review_velocity_30d') is not None else '—'}",
        "",
        "Spend / Pages / Keywords",
        f"PPC Status: {ds.get('google_ads_line') or '—'}",
        f"Landing pages: {len(missing_pages)} missing" if missing_pages else "Landing pages: all key pages present",
        "Keywords/SERP: excluded from V1",
    ]

    lines.extend(["", "Paid & Demand"])
    lines.append(f"Paid status: {ds.get('google_ads_line') or '—'}")
    lines.append(f"Opportunity value: {ed.get('modeled_revenue_upside') or '—'}")
    if ds.get("paid_channels_detected"):
        lines.append(f"Paid channels: {', '.join(str(x) for x in ds.get('paid_channels_detected'))}")
    if ds.get("organic_visibility_tier"):
        reason = f" — {ds.get('organic_visibility_reason')}" if ds.get("organic_visibility_reason") else ""
        lines.append(f"Organic visibility: {ds.get('organic_visibility_tier')}{reason}")

    lines.extend(["", "Service / Page Analysis"])
    if ht.get("high_ticket_services_detected"):
        lines.append(f"Detected high-value services: {', '.join(str(x) for x in ht.get('high_ticket_services_detected'))}")
    if missing_pages:
        lines.append(f"Missing landing pages: {', '.join(str(x) for x in missing_pages)}")
    if ht.get("schema"):
        lines.append(f"Schema bonus: {ht.get('schema')}")
    if cd:
        has_comp_site_metrics = bool(
            cd.get("competitor_metrics_available")
            or cd.get("competitor_avg_service_pages") is not None
            or cd.get("competitor_avg_pages_with_schema") is not None
            or cd.get("competitor_avg_word_count") is not None
        )
        lines.append(
            (
                f"Service pages: {cd.get('target_service_page_count', '—')} vs competitor avg "
                f"{cd.get('competitor_avg_service_pages', '—')}"
                if has_comp_site_metrics
                else f"Service pages: {cd.get('target_service_page_count', '—')}"
            )
        )
        lines.append(
            (
                f"Structured trust pages (FAQ/schema bonus): {cd.get('target_pages_with_faq_schema', '—')} vs competitor avg "
                f"{cd.get('competitor_avg_pages_with_schema', '—')}"
                if has_comp_site_metrics
                else f"Structured trust pages (FAQ/schema bonus): {cd.get('target_pages_with_faq_schema', '—')}"
            )
        )
        lines.append(
            (
                f"Content depth: {cd.get('target_avg_word_count_service_pages', '—')} words vs competitor avg "
                f"{cd.get('competitor_avg_word_count', '—')}"
                if has_comp_site_metrics
                else f"Content depth: {cd.get('target_avg_word_count_service_pages', '—')} words"
            )
        )
    for svc in ht.get("service_level_upside") or []:
        if isinstance(svc, dict):
            lines.append(f"Revenue impact — {svc.get('service', 'service')}: {svc.get('upside', '—')}")

    lines.extend(["", "Share of Voice / Competitive Position"])
    if cc.get("line1"):
        lines.append(str(cc.get("line1")))
    if cc.get("line2"):
        lines.append(str(cc.get("line2")))
    if cc.get("line3"):
        lines.append(str(cc.get("line3")))
    for item in cc.get("line3_items") or []:
        lines.append(f"- {item}")
    if sg and sg.get("competitor_name"):
        lines.append(
            f"Nearest competitor: {sg.get('competitor_name')} ({sg.get('competitor_reviews', '—')} reviews, "
            f"{sg.get('distance_miles', '—')} mi)"
        )
    if csg and csg.get("competitor_name"):
        lines.append(
            f"Service gap competitor: {csg.get('competitor_name')} ({csg.get('service', '—')}, "
            f"{csg.get('distance_miles', '—')} mi)"
        )

    lines.extend(["", "Review Authority & Velocity"])
    lines.append(f"Reviews: {mp.get('reviews') or '—'} (local avg {mp.get('local_avg') or '—'})")
    if ds.get("last_review_days_ago") is not None:
        lines.append(f"Last review: ~{ds.get('last_review_days_ago')} days ago")
    if ds.get("review_velocity_30d") is not None:
        lines.append(f"Review velocity: ~{ds.get('review_velocity_30d')} in 30 days")
    if review_intel.get("summary"):
        lines.append(f"Review summary: {review_intel.get('summary')}")
    if review_intel.get("service_mentions"):
        lines.append(f"Service mentions: {review_intel.get('service_mentions')}")
    if review_intel.get("complaint_themes"):
        lines.append(f"Complaint themes: {review_intel.get('complaint_themes')}")

    lines.extend(["", "Validated Revenue Uplift"])
    if rucg and rucg.get("primary_service"):
        lines.append(f"Primary service: {rucg.get('primary_service')}")
        lines.append(f"Consult range: {rucg.get('consult_low', '—')}–{rucg.get('consult_high', '—')} / month")
        lines.append(f"Case value: ${int(rucg.get('case_low', 0)):,}–${int(rucg.get('case_high', 0)):,}")
        lines.append(f"Annual range: ${int(rucg.get('annual_low', 0)):,}–${int(rucg.get('annual_high', 0)):,}")
    if ed.get("modeled_revenue_upside"):
        lines.append(f"Total projected uplift: {ed.get('modeled_revenue_upside')}")
    if rucg.get("method_note"):
        lines.append(f"Method: {rucg.get('method_note')}")

    if intervention_structured:
        lines.extend(["", "Intervention Plan"])
        for step in intervention_structured:
            if isinstance(step, dict):
                lines.append(
                    f"- Step {step.get('step', '—')} [{step.get('category', 'Operational')}]: {step.get('action', '—')}"
                )
    elif plan:
        lines.extend(["", "Intervention Plan"])
        for step in plan[:3]:
            lines.append(f"- {step}")

    lines.extend(["", "Conversion Infrastructure"])
    if ci:
        if ci.get("online_booking") is not None:
            lines.append(f"Online Booking: {'Yes' if ci.get('online_booking') else 'No'}")
        if ci.get("contact_form") is not None:
            lines.append(f"Contact Form: {'Yes' if ci.get('contact_form') else 'No'}")
        if ci.get("phone_prominent") is not None:
            lines.append(f"Phone Prominent: {'Yes' if ci.get('phone_prominent') else 'No'}")
        if ci.get("mobile_optimized") is not None:
            lines.append(f"Mobile Optimized: {'Yes' if ci.get('mobile_optimized') else 'No'}")
        if ci.get("page_load_ms") is not None:
            lines.append(f"Page Load: {ci.get('page_load_ms')} ms")
    if convs:
        if convs.get("phone_clickable") is not None:
            lines.append(f"Phone clickable: {'Yes' if convs.get('phone_clickable') else 'No'}")
        if convs.get("cta_count") is not None:
            lines.append(f"CTA count: {convs.get('cta_count')}")
        if convs.get("form_single_or_multi_step"):
            lines.append(f"Form structure: {convs.get('form_single_or_multi_step')}")

    lines.extend(["", "Market Position (Full Detail)"])
    lines.append(f"Revenue Band: {mp.get('revenue_band') or '—'}")
    lines.append(f"Reviews: {mp.get('reviews') or '—'}")
    lines.append(f"Local Avg: {mp.get('local_avg') or '—'}")
    lines.append(f"Market Density: {mp.get('market_density') or '—'}")
    if mp.get("revenue_band_method"):
        lines.append(f"Method note: {mp.get('revenue_band_method')}")

    lines.extend(["", "Competitive Context (Full Detail)"])
    if cc.get("line1"):
        lines.append(str(cc.get("line1")))
    if cc.get("line2"):
        lines.append(str(cc.get("line2")))
    if cc.get("line3"):
        lines.append(str(cc.get("line3")))
    for item in cc.get("line3_items") or []:
        lines.append(f"- {item}")

    lines.extend(["", "Competitive Service Gap (Full Detail)"])
    if csg:
        lines.append(f"Type: {csg.get('type') or '—'}")
        lines.append(f"Service: {csg.get('service') or '—'}")
        lines.append(f"Nearest competitor: {csg.get('competitor_name') or '—'}")
        lines.append(f"Competitor reviews: {csg.get('competitor_reviews', '—')}")
        lines.append(f"Lead reviews: {csg.get('lead_reviews', '—')}")
        if csg.get("distance_miles") is not None:
            lines.append(f"Distance: {csg.get('distance_miles')} mi")
        if csg.get("schema_missing"):
            lines.append("Schema bonus: Not detected")

    lines.extend(["", "Competitive Delta (Full Detail)"])
    if cd:
        lines.append(f"Target service pages: {cd.get('target_service_page_count', '—')}")
        lines.append(f"Target structured trust pages (FAQ/schema bonus): {cd.get('target_pages_with_faq_schema', '—')}")
        lines.append(f"Target avg word count: {cd.get('target_avg_word_count_service_pages', '—')}")
        lines.append(f"Competitor avg service pages: {cd.get('competitor_avg_service_pages', '—')}")
        lines.append(f"Competitor avg structured trust pages: {cd.get('competitor_avg_pages_with_schema', '—')}")
        lines.append(f"Competitor avg word count: {cd.get('competitor_avg_word_count', '—')}")
        if cd.get("competitors_sampled") is not None:
            lines.append(f"Competitors sampled: {cd.get('competitors_sampled')}")
        if cd.get("competitor_site_metrics_count") is not None:
            lines.append(f"Competitor sites crawled: {cd.get('competitor_site_metrics_count')}")
        if cd.get("competitor_crawl_note"):
            lines.append(f"Crawl note: {cd.get('competitor_crawl_note')}")

    lines.extend(["", "Demand Signals (Full Detail)"])
    if ds:
        if ds.get("google_ads_line"):
            lines.append(f"Google Ads: {ds.get('google_ads_line')}")
        if ds.get("google_ads_source"):
            lines.append(f"Google Ads source: {ds.get('google_ads_source')}")
        if ds.get("meta_ads_line"):
            lines.append(f"Meta Ads: {ds.get('meta_ads_line')}")
        if ds.get("meta_ads_source"):
            lines.append(f"Meta Ads source: {ds.get('meta_ads_source')}")
        if ds.get("paid_channels_detected"):
            lines.append(f"Paid channels detected: {', '.join(str(x) for x in ds.get('paid_channels_detected'))}")
        if ds.get("organic_visibility_tier"):
            reason = f" — {ds.get('organic_visibility_reason')}" if ds.get("organic_visibility_reason") else ""
            lines.append(f"Organic Visibility: {ds.get('organic_visibility_tier')}{reason}")
        if ds.get("last_review_days_ago") is not None:
            lines.append(f"Last Review: ~{ds.get('last_review_days_ago')} days ago")
        if ds.get("review_velocity_30d") is not None:
            lines.append(f"Review Velocity (30d): ~{ds.get('review_velocity_30d')}")

    lines.extend(["", "Review Intelligence (Full Detail)"])
    if review_intel:
        if review_intel.get("review_sample_size") is not None:
            lines.append(f"Review sample size: {review_intel.get('review_sample_size')}")
        if review_intel.get("summary"):
            lines.append(f"Summary: {review_intel.get('summary')}")
        if review_intel.get("service_mentions"):
            lines.append(f"Service mentions: {review_intel.get('service_mentions')}")
        if review_intel.get("complaint_themes"):
            lines.append(f"Complaint themes: {review_intel.get('complaint_themes')}")

    lines.extend(["", "High-Ticket Gaps (Full Detail)"])
    if ht:
        if ht.get("high_ticket_services_detected"):
            lines.append(f"Detected services: {', '.join(str(x) for x in ht.get('high_ticket_services_detected'))}")
        if ht.get("missing_landing_pages"):
            lines.append(f"Missing landing pages: {', '.join(str(x) for x in ht.get('missing_landing_pages'))}")
        if ht.get("schema"):
            lines.append(f"Schema bonus: {ht.get('schema')}")
        for svc in ht.get("service_level_upside") or []:
            if isinstance(svc, dict):
                lines.append(f"- {svc.get('service', 'Service')}: {svc.get('upside', '—')}")

    lines.extend(["", "Strategic Gap (Full Detail)"])
    if sg and sg.get("competitor_name"):
        comparison = "relative to that competitor."
        try:
            c_reviews = int(sg.get("competitor_reviews")) if sg.get("competitor_reviews") is not None else None
            l_reviews = int(sg.get("lead_reviews")) if sg.get("lead_reviews") is not None else None
            if c_reviews is not None and l_reviews is not None:
                if l_reviews < c_reviews:
                    comparison = "below that competitor."
                elif l_reviews > c_reviews:
                    comparison = "above that competitor."
                else:
                    comparison = "in line with that competitor."
        except (TypeError, ValueError):
            pass
        lines.append(
            f"Nearest competitor {sg.get('competitor_name')} holds {sg.get('competitor_reviews', '—')} reviews "
            f"within {sg.get('distance_miles', '—')} miles in a {sg.get('market_density', '—')} density market."
        )
        lines.append(f"This practice's review position is {comparison}")

    lines.extend(["", "Market Saturation"])
    if market_sat:
        if market_sat.get("top_5_avg_reviews") is not None:
            lines.append(f"Top 5 avg reviews: {market_sat.get('top_5_avg_reviews')}")
        if market_sat.get("competitor_median_reviews") is not None:
            lines.append(f"Competitor median reviews: {market_sat.get('competitor_median_reviews')}")
        if market_sat.get("target_gap_from_median") is not None:
            lines.append(f"Target gap from median: {market_sat.get('target_gap_from_median')}")

    lines.extend(["", "Geographic Coverage"])
    if geo:
        if geo.get("city_or_near_me_page_count") is not None:
            lines.append(f"City/near-me pages: {geo.get('city_or_near_me_page_count')}")
        if geo.get("has_multi_location_page") is not None:
            lines.append(f"Multi-location page: {'Detected' if geo.get('has_multi_location_page') else 'Not detected'}")

    if risks:
        lines.extend(["", "Risk Flags"])
        for risk in risks:
            lines.append(f"- {risk}")
    if evidence:
        lines.extend(["", "Evidence"])
        for item in evidence:
            lines.append(f"- {item}")
    return lines


@router.get("/outcomes/summary")
def outcomes_summary(request: Request):
    user_id = getattr(request.state, "user_id", 1)
    return get_outcome_summary_for_user(user_id)


@router.get("/outcomes")
def outcomes_list(request: Request, limit: int = Query(default=200, ge=1, le=1000)):
    user_id = getattr(request.state, "user_id", 1)
    return {"items": list_outcomes_for_user(user_id, limit=limit)}


@router.post("/{diagnostic_id}/share")
def create_share_link(diagnostic_id: int, request: Request):
    user_id = getattr(request.state, "user_id", 1)
    row = get_diagnostic(diagnostic_id, user_id)
    if not row:
        raise HTTPException(status_code=404, detail="Diagnostic not found")
    token = secrets.token_hex(16)
    expires_at = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()
    create_brief_share_token(
        diagnostic_id=diagnostic_id,
        user_id=user_id,
        token=token,
        expires_at=expires_at,
    )
    base = str(request.base_url).rstrip("/")
    return {
        "token": token,
        "share_url": f"{base}/brief/s/{token}",
        "expires_at": expires_at,
    }


@router.get("/{diagnostic_id}/brief.pdf")
def diagnostic_brief_pdf(diagnostic_id: int, request: Request):
    user_id = getattr(request.state, "user_id", 1)
    row = get_diagnostic(diagnostic_id, user_id)
    if not row:
        raise HTTPException(status_code=404, detail="Diagnostic not found")
    resp = row.get("response") or {}
    business = str(resp.get("business_name") or f"diagnostic-{diagnostic_id}").replace("/", "-")
    city = str(resp.get("city") or "").replace("/", "-")
    filename = f"Brief-{business}-{city}.pdf".replace(" ", "-")
    pdf_bytes = _render_pdf_from_lines(
        f"Revenue Intelligence Brief — {resp.get('business_name', 'Business')}",
        _brief_pdf_lines(resp),
    )
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return Response(content=pdf_bytes, media_type="application/pdf", headers=headers)


@router.get("", response_model=DiagnosticListResponse)
def list_all(
    request: Request,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
):
    user_id = getattr(request.state, "user_id", 1)
    rows = list_diagnostics(user_id, limit=limit, offset=offset)
    total = count_diagnostics(user_id)

    items = []
    for row in rows:
        resp = row.get("response", {})
        brief = resp.get("brief", {}) or {}
        ed = brief.get("executive_diagnosis", {}) or {}

        items.append(DiagnosticListItem(
            id=row["id"],
            business_name=row["business_name"],
            city=row["city"],
            state=row.get("state"),
            place_id=row.get("place_id"),
            created_at=row["created_at"],
            opportunity_profile=resp.get("opportunity_profile"),
            constraint=resp.get("constraint"),
            modeled_revenue_upside=ed.get("modeled_revenue_upside"),
        ))

    return DiagnosticListResponse(items=items, total=total, limit=limit, offset=offset)


@router.get("/{diagnostic_id}", response_model=DiagnosticResponse)
def get_one(diagnostic_id: int, request: Request):
    user_id = getattr(request.state, "user_id", 1)
    row = get_diagnostic(diagnostic_id, user_id)
    if not row:
        raise HTTPException(status_code=404, detail="Diagnostic not found")
    out = _response_from_saved(row["response"])
    if not out.phone or not out.website:
        fallback = get_territory_contact_for_diagnostic(diagnostic_id, place_id=row.get("place_id"))
        if not out.phone and fallback.get("phone"):
            out.phone = fallback["phone"]
        if not out.website and fallback.get("website"):
            out.website = fallback["website"]
    return out


@router.delete("/{diagnostic_id}")
def remove(diagnostic_id: int, request: Request):
    user_id = getattr(request.state, "user_id", 1)
    deleted = delete_diagnostic(diagnostic_id, user_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Diagnostic not found")
    return {"deleted": True}
