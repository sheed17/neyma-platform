"""
Opportunity Intelligence Builder

This is the CORE intelligence layer. It consumes extracted signals
and emits structured Opportunity objects per lead.

Philosophy:
- We do NOT score leads
- We extract business signals, interpret opportunities, then provide
  prioritization guidance (High / Medium / Low) as a UI affordance
- Opportunities are the primary intelligence output
- Unknown ≠ bad (null signals don't penalize, just reduce confidence)

Design Principles:
- Conservative > clever
- Explainable > complex
- Accuracy and trust > completeness
"""

from typing import Dict, List, Optional
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

# Review thresholds
REVIEW_FRESH_DAYS = 30
REVIEW_WARM_DAYS = 90
REVIEW_STALE_DAYS = 180
LOW_REVIEW_COUNT = 30

# Confidence weights for signals used in opportunity detection
SIGNAL_WEIGHTS = {
    "has_website": 1.0,
    "website_accessible": 1.0,
    "has_phone": 1.0,
    "has_contact_form": 1.5,
    "has_email": 1.0,
    "has_automated_scheduling": 1.5,
    "review_count": 1.0,
    "last_review_days_ago": 1.0,
    "runs_paid_ads": 1.0,
    "hiring_active": 0.5,
}


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class Opportunity:
    """
    A single detected business opportunity.
    
    This is the primary intelligence output of the system.
    """
    type: str                      # e.g. "Paid Traffic Leakage"
    strength: str                  # "Weak", "Moderate", "Strong"
    timing: str                    # "Cold", "Emerging", "Active", "Urgent"
    evidence: List[str]            # Human-readable reasons
    confidence: float              # 0.0-1.0 for this specific opportunity
    
    def to_dict(self) -> Dict:
        return {
            "type": self.type,
            "strength": self.strength,
            "timing": self.timing,
            "evidence": self.evidence,
            "confidence": self.confidence,
        }


@dataclass
class OpportunityReport:
    """
    Complete opportunity intelligence for a lead.
    
    Opportunities are primary. Priority is a derived UI affordance.
    """
    opportunities: List[Opportunity]
    priority: str                  # "High", "Medium", "Low" (derived)
    confidence: float              # Overall data coverage
    review_summary: Dict           # Review context for agencies
    
    def to_dict(self) -> Dict:
        return {
            "opportunities": [o.to_dict() for o in self.opportunities],
            "priority": self.priority,
            "confidence": self.confidence,
            "review_summary": self.review_summary,
        }


# =============================================================================
# HELPERS
# =============================================================================

def _is_true(value) -> bool:
    return value is True

def _is_false(value) -> bool:
    return value is False

def _is_known(value) -> bool:
    return value is not None

def _get_freshness(days: Optional[int]) -> str:
    if days is None:
        return "Unknown"
    if days <= REVIEW_FRESH_DAYS:
        return "Fresh"
    if days <= REVIEW_WARM_DAYS:
        return "Warm"
    if days <= REVIEW_STALE_DAYS:
        return "Stale"
    return "Very Stale"


# =============================================================================
# CONFIDENCE CALCULATION
# =============================================================================

def calculate_confidence(signals: Dict) -> float:
    """
    Calculate overall data coverage confidence.
    
    confidence = (weight of known signals) / (total weight)
    Known = true or false. Unknown = null.
    """
    observed_weight = 0.0
    total_weight = 0.0
    
    for signal_name, weight in SIGNAL_WEIGHTS.items():
        total_weight += weight
        value = signals.get(signal_name)
        if value is None:
            value = signals.get(f"signal_{signal_name}")
        if _is_known(value):
            observed_weight += weight
    
    if total_weight == 0:
        return 0.0
    
    return round(observed_weight / total_weight, 2)


# =============================================================================
# REVIEW SUMMARY (preserved from scoring system)
# =============================================================================

def _build_review_summary(signals: Dict) -> Dict:
    """Build human-readable review context for agencies."""
    review_count = signals.get("review_count")
    rating = signals.get("rating")
    last_review_days = signals.get("last_review_days_ago")
    
    freshness = _get_freshness(last_review_days)
    
    if last_review_days is None:
        last_review_text = "Unknown"
    elif last_review_days <= REVIEW_WARM_DAYS:
        last_review_text = f"{last_review_days} days ago"
    else:
        months = last_review_days // 30
        last_review_text = f"~{months} months ago"
    
    if review_count is None:
        volume = "Unknown"
    elif review_count < 10:
        volume = "Very Low"
    elif review_count < LOW_REVIEW_COUNT:
        volume = "Low"
    elif review_count < 100:
        volume = "Moderate"
    else:
        volume = "High"
    
    return {
        "review_count": review_count,
        "rating": rating,
        "last_review_days_ago": last_review_days,
        "last_review_text": last_review_text,
        "freshness": freshness,
        "volume": volume,
    }


# =============================================================================
# OPPORTUNITY DETECTORS
# =============================================================================
# Each detector returns an Opportunity or None.
# Detectors only fire when evidence exists.
# null signals NEVER produce opportunities (we don't guess).
# =============================================================================

def _detect_paid_traffic_leakage(signals: Dict) -> Optional[Opportunity]:
    """
    Running paid ads + no booking/contact optimization = leaking money.
    
    This is a STRONG opportunity: they have budget but poor conversion.
    """
    runs_ads = signals.get("runs_paid_ads")
    
    if not _is_true(runs_ads):
        return None
    
    # Check for conversion gaps
    evidence = []
    gap_count = 0
    
    channels = signals.get("paid_ads_channels") or []
    channel_str = ", ".join(channels) if channels else "detected"
    evidence.append(f"Running paid ads ({channel_str})")
    
    if _is_false(signals.get("has_contact_form")):
        evidence.append("No contact form to capture ad traffic")
        gap_count += 1

    if _is_false(signals.get("has_automated_scheduling")):
        evidence.append("No automated scheduling to convert visitors")
        gap_count += 1
    
    if _is_false(signals.get("mobile_friendly")):
        evidence.append("Website not mobile-friendly (ads often serve mobile)")
        gap_count += 1
    
    if not _is_true(signals.get("website_accessible")):
        evidence.append("Website accessibility issues (ad spend may be wasted)")
        gap_count += 2
    
    if gap_count == 0:
        # Ads running but well-optimized, still notable but weak
        evidence.append("Ad spend active — review ROI opportunity")
        return Opportunity(
            type="Paid Traffic Optimization",
            strength="Weak",
            timing="Active",
            evidence=evidence,
            confidence=0.7,
        )
    
    strength = "Strong" if gap_count >= 2 else "Moderate"
    
    return Opportunity(
        type="Paid Traffic Leakage",
        strength=strength,
        timing="Urgent" if gap_count >= 2 else "Active",
        evidence=evidence,
        confidence=0.8,
    )


def _detect_operational_scaling_pressure(signals: Dict) -> Optional[Opportunity]:
    """
    Hiring active + manual operations = scaling pain.
    
    Business is growing but hasn't invested in automation.
    """
    hiring = signals.get("hiring_active")
    
    if not _is_true(hiring):
        return None
    
    evidence = []
    roles = signals.get("hiring_roles") or []
    
    if roles:
        evidence.append(f"Actively hiring: {', '.join(roles)}")
    else:
        evidence.append("Hiring activity detected on website")
    
    strength = "Moderate"
    timing = "Active"
    
    if _is_false(signals.get("has_automated_scheduling")):
        evidence.append("Running manual scheduling during growth phase")
        strength = "Strong"
        timing = "Urgent"
    
    if _is_false(signals.get("has_contact_form")):
        evidence.append("No online contact form to handle increased volume")
        strength = "Strong"
    
    review_count = signals.get("review_count")
    if review_count is not None and review_count > 50:
        evidence.append(f"Established business ({review_count} reviews) in growth mode")
    
    return Opportunity(
        type="Operational Scaling Pressure",
        strength=strength,
        timing=timing,
        evidence=evidence,
        confidence=0.7 if not roles else 0.8,
    )


def _detect_reputation_recovery(signals: Dict) -> Optional[Opportunity]:
    """
    Declining or stale reviews = reputation pain point.
    
    Business may be losing customers due to poor/absent online reputation.
    """
    review_count = signals.get("review_count")
    last_review_days = signals.get("last_review_days_ago")
    rating = signals.get("rating")
    rating_delta = signals.get("rating_delta_60d")
    
    evidence = []
    signals_found = 0
    
    # Stale reviews
    if last_review_days is not None and last_review_days > REVIEW_STALE_DAYS:
        months = last_review_days // 30
        evidence.append(f"No recent reviews in {months}+ months")
        signals_found += 1
    
    # Low review count
    if review_count is not None and review_count < 10:
        evidence.append(f"Very low review volume ({review_count} reviews)")
        signals_found += 1
    elif review_count is not None and review_count < LOW_REVIEW_COUNT:
        evidence.append(f"Low review volume ({review_count} reviews)")
        signals_found += 1
    
    # Declining ratings
    if rating_delta is not None and rating_delta < -0.3:
        evidence.append(f"Rating trending down ({rating_delta:+.1f} in last 60 days)")
        signals_found += 1
    
    # Low overall rating
    if rating is not None and rating < 4.0:
        evidence.append(f"Below-average rating ({rating})")
        signals_found += 1
    
    if signals_found == 0:
        return None
    
    if signals_found >= 3:
        strength = "Strong"
        timing = "Urgent"
    elif signals_found >= 2:
        strength = "Moderate"
        timing = "Active"
    else:
        strength = "Weak"
        timing = "Emerging"
    
    return Opportunity(
        type="Reputation Recovery",
        strength=strength,
        timing=timing,
        evidence=evidence,
        confidence=0.85,  # Review data from Google is reliable
    )


def _detect_digital_presence_gap(signals: Dict) -> Optional[Opportunity]:
    """
    Missing or broken web presence = needs help going digital.
    
    Common for small HVAC businesses.
    """
    has_website = signals.get("has_website")
    website_accessible = signals.get("website_accessible")
    has_phone = signals.get("has_phone")
    review_count = signals.get("review_count")
    
    evidence = []
    
    if _is_false(has_website):
        evidence.append("No business website listed")
        
        if _is_true(has_phone) and review_count is not None and review_count >= 5:
            evidence.append(
                f"Active business ({review_count} reviews) operating without web presence"
            )
            return Opportunity(
                type="Digital Presence Gap",
                strength="Strong",
                timing="Active",
                evidence=evidence,
                confidence=0.9,
            )
        
        return Opportunity(
            type="Digital Presence Gap",
            strength="Moderate",
            timing="Emerging",
            evidence=evidence,
            confidence=0.9,
        )
    
    if _is_true(has_website) and _is_false(website_accessible):
        evidence.append("Website exists but is not accessible")
        return Opportunity(
            type="Digital Presence Gap",
            strength="Strong",
            timing="Urgent",
            evidence=evidence,
            confidence=0.9,
        )
    
    # Check for SSL issues on accessible sites
    if _is_true(has_website) and _is_false(signals.get("has_ssl")):
        evidence.append("Website lacks SSL (no HTTPS)")
    
    if _is_true(has_website) and _is_false(signals.get("mobile_friendly")):
        evidence.append("Website not mobile-friendly")
    
    if evidence:
        return Opportunity(
            type="Digital Presence Gap",
            strength="Weak",
            timing="Cold",
            evidence=evidence,
            confidence=0.8,
        )
    
    return None


def _detect_inbound_optimization(signals: Dict) -> Optional[Opportunity]:
    """
    Has website but missing contact/conversion paths.
    
    Business invested in web presence but isn't capturing leads.
    """
    if not _is_true(signals.get("has_website")):
        return None
    
    if not _is_true(signals.get("website_accessible")):
        return None
    
    evidence = []
    gaps = 0
    
    if _is_false(signals.get("has_contact_form")):
        evidence.append("No contact form on website")
        gaps += 1
    
    if _is_false(signals.get("has_email")):
        evidence.append("No email address visible on website")
        gaps += 1
    
    if _is_false(signals.get("has_automated_scheduling")):
        evidence.append("No automated scheduling system")
        gaps += 1
    
    if gaps == 0:
        return None
    
    if gaps >= 2:
        strength = "Strong"
        timing = "Active"
        evidence.insert(0, "Website exists but missing key conversion paths")
    else:
        strength = "Moderate"
        timing = "Emerging"
    
    return Opportunity(
        type="Inbound Optimization",
        strength=strength,
        timing=timing,
        evidence=evidence,
        confidence=0.75,
    )


def _detect_manual_operations_opportunity(signals: Dict) -> Optional[Opportunity]:
    """
    Manual operations = room for automation/optimization services.
    
    For HVAC: phone is the primary booking mechanism.
    No automated scheduling = they're running everything manually.
    """
    scheduling = signals.get("has_automated_scheduling")
    
    if not _is_false(scheduling):
        # Only fire if we KNOW they don't have automation
        return None
    
    evidence = ["No automated scheduling system detected"]
    
    has_phone = signals.get("has_phone")
    review_count = signals.get("review_count")
    
    if _is_true(has_phone):
        evidence.append("Phone is primary contact method (manual intake)")
    
    if review_count is not None and review_count > 20:
        evidence.append(
            f"Active business ({review_count} reviews) running manual operations"
        )
        strength = "Strong"
        timing = "Active"
    elif review_count is not None and review_count > 5:
        strength = "Moderate"
        timing = "Emerging"
    else:
        strength = "Weak"
        timing = "Cold"
    
    return Opportunity(
        type="Manual Operations Opportunity",
        strength=strength,
        timing=timing,
        evidence=evidence,
        confidence=0.8,
    )


# =============================================================================
# PRIORITY DERIVATION
# =============================================================================

# Strength → numeric for sorting
STRENGTH_SCORES = {"Strong": 3, "Moderate": 2, "Weak": 1}

# Timing → numeric for urgency
TIMING_SCORES = {"Urgent": 4, "Active": 3, "Emerging": 2, "Cold": 1}


def _derive_priority(
    opportunities: List[Opportunity],
    confidence: float
) -> str:
    """
    Derive priority bucket from opportunities.
    
    Priority is a UI affordance, NOT the core abstraction.
    It's derived from:
    - Best opportunity strength
    - Best timing urgency
    - Overall confidence
    """
    if not opportunities:
        return "Low"
    
    # Get the strongest signals
    max_strength = max(STRENGTH_SCORES.get(o.strength, 0) for o in opportunities)
    max_timing = max(TIMING_SCORES.get(o.timing, 0) for o in opportunities)
    num_opportunities = len(opportunities)
    
    # Composite score for prioritization (internal only)
    composite = max_strength + max_timing + min(num_opportunities, 3)
    
    # Apply confidence dampening
    # Low confidence limits how high priority can go
    if confidence < 0.5:
        return "Low"
    
    if confidence < 0.7 and composite >= 8:
        composite = 7  # Cap at Medium if confidence is mediocre
    
    if composite >= 8:
        return "High"
    elif composite >= 5:
        return "Medium"
    else:
        return "Low"


# =============================================================================
# MAIN OPPORTUNITY BUILDER
# =============================================================================

def analyze_opportunities(lead: Dict) -> OpportunityReport:
    """
    Analyze a lead and emit structured Opportunity Intelligence.
    
    This is the primary intelligence function.
    
    Args:
        lead: Lead dictionary with signals (signal_ prefix or not)
    
    Returns:
        OpportunityReport with opportunities, priority, and context
    """
    # Normalize signals - handle both prefixed and non-prefixed
    signals = {}
    for key, value in lead.items():
        if key.startswith("signal_"):
            clean_key = key[7:]
            signals[clean_key] = value
        else:
            signals[key] = value
    
    # Run all opportunity detectors
    detectors = [
        _detect_paid_traffic_leakage,
        _detect_operational_scaling_pressure,
        _detect_reputation_recovery,
        _detect_digital_presence_gap,
        _detect_inbound_optimization,
        _detect_manual_operations_opportunity,
    ]
    
    opportunities = []
    for detector in detectors:
        opp = detector(signals)
        if opp is not None:
            opportunities.append(opp)
    
    # Sort by strength (strongest first)
    opportunities.sort(
        key=lambda o: (
            STRENGTH_SCORES.get(o.strength, 0),
            TIMING_SCORES.get(o.timing, 0)
        ),
        reverse=True
    )
    
    # Limit to top 3 (most relevant)
    opportunities = opportunities[:3]
    
    # Calculate overall confidence
    confidence = calculate_confidence(signals)
    
    # Build review summary
    review_summary = _build_review_summary(signals)
    
    # Derive priority (UI affordance)
    priority = _derive_priority(opportunities, confidence)
    
    return OpportunityReport(
        opportunities=opportunities,
        priority=priority,
        confidence=confidence,
        review_summary=review_summary,
    )


def analyze_opportunities_batch(leads: List[Dict]) -> List[Dict]:
    """
    Analyze opportunities for multiple leads.
    
    Returns leads with opportunity intelligence merged in.
    """
    results = []
    
    for lead in leads:
        report = analyze_opportunities(lead)
        
        enriched = lead.copy()
        enriched["opportunities"] = [o.to_dict() for o in report.opportunities]
        enriched["priority"] = report.priority
        enriched["confidence"] = report.confidence
        enriched["review_summary"] = report.review_summary
        
        results.append(enriched)
    
    return results


def get_opportunity_summary(analyzed_leads: List[Dict]) -> Dict:
    """
    Generate summary statistics for analyzed leads.
    """
    if not analyzed_leads:
        return {"total": 0}
    
    total = len(analyzed_leads)
    
    # Priority distribution
    priorities = [l.get("priority", "Unknown") for l in analyzed_leads]
    high = sum(1 for p in priorities if p == "High")
    medium = sum(1 for p in priorities if p == "Medium")
    low = sum(1 for p in priorities if p == "Low")
    
    # Opportunity type distribution
    opp_types = {}
    for lead in analyzed_leads:
        for opp in lead.get("opportunities", []):
            opp_type = opp.get("type", "Unknown")
            opp_types[opp_type] = opp_types.get(opp_type, 0) + 1
    
    # Confidence
    confidences = [l.get("confidence", 0) for l in analyzed_leads]
    
    # Average opportunities per lead
    opp_counts = [len(l.get("opportunities", [])) for l in analyzed_leads]
    
    return {
        "total_leads": total,
        "priority": {
            "high": high,
            "high_pct": round(high / total * 100, 1),
            "medium": medium,
            "medium_pct": round(medium / total * 100, 1),
            "low": low,
            "low_pct": round(low / total * 100, 1),
        },
        "confidence": {
            "avg": round(sum(confidences) / total, 2),
            "min": min(confidences),
            "max": max(confidences),
        },
        "opportunities": {
            "avg_per_lead": round(sum(opp_counts) / total, 1),
            "by_type": dict(sorted(opp_types.items(), key=lambda x: x[1], reverse=True)),
        }
    }
