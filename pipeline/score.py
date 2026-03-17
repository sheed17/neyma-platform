"""
Prioritization Helper (Refactored from Lead Scoring V1)

This module provides backward-compatible scoring that wraps the
Opportunity Intelligence system.

Core Philosophy:
- We do NOT score leads as the primary abstraction
- Opportunities are the core intelligence output
- Scores exist internally for sorting/filtering only
- Priority (High/Medium/Low) is a UI affordance derived from opportunities

Backward Compatibility:
- score_lead() still works and returns ScoringResult
- score_leads_batch() still works
- All existing fields preserved
- Now also includes opportunities in output
"""

from typing import Dict, List, Tuple
from dataclasses import dataclass
import logging

from .opportunities import (
    analyze_opportunities,
    OpportunityReport,
    calculate_confidence,
    _build_review_summary,
    _is_true,
    _is_false,
    _is_known,
    REVIEW_FRESH_DAYS,
    REVIEW_WARM_DAYS,
    REVIEW_STALE_DAYS,
    LOW_REVIEW_COUNT,
)

logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION (preserved from V1)
# =============================================================================

BASE_SCORE = 40

# Signal weights for confidence (delegated to opportunities.py)
SIGNAL_WEIGHTS = {
    "has_website": 1.0,
    "website_accessible": 1.0,
    "has_phone": 1.0,
    "has_contact_form": 1.5,
    "has_email": 1.0,
    "has_automated_scheduling": 1.5,
    "review_count": 1.0,
    "last_review_days_ago": 1.0,
}


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class ScoringResult:
    """Result of lead analysis - backward compatible."""
    lead_score: int           # 0-100 (internal, for sorting)
    priority: str             # "High", "Medium", "Low" (from opportunities)
    confidence: float         # 0.0-1.0
    reasons: List[str]        # Human-readable explanations
    review_summary: Dict      # Review context
    opportunities: List[Dict] # NEW: opportunity intelligence (primary output)
    
    def to_dict(self) -> Dict:
        return {
            "lead_score": self.lead_score,
            "priority": self.priority,
            "confidence": self.confidence,
            "reasons": self.reasons,
            "review_summary": self.review_summary,
            "opportunities": self.opportunities,
        }


# =============================================================================
# INTERNAL SCORING RULES (for sorting, NOT primary output)
# =============================================================================

def _clamp(value: int, min_val: int, max_val: int) -> int:
    return max(min_val, min(max_val, value))


def _compute_internal_score(signals: Dict) -> Tuple[int, List[str]]:
    """
    Compute internal score for sorting/ranking purposes.
    
    This is NOT the primary intelligence output.
    It exists to provide a numeric value for ordering leads.
    """
    score = BASE_SCORE
    reasons = []
    
    # --- Reachability ---
    if _is_true(signals.get("has_website")):
        score += 10
        reasons.append("Has business website")
    
    if _is_true(signals.get("website_accessible")):
        score += 10
        reasons.append("Website is accessible and functional")
    
    if _is_true(signals.get("has_phone")):
        score += 10
        reasons.append("Phone contact available")
    
    if _is_true(signals.get("has_contact_form")):
        score += 15
        reasons.append("Accepts online requests via website")
    
    if _is_true(signals.get("has_email")):
        score += 10
        reasons.append("Email contact available")
    
    # --- Operations maturity ---
    scheduling = signals.get("has_automated_scheduling")
    if _is_false(scheduling):
        score += 20
        reasons.append("Manual scheduling detected (optimization opportunity)")
    
    # --- Reputation opportunity ---
    review_count = signals.get("review_count")
    last_review_days = signals.get("last_review_days_ago")
    
    if review_count is not None and review_count < LOW_REVIEW_COUNT:
        score += 10
        reasons.append(f"Low review volume ({review_count} reviews)")
    
    if last_review_days is not None:
        if last_review_days > REVIEW_STALE_DAYS:
            score += 15
            months = last_review_days // 30
            reasons.append(f"No recent reviews in {months}+ months")
        elif last_review_days > REVIEW_WARM_DAYS:
            score += 10
            reasons.append("Reviews becoming stale (3+ months)")
    
    # --- Disqualifiers ---
    if (_is_false(signals.get("has_phone")) and 
        _is_false(signals.get("has_contact_form")) and 
        _is_false(signals.get("has_email"))):
        score -= 40
        reasons.append("No contact methods available")
    
    if (_is_true(signals.get("has_website")) and 
        _is_false(signals.get("website_accessible"))):
        score -= 20
        reasons.append("Website exists but is not accessible")
    
    # --- Penalties for "already optimized" ---
    if _is_true(signals.get("has_automated_scheduling")):
        score -= 5
        reasons.append("Already uses scheduling automation (-5)")
    
    if _is_true(signals.get("has_trust_badges")):
        score -= 3
        reasons.append("Has trust badges (established presence, -3)")
    
    # --- Paid ads bonus (budget signal) ---
    if _is_true(signals.get("runs_paid_ads")):
        score += 5
        reasons.append("Running paid advertising (has budget)")
    
    # --- Hiring bonus (timing signal) ---
    if _is_true(signals.get("hiring_active")):
        score += 5
        reasons.append("Actively hiring (growth phase)")
    
    return score, reasons


def _apply_refinements(
    score: int,
    signals: Dict,
    confidence: float,
    review_summary: Dict,
    reasons: List[str]
) -> int:
    """Apply score refinements (confidence dampening, caps, elite gate)."""
    
    review_count = signals.get("review_count")
    freshness = review_summary.get("freshness")
    
    # Saturated market signal
    if freshness == "Fresh" and review_count is not None and review_count > 100:
        score -= 3
        reasons.append("High volume with fresh reviews (saturated, -3)")
    
    # Confidence-weighted score
    confidence_multiplier = 0.7 + 0.3 * confidence
    score = int(score * confidence_multiplier)
    
    if confidence < 0.8:
        reasons.append(f"Score adjusted for data coverage ({confidence:.0%} confidence)")
    
    # Review-count ceiling
    if review_count is not None:
        if review_count > 200:
            if score > 92:
                score = 92
                reasons.append("Capped at 92 (large brand, 200+ reviews)")
        elif review_count > 100:
            if score > 95:
                score = 95
                reasons.append("Capped at 95 (established brand, 100+ reviews)")
    
    # Elite gate
    can_be_elite = (
        _is_true(signals.get("has_website")) and
        _is_true(signals.get("has_contact_form")) and
        _is_true(signals.get("has_phone")) and
        _is_false(signals.get("has_automated_scheduling")) and
        review_count is not None and 5 <= review_count <= 80 and
        freshness != "Fresh" and
        confidence >= 0.9
    )
    
    if score >= 100 and not can_be_elite:
        score = 95
        reasons.append("Capped at 95 (does not meet elite criteria)")
    
    return _clamp(score, 0, 100)


# =============================================================================
# MAIN FUNCTION (backward compatible)
# =============================================================================

def score_lead(lead: Dict) -> ScoringResult:
    """
    Analyze a lead: extract opportunities AND compute internal score.
    
    The primary intelligence is in the opportunities.
    The score is for sorting/filtering only.
    Priority is derived from opportunities, NOT the score.
    """
    # Normalize signals
    signals = {}
    for key, value in lead.items():
        if key.startswith("signal_"):
            signals[key[7:]] = value
        else:
            signals[key] = value
    
    # --- PRIMARY: Opportunity analysis ---
    report = analyze_opportunities(lead)
    
    # --- SECONDARY: Internal score for sorting ---
    raw_score, reasons = _compute_internal_score(signals)
    
    score = _apply_refinements(
        raw_score, signals,
        report.confidence, report.review_summary,
        reasons
    )
    
    # Low confidence context
    if report.confidence < 0.5:
        reasons.append("Limited data available (low confidence)")
    
    return ScoringResult(
        lead_score=score,
        priority=report.priority,  # Derived from opportunities, NOT score
        confidence=report.confidence,
        reasons=reasons,
        review_summary=report.review_summary,
        opportunities=[o.to_dict() for o in report.opportunities],
    )


def score_leads_batch(leads: List[Dict]) -> List[Dict]:
    """
    Analyze multiple leads with opportunity intelligence + internal scoring.
    """
    scored_leads = []
    
    for lead in leads:
        result = score_lead(lead)
        
        scored_lead = lead.copy()
        scored_lead["lead_score"] = result.lead_score
        scored_lead["priority"] = result.priority
        scored_lead["confidence"] = result.confidence
        scored_lead["reasons"] = result.reasons
        scored_lead["review_summary"] = result.review_summary
        scored_lead["opportunities"] = result.opportunities
        
        scored_leads.append(scored_lead)
    
    return scored_leads


def get_scoring_summary(scored_leads: List[Dict]) -> Dict:
    """Generate summary statistics (backward compatible)."""
    if not scored_leads:
        return {"total": 0}
    
    total = len(scored_leads)
    scores = [l.get("lead_score", 0) for l in scored_leads]
    confidences = [l.get("confidence", 0) for l in scored_leads]
    priorities = [l.get("priority", "Unknown") for l in scored_leads]
    
    high = sum(1 for p in priorities if p == "High")
    medium = sum(1 for p in priorities if p == "Medium")
    low = sum(1 for p in priorities if p == "Low")
    
    # Opportunity type distribution
    opp_types = {}
    for lead in scored_leads:
        for opp in lead.get("opportunities", []):
            opp_type = opp.get("type", "Unknown")
            opp_types[opp_type] = opp_types.get(opp_type, 0) + 1
    
    opp_counts = [len(l.get("opportunities", [])) for l in scored_leads]
    
    return {
        "total_leads": total,
        "score": {
            "avg": round(sum(scores) / total, 1),
            "min": min(scores),
            "max": max(scores),
        },
        "confidence": {
            "avg": round(sum(confidences) / total, 2),
            "min": min(confidences),
            "max": max(confidences),
        },
        "priority": {
            "high": high,
            "high_pct": round(high / total * 100, 1),
            "medium": medium,
            "medium_pct": round(medium / total * 100, 1),
            "low": low,
            "low_pct": round(low / total * 100, 1),
        },
        "opportunities": {
            "avg_per_lead": round(sum(opp_counts) / total, 1),
            "by_type": dict(sorted(opp_types.items(), key=lambda x: x[1], reverse=True)),
        },
    }
