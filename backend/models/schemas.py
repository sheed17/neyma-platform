"""
Pydantic schemas for the diagnostic API.
"""

from typing import Optional, List, Any, Dict
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Diagnostic input / output
# ---------------------------------------------------------------------------

class DiagnosticRequest(BaseModel):
    """Request body for POST /diagnostic."""

    business_name: str
    city: str
    state: str
    website: Optional[str] = None
    deep_audit: Optional[bool] = False
    source_diagnostic_id: Optional[int] = None


class InterventionPlanItem(BaseModel):
    step: int
    category: str
    action: str


class ServiceIntelligence(BaseModel):
    detected_services: List[str] = []
    missing_services: List[str] = []
    schema_detected: Optional[bool] = None
    crawl_method: Optional[str] = None
    deep_scan: Optional[bool] = None
    crawl_confidence: Optional[str] = None
    pages_crawled: Optional[int] = None
    js_detected: Optional[bool] = None
    service_page_count: Optional[int] = None
    playwright_fetch_summary: Optional[Dict[str, Any]] = None
    crawl_warning: Optional[str] = None
    suppress_service_gap: Optional[bool] = None
    suppress_conversion_absence_claims: Optional[bool] = None
    suppress_revenue_modeling: Optional[bool] = None
    high_value_services: List[Dict[str, Any]] = []
    high_value_summary: Optional[Dict[str, Any]] = None
    high_value_service_leverage: Optional[str] = None
    service_page_analysis_v2: Optional[Dict[str, Any]] = None
    cta_elements: Optional[List[Dict[str, Any]]] = None
    cta_clickable_by_type: Optional[Dict[str, int]] = None
    cta_clickable_count: Optional[int] = None
    geo_intent_pages: Optional[List[Dict[str, Any]]] = None
    missing_geo_pages: Optional[List[Dict[str, Any]]] = None


class RevenueBreakdown(BaseModel):
    service: str
    consults_per_month: str
    revenue_per_case: str
    annual_revenue_range: str


class ConversionInfrastructure(BaseModel):
    online_booking: Optional[bool] = None
    contact_form: Optional[bool] = None
    phone_prominent: Optional[bool] = None
    mobile_optimized: Optional[bool] = None
    page_load_ms: Optional[int] = None


class EvidenceItem(BaseModel):
    label: str
    value: str


class LeadQualityReason(BaseModel):
    code: str
    label: str
    direction: str
    value: Optional[str] = None
    evidence_refs: Optional[List[str]] = None


class LeadQualityComponents(BaseModel):
    benefit_score: Optional[float] = None
    buyability_score: Optional[float] = None


class LeadQualityPrediction(BaseModel):
    model_config = {"populate_by_name": True}
    class_name: str = Field(default="", alias="class")
    score: float = 0.0
    prob_high_value: Optional[float] = None
    data_confidence: Optional[float] = None
    feature_scope: Optional[str] = None
    model_name: Optional[str] = None
    model_version: Optional[str] = None
    feature_version: Optional[str] = None
    label_version: Optional[str] = None
    components: Optional[LeadQualityComponents] = None
    reasons: List[LeadQualityReason] = []
    caveats: List[LeadQualityReason] = []


class DiagnosticResponse(BaseModel):
    """Structured JSON summary returned by POST /diagnostic."""

    lead_id: int
    business_name: str
    city: str
    state: Optional[str] = None
    phone: Optional[str] = None
    website: Optional[str] = None
    opportunity_profile: str
    constraint: str
    primary_leverage: str
    market_density: str
    review_position: str
    paid_status: str
    intervention_plan: List[InterventionPlanItem]
    brief: Optional[Dict[str, Any]] = None
    service_intelligence: Optional[ServiceIntelligence] = None
    revenue_breakdowns: List[RevenueBreakdown] = []
    conversion_infrastructure: Optional[ConversionInfrastructure] = None
    risk_flags: List[str] = []
    evidence: List[EvidenceItem] = []
    cohort_count: Optional[int] = None
    cohort_close_rate: Optional[float] = None
    top_constraints: List[Dict[str, Any]] = []
    top_outreach_angles: List[Dict[str, Any]] = []
    similar_leads_count: Optional[int] = None
    rag_used: Optional[bool] = None
    retrieval_time_ms: Optional[int] = None
    num_similar_docs: Optional[int] = None
    extraction_method: Optional[str] = None
    confidence: Optional[float] = None
    competitors: Optional[List[Dict[str, Any]]] = None
    local_avg_rating: Optional[float] = None
    local_avg_rating_points: Optional[int] = None
    lead_quality: Optional[LeadQualityPrediction] = None


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------

class JobSubmitResponse(BaseModel):
    job_id: str
    status: str = "pending"


class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    created_at: str
    completed_at: Optional[str] = None
    error: Optional[str] = None
    diagnostic_id: Optional[int] = None
    progress: Optional[Dict[str, Any]] = None


# ---------------------------------------------------------------------------
# Diagnostics (SaaS layer)
# ---------------------------------------------------------------------------

class DiagnosticListItem(BaseModel):
    id: int
    business_name: str
    city: str
    state: Optional[str] = None
    place_id: Optional[str] = None
    created_at: str
    opportunity_profile: Optional[str] = None
    constraint: Optional[str] = None
    modeled_revenue_upside: Optional[str] = None


class DiagnosticListResponse(BaseModel):
    items: List[DiagnosticListItem]
    total: int
    limit: int
    offset: int
