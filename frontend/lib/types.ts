export type DiagnosticRequest = {
  business_name: string;
  city: string;
  state: string;
  website?: string;
  deep_audit?: boolean;
  source_diagnostic_id?: number;
};

export type InterventionPlanItem = {
  step: number;
  category: string;
  action: string;
};

export interface ServiceIntelligence {
  detected_services: string[];
  missing_services: string[];
  crawl_confidence?: "low" | "medium" | "high" | string;
  pages_crawled?: number;
  js_detected?: boolean;
  crawl_method?: string;
  deep_scan?: boolean;
  service_page_count?: number;
  playwright_fetch_summary?: {
    playwright_pages?: number;
    requests_pages?: number;
    playwright_fallback_to_requests?: number;
  };
  crawl_warning?: string;
  suppress_service_gap?: boolean;
  suppress_conversion_absence_claims?: boolean;
  suppress_revenue_modeling?: boolean;
  schema_detected?: boolean;
  high_value_services?: Array<Record<string, unknown>>;
  high_value_summary?: Record<string, unknown>;
  high_value_service_leverage?: "high" | "moderate" | "low" | string;
  service_page_analysis_v2?: Record<string, unknown>;
  cta_elements?: Array<{ type: "Book" | "Schedule" | "Contact" | "Call" | string; count: number; pages: string[]; clickable_count?: number }>;
  cta_clickable_by_type?: Record<string, number>;
  cta_clickable_count?: number;
  geo_intent_pages?: Array<{
    url: string;
    title: string;
    signals: Array<"city" | "near-me" | "schema" | "meta" | string>;
    hasCTA: boolean;
  }>;
  missing_geo_pages?: Array<{
    slug: string;
    title: string;
    priority: "high" | "medium" | "low" | string;
    reason: string;
  }>;
  signal_verification?: {
    services?: Array<{
      service?: string;
      display_name?: string;
      deterministic_verdict?: "missing" | "present" | "not_evaluated" | string;
      deterministic_confidence?: "low" | "medium" | "high" | string;
      final_verdict?: "missing" | "present" | "not_evaluated" | string;
      final_confidence?: "low" | "medium" | "high" | string;
      reason?: string;
      url?: string | null;
      ai_validation?: {
        enabled?: boolean;
        verdict?: "likely_missing" | "likely_present" | "unclear" | string | null;
        confidence?: "low" | "medium" | "high" | string | null;
        reason?: string | null;
        model?: string | null;
      };
    }>;
    summary?: {
      total?: number;
      missing?: number;
      present?: number;
      not_evaluated?: number;
      ai_checked?: number;
      disagreements?: number;
      crawl_confidence?: string | null;
    };
  };
}

export interface CompetitorEntry {
  name: string;
  reviews: number;
  rating: number | null;
  distance: string;
  placeId: string;
  note: string;
  isYou: boolean;
}

export interface RevenueBreakdown {
  service: string;
  consults_per_month: string;
  revenue_per_case: string;
  annual_revenue_range: string;
}

export interface CaptureVerificationSignal {
  status?: string;
  value?: string;
  confidence?: string;
  observed_pages?: string[];
  evidence?: string[];
}

export interface CaptureVerificationMethod {
  page: string;
  method: string;
  source: string;
}

export interface CaptureVerification {
  homepage_page?: string;
  followup_pages_checked?: string[];
  verification_methods?: CaptureVerificationMethod[];
  scheduling_cta?: CaptureVerificationSignal;
  booking_flow?: CaptureVerificationSignal;
  contact_form?: CaptureVerificationSignal;
}

export interface ConversionInfrastructure {
  online_booking?: boolean;
  contact_form?: boolean;
  booking_flow_type?: string | null;
  booking_flow_confidence?: string | null;
  scheduling_cta_detected?: boolean | null;
  contact_form_confidence?: string | null;
  contact_form_cta_detected?: boolean | null;
  capture_verification?: CaptureVerification | null;
  phone_prominent?: boolean;
  mobile_optimized?: boolean;
  page_load_ms?: number;
}

export interface EvidenceItem {
  label: string;
  value: string;
}

export interface BriefExecutiveDiagnosis {
  constraint?: string;
  primary_leverage?: string;
  opportunity_profile?: {
    label?: string;
    why?: string;
    leverage_drivers?: {
      missing_high_value_pages?: boolean;
      market_density_high?: boolean;
      structured_trust_weak?: boolean;
      paid_active?: boolean;
      review_deficit?: boolean;
    };
  } | string;
  modeled_revenue_upside?: string;
}

export interface BriefMarketPosition {
  revenue_band?: string;
  revenue_band_method?: string;
  reviews?: string;
  local_avg?: string;
  market_density?: string;
}

export interface BriefCompetitiveContext {
  line1?: string;
  line2?: string;
  line3?: string;
  line3_items?: string[];
}

export interface BriefDemandSignals {
  google_ads_line?: string;
  google_ads_source?: string;
  meta_ads_line?: string;
  meta_ads_source?: string;
  paid_channels_detected?: string[];
  paid_spend_estimate?: string;
  paid_spend_method?: string;
  organic_visibility_tier?: string;
  organic_visibility_reason?: string;
  last_review_days_ago?: number;
  last_review_estimated?: boolean;
  review_velocity_30d?: number;
  review_velocity_estimated?: boolean;
}

export interface BriefCompetitiveServiceGap {
  type?: string;
  service?: string;
  competitor_name?: string;
  competitor_reviews?: number;
  lead_reviews?: number;
  distance_miles?: number;
  schema_missing?: boolean;
}

export interface BriefStrategicGap {
  service?: string;
  competitor_name?: string;
  competitor_reviews?: number;
  distance_miles?: number;
  market_density?: string;
}

export interface BriefHighTicketGaps {
  high_ticket_services_detected?: string[];
  missing_landing_pages?: string[];
  schema?: string;
  service_level_upside?: Array<{ service?: string; upside?: string }>;
}

export interface BriefRevenueUpsideCaptureGap {
  primary_service?: string;
  gap_service?: string;
  consult_low?: number;
  consult_high?: number;
  case_low?: number;
  case_high?: number;
  annual_low?: number;
  annual_high?: number;
  source?: string;
  method_note?: string;
  display_mode?: "range" | "indicative" | "suppressed" | string;
  display_value?: string | null;
  confidence_score?: number | null;
  confidence_label?: string | null;
  reliability_grade?: string | null;
  basis?: string | null;
  context?: string | null;
  suppressed_reason?: string | null;
  service_context?: string | null;
}

export interface BriefConversionInfrastructure {
  online_booking?: boolean;
  contact_form?: boolean;
  booking_flow_type?: string | null;
  booking_flow_confidence?: string | null;
  scheduling_cta_detected?: boolean | null;
  contact_form_confidence?: string | null;
  contact_form_cta_detected?: boolean | null;
  capture_verification?: CaptureVerification | null;
  phone_prominent?: boolean;
  mobile_optimized?: boolean;
  page_load_ms?: number;
}

export interface BriefCompetitiveDelta {
  target_service_page_count?: number;
  target_pages_with_faq_schema?: number;
  target_avg_word_count_service_pages?: number;
  target_min_word_count_service_pages?: number;
  target_max_word_count_service_pages?: number;
  competitor_avg_service_pages?: number | null;
  competitor_avg_pages_with_schema?: number | null;
  competitor_avg_word_count?: number | null;
  competitors_sampled?: number;
  competitor_site_metrics_count?: number;
  competitor_crawl_note?: string | null;
}

export interface BriefSerpPresence {
  domain?: string;
  as_of_date?: string;
  keywords?: Array<{ keyword?: string; position?: number | null; in_top_10?: boolean; page_type?: string | null }>;
}

export interface BriefReviewIntelligence {
  review_sample_size?: number;
  summary?: string;
  service_mentions?: Record<string, number>;
  complaint_themes?: Record<string, number>;
}

export interface BriefConversionStructure {
  phone_clickable?: boolean | null;
  cta_count?: number;
  form_single_or_multi_step?: string;
}

export interface BriefMarketSaturation {
  top_5_avg_reviews?: number;
  competitor_median_reviews?: number;
  target_gap_from_median?: string | number;
}

export interface BriefGeoCoverage {
  city_or_near_me_page_count?: number;
  has_multi_location_page?: boolean;
  geo_page_examples?: string[];
}

export interface BriefAuthorityProxy {
  page_count?: number;
  blog_page_count?: number;
  domain_age_years?: number | null;
  serp_keyword_appearances?: number;
  authority_proxy_score?: number;
  methodology?: string;
}

/** Brief view model — full structure from build_revenue_brief_view_model */
export type Brief = {
  executive_diagnosis?: BriefExecutiveDiagnosis;
  executive_footnote?: string;
  market_position?: BriefMarketPosition;
  competitive_context?: BriefCompetitiveContext;
  competitive_service_gap?: BriefCompetitiveServiceGap | null;
  strategic_gap?: BriefStrategicGap | null;
  demand_signals?: BriefDemandSignals;
  high_ticket_gaps?: BriefHighTicketGaps;
  service_page_analysis?: {
    services?: Array<Record<string, unknown>>;
    summary?: Record<string, unknown>;
    leverage?: string;
    v2?: Record<string, unknown>;
  } | null;
  revenue_upside_capture_gap?: BriefRevenueUpsideCaptureGap | null;
  conversion_infrastructure?: BriefConversionInfrastructure;
  competitive_delta?: BriefCompetitiveDelta | null;
  serp_presence?: BriefSerpPresence | null;
  review_intelligence?: BriefReviewIntelligence | null;
  conversion_structure?: BriefConversionStructure | null;
  market_saturation?: BriefMarketSaturation | null;
  geo_coverage?: BriefGeoCoverage | null;
  authority_proxy?: BriefAuthorityProxy | null;
  risk_flags?: string[];
  intervention_plan?: string[];
  intervention_fallback?: { strategic_frame?: string; tactical_levers?: string };
  evidence_bullets?: string[];
  [key: string]: unknown;
};

export type DiagnosticResponse = {
  lead_id: number;
  business_name: string;
  city: string;
  state?: string | null;
  phone?: string | null;
  website?: string | null;
  opportunity_profile: string;
  constraint: string;
  primary_leverage: string;
  market_density: string;
  review_position: string;
  paid_status: string;
  intervention_plan: InterventionPlanItem[];
  brief?: Brief | null;
  service_intelligence?: ServiceIntelligence;
  revenue_breakdowns?: RevenueBreakdown[];
  conversion_infrastructure?: ConversionInfrastructure;
  risk_flags?: string[];
  evidence?: EvidenceItem[];
  competitors?: CompetitorEntry[];
  local_avg_rating?: number | null;
  local_avg_rating_points?: number | null;
};

// ---------------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------------

export type JobSubmitResponse = {
  job_id: string;
  status: string;
};

export type JobStatusResponse = {
  job_id: string;
  status: string;
  created_at: string;
  completed_at?: string | null;
  error?: string | null;
  diagnostic_id?: number | null;
  progress?: Record<string, unknown> | null;
};

// ---------------------------------------------------------------------------
// Diagnostics (SaaS layer)
// ---------------------------------------------------------------------------

export type DiagnosticListItem = {
  id: number;
  business_name: string;
  city: string;
  state?: string | null;
  place_id?: string | null;
  created_at: string;
  rating?: number | null;
  local_avg_rating?: number | null;
  opportunity_profile?: string | null;
  constraint?: string | null;
  modeled_revenue_upside?: string | null;
};

export type DiagnosticListResponse = {
  items: DiagnosticListItem[];
  total: number;
  limit: number;
  offset: number;
};

export type OutcomeStatus = {
  status?: "contacted" | "closed_won" | "closed_lost";
  note?: string | null;
  updated_at?: string;
};

export type TerritoryScanRequest = {
  city: string;
  state?: string;
  vertical: string;
  limit?: number;
  filters?: {
    has_implant_gap?: boolean;
    below_review_avg?: boolean;
  };
};

export type TerritoryScanCreateResponse = {
  scan_id: string;
  status: string;
  message: string;
};

export type TerritoryScanStatusResponse = {
  scan_id: string;
  city?: string | null;
  state?: string | null;
  vertical?: string | null;
  status: string;
  created_at?: string | null;
  completed_at?: string | null;
  summary?: Record<string, unknown>;
  error?: string | null;
};

export type ProspectRow = {
  prospect_id?: number;
  diagnostic_id: number | null;
  place_id?: string | null;
  rank?: number;
  rank_score?: number;
  business_name: string;
  city?: string | null;
  state?: string | null;
  revenue_band?: string | null;
  modeled_revenue_upside?: string | null;
  primary_leverage?: string | null;
  constraint?: string | null;
  opportunity_profile?: string | null;
  review_position_summary?: string | null;
  brief_url?: string;
  website?: string | null;
  phone?: string | null;
  email?: string | null;
  key_signal?: string | null;
  ai_explanation?: string | null;
  match_evidence_level?: "deep_verified" | "lightweight_verified" | "deterministic" | "inferred" | null;
  match_evidence?: Array<{
    criterion_key?: string;
    criterion_type?: string;
    service?: string | null;
    source?: string;
    matched?: boolean;
    details?: Record<string, unknown> | null;
  }> | null;
  ai_rerank?: Record<string, unknown> | null;
  rating?: number | null;
  user_ratings_total?: number | null;
  full_brief_ready?: boolean;
  tier1_signals?: {
    has_website?: boolean;
    ssl?: boolean | null;
    has_contact_form?: boolean | null;
    has_phone?: boolean;
    has_viewport?: boolean | null;
    has_schema?: boolean | null;
    has_booking?: boolean | null;
    booking_conversion_path?: string | null;
    capture_verification?: CaptureVerification | null;
  };
  change?: {
    changed: boolean;
    deltas: Array<{ field: string; before?: string | null; after?: string | null }>;
  } | null;
  outcome_status?: OutcomeStatus;
  added_at?: string;
};

export type TerritoryScanResultsResponse = TerritoryScanStatusResponse & {
  prospects: ProspectRow[];
};

export type ProspectList = {
  id: number;
  name: string;
  created_at?: string;
  members_count?: number;
};

export type ProspectListsResponse = {
  items: ProspectList[];
};

export type ProspectListMembersResponse = {
  list: { id: number; name: string };
  members: ProspectRow[];
};

export type TerritoryScanListItem = {
  id: string;
  city?: string | null;
  state?: string | null;
  vertical?: string | null;
  limit_count?: number;
  status: string;
  created_at: string;
  completed_at?: string | null;
  prospects_count?: number;
  summary?: Record<string, unknown>;
};

export type TerritoryScansResponse = {
  items: TerritoryScanListItem[];
};

export type DeepBriefStartResponse = {
  job_id: string;
  status: string;
  message: string;
};

export type AskIntent = {
  query: string;
  query_raw?: string;
  city: string | null;
  state?: string | null;
  vertical: string | null;
  limit: number;
  criteria: Array<Record<string, unknown>>;
  must_not?: Array<Record<string, unknown>>;
  unsupported_parts?: string[];
  missing_required?: string[];
  intent_confidence?: "high" | "medium" | "low";
  requires_deep?: boolean;
  requires_lightweight?: boolean;
};

export type AskStartResponse = {
  job_id: string | null;
  status?: string;
  intent?: AskIntent;
  normalized_intent?: AskIntent;
  message?: string;
  confidence?: "high" | "medium" | "low";
  question?: string;
  unsupported_parts?: string[];
  requires_confirmation?: boolean;
};

export type AskResultsResponse = {
  job_id: string;
  status: string;
  result: Record<string, unknown> | null;
};

export type AskEnsureBriefResponse = {
  status: "ready" | "building";
  diagnostic_id?: number;
  job_id?: string;
};

export type DiagnosticShareResponse = {
  token: string;
  share_url: string;
  expires_at?: string | null;
};

export type OutcomesSummaryResponse = {
  contacted: number;
  closed_won: number;
  closed_lost: number;
  not_contacted: number;
  closed_this_month?: number;
};

export type OutcomesListItem = {
  diagnostic_id: number;
  business_name: string;
  city: string;
  state?: string | null;
  status?: "contacted" | "closed_won" | "closed_lost";
  note?: string | null;
  updated_at?: string | null;
};

export type PlanTier = "guest" | "free" | "pro" | "team";

export type AccessWorkspace = {
  id: number;
  name?: string | null;
  plan_tier?: PlanTier | string | null;
  seat_limit?: number | null;
  seat_count?: number | null;
  role?: "owner" | "admin" | "member" | string | null;
  status?: "active" | "invited" | "removed" | string | null;
};

export type AccessState = {
  viewer: {
    user_id: number;
    email?: string | null;
    name?: string | null;
    is_guest: boolean;
  };
  plan_tier: PlanTier | string;
  workspace?: AccessWorkspace | null;
  usage: {
    territory_scan: number;
    diagnostic: number;
    ask: number;
  };
  limits: {
    territory_scan: number | null;
    diagnostic: number | null;
    ask: number | null;
  };
  remaining: {
    territory_scan: number | null;
    diagnostic: number | null;
    ask: number | null;
  };
  period_key?: string;
  can_use: {
    territory_scan: boolean;
    diagnostic: boolean;
    ask: boolean;
    workspace: boolean;
    save: boolean;
    share: boolean;
    export: boolean;
  };
  recommended_cta?: string | null;
};
