export type DiagnosticRequest = {
  business_name: string;
  city: string;
  state: string;
  website?: string;
};

export type InterventionPlanItem = {
  step: number;
  category: string;
  action: string;
};

export interface ServiceIntelligence {
  detected_services: string[];
  missing_services: string[];
  schema_detected?: boolean;
  high_value_services?: Array<Record<string, unknown>>;
  high_value_summary?: Record<string, unknown>;
  high_value_service_leverage?: "high" | "moderate" | "low" | string;
  service_page_analysis_v2?: Record<string, unknown>;
}

export interface RevenueBreakdown {
  service: string;
  consults_per_month: string;
  revenue_per_case: string;
  annual_revenue_range: string;
}

export interface ConversionInfrastructure {
  online_booking?: boolean;
  contact_form?: boolean;
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
  method_note?: string;
}

export interface BriefConversionInfrastructure {
  online_booking?: boolean;
  contact_form?: boolean;
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
  rating?: number | null;
  user_ratings_total?: number | null;
  full_brief_ready?: boolean;
  tier1_signals?: {
    has_website?: boolean;
    ssl?: boolean;
    has_contact_form?: boolean;
    has_phone?: boolean;
    has_viewport?: boolean;
    has_schema?: boolean;
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
  accuracy_mode?: "fast" | "verified";
  requires_deep?: boolean;
  requires_lightweight?: boolean;
};

export type AskStartResponse = {
  job_id: string | null;
  status: string; // pending | requires_confirmation
  intent: AskIntent;
  message: string;
  accuracy_mode?: "fast" | "verified";
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
