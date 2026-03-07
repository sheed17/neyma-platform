import type { DiagnosticResponse } from "./types";

export function parseNum(v: unknown): number | null {
  if (v == null) return null;
  const m = String(v).match(/-?\d+(\.\d+)?/);
  if (!m) return null;
  const n = Number(m[0]);
  return Number.isFinite(n) ? n : null;
}

export function isPaidActive(data: DiagnosticResponse): boolean {
  const paid = String(data.brief?.demand_signals?.google_ads_line || data.paid_status || "").toLowerCase();
  return paid.includes("active") || paid.includes("running");
}

export function getMissingServices(data: DiagnosticResponse): string[] {
  const rows = data.service_intelligence?.high_value_services;
  if (Array.isArray(rows) && rows.length > 0) {
    const gaps = rows
      .filter((r) => {
        const status = String((r as Record<string, unknown>).service_status || "").toLowerCase();
        return status === "missing" || status === "mention_only";
      })
      .sort((a, b) => Number((b as Record<string, unknown>).revenue_weight || 0) - Number((a as Record<string, unknown>).revenue_weight || 0))
      .map((r) => String((r as Record<string, unknown>).display_name || (r as Record<string, unknown>).service || ""))
      .filter(Boolean);
    if (gaps.length) return gaps;
  }
  return (data.brief?.high_ticket_gaps?.missing_landing_pages || data.service_intelligence?.missing_services || []) as string[];
}

export function getTopGapService(data: DiagnosticResponse): { name: string; status: string } | null {
  const rows = data.service_intelligence?.high_value_services;
  if (!Array.isArray(rows) || !rows.length) return null;
  const gaps = rows
    .filter((r) => {
      const st = String((r as Record<string, unknown>).service_status || "").toLowerCase();
      return st === "missing" || st === "mention_only";
    })
    .sort((a, b) => Number((b as Record<string, unknown>).revenue_weight || 0) - Number((a as Record<string, unknown>).revenue_weight || 0));
  if (!gaps.length) return null;
  const top = gaps[0] as Record<string, unknown>;
  return {
    name: String(top.display_name || top.service || ""),
    status: String(top.service_status || "").toLowerCase(),
  };
}

export function isServiceSuppressed(data: DiagnosticResponse): boolean {
  return Boolean(data.service_intelligence?.suppress_service_gap);
}

export function isConversionSuppressed(data: DiagnosticResponse): boolean {
  return Boolean(data.service_intelligence?.suppress_conversion_absence_claims || data.service_intelligence?.suppress_service_gap);
}

export function isRevenueSuppressed(data: DiagnosticResponse): boolean {
  return Boolean(data.service_intelligence?.suppress_revenue_modeling || data.service_intelligence?.suppress_service_gap);
}

export function isMarketHigh(data: DiagnosticResponse): boolean {
  const m = String(data.brief?.market_position?.market_density || data.market_density || "").toLowerCase();
  return m.includes("high");
}

export function hasConversionGap(data: DiagnosticResponse): boolean {
  if (isConversionSuppressed(data)) return false;
  const c = data.brief?.conversion_infrastructure || data.conversion_infrastructure;
  if (!c) return false;
  return c.online_booking === false || c.contact_form === false;
}
