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

export function getServiceCoverageGaps(data: DiagnosticResponse): {
  missing: string[];
  mentionOnly: string[];
  all: string[];
} {
  const rows = data.service_intelligence?.high_value_services;
  if (Array.isArray(rows) && rows.length > 0) {
    const normalized = rows
      .map((r) => {
        const row = r as Record<string, unknown>;
        const status = String(row.service_status || "").toLowerCase();
        return {
          name: String(row.display_name || row.service || ""),
          status,
          revenueWeight: Number(row.revenue_weight || 0),
        };
      })
      .filter((r) => Boolean(r.name) && (r.status === "missing" || r.status === "mention_only"))
      .sort((a, b) => {
        const rankA = a.status === "missing" ? 0 : 1;
        const rankB = b.status === "missing" ? 0 : 1;
        if (rankA !== rankB) return rankA - rankB;
        return b.revenueWeight - a.revenueWeight;
      });
    if (normalized.length) {
      const missing = normalized.filter((r) => r.status === "missing").map((r) => r.name);
      const mentionOnly = normalized.filter((r) => r.status === "mention_only").map((r) => r.name);
      return {
        missing,
        mentionOnly,
        all: [...missing, ...mentionOnly],
      };
    }
  }
  const fallback = (data.brief?.high_ticket_gaps?.missing_landing_pages || data.service_intelligence?.missing_services || []) as string[];
  return {
    missing: fallback,
    mentionOnly: [],
    all: fallback,
  };
}

export function getMissingServices(data: DiagnosticResponse): string[] {
  return getServiceCoverageGaps(data).missing;
}

export function getTopGapService(data: DiagnosticResponse): { name: string; status: string } | null {
  const rows = data.service_intelligence?.high_value_services;
  if (!Array.isArray(rows) || !rows.length) return null;
  const gaps = rows
    .filter((r) => {
      const st = String((r as Record<string, unknown>).service_status || "").toLowerCase();
      return st === "missing" || st === "mention_only";
    })
    .sort((a, b) => {
      const aStatus = String((a as Record<string, unknown>).service_status || "").toLowerCase();
      const bStatus = String((b as Record<string, unknown>).service_status || "").toLowerCase();
      const aRank = aStatus === "missing" ? 0 : 1;
      const bRank = bStatus === "missing" ? 0 : 1;
      if (aRank !== bRank) return aRank - bRank;
      return Number((b as Record<string, unknown>).revenue_weight || 0) - Number((a as Record<string, unknown>).revenue_weight || 0);
    });
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
