import type { DiagnosticResponse } from "./types";

export type ModeledUpsideDisplay = {
  mode: "range" | "indicative" | "suppressed";
  value: string;
  status: string;
  basis: string;
  context: string;
  serviceContext: string;
};

export function getModeledUpsideDisplay(data: DiagnosticResponse): ModeledUpsideDisplay {
  const brief = data.brief || {};
  const gap = brief.revenue_upside_capture_gap;
  const fallbackRange = String(
    brief.executive_diagnosis?.modeled_revenue_upside
    || (gap?.annual_low != null && gap?.annual_high != null
      ? `$${Number(gap.annual_low).toLocaleString()}–$${Number(gap.annual_high).toLocaleString()} annually`
      : ""),
  ).trim();
  const serviceContext = String(
    gap?.service_context
    || (gap?.primary_service ? `${gap.primary_service} gap` : "Primary gap"),
  ).trim();
  const basis = String(
    gap?.basis
    || gap?.method_note
    || brief.market_position?.revenue_band_method
    || brief.executive_footnote
    || "Modeled from public proxy signals.",
  ).trim();
  const context = String(
    gap?.context
    || gap?.suppressed_reason
    || brief.executive_footnote
    || "Directional estimate only.",
  ).trim();

  const normalizedMode = String(gap?.display_mode || "").toLowerCase();
  if (normalizedMode === "range" && (gap?.display_value || fallbackRange)) {
    return {
      mode: "range",
      value: String(gap?.display_value || fallbackRange),
      status: "Strong enough to show a dollar estimate.",
      basis,
      context: context || "Not a booked revenue forecast.",
      serviceContext,
    };
  }
  if (normalizedMode === "suppressed") {
    return {
      mode: "suppressed",
      value: "Estimate withheld",
      status: "Not enough evidence to show a dollar estimate.",
      basis,
      context: context || "Neyma is withholding the dollar estimate rather than overstating precision.",
      serviceContext,
    };
  }
  if (fallbackRange || normalizedMode === "indicative") {
    return {
      mode: "indicative",
      value: "Directional opportunity only",
      status: "Real upside signal, but no reliable dollar estimate yet.",
      basis,
      context: context || "Use the signal for prioritization, not as a revenue forecast.",
      serviceContext,
    };
  }
  return {
    mode: "suppressed",
    value: "Estimate withheld",
    status: "Not enough evidence to show a dollar estimate.",
    basis,
    context: context || "Neyma is withholding the dollar estimate rather than overstating precision.",
    serviceContext,
  };
}
