import type { DiagnosticResponse } from "./types";
import { parseNum, isPaidActive, getServiceCoverageGaps, isServiceSuppressed, isConversionSuppressed } from "./signals";

function bookingFlowLabel(flow: string | null | undefined): string {
  const normalized = String(flow || "").trim().toLowerCase();
  if (normalized === "online_self_scheduling") return "online self-scheduling";
  if (normalized === "appointment_request_form") return "appointment request form";
  if (normalized === "call_only") return "phone-only scheduling";
  return "";
}

function serviceList(items: string[]): string {
  const filtered = items.map((item) => String(item || "").trim()).filter(Boolean);
  if (!filtered.length) return "high-value services";
  if (filtered.length === 1) return filtered[0];
  if (filtered.length === 2) return `${filtered[0]} and ${filtered[1]}`;
  return `${filtered[0]}, ${filtered[1]}, and more`;
}

function normalizeFocusTopic(value: string | null | undefined): string {
  const raw = String(value || "").trim();
  if (!raw) return "";
  return raw
    .replace(/\s+(capture|service|primary)\s+gap$/i, "")
    .replace(/\s+gap$/i, "")
    .trim();
}

function articleFor(value: string): string {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return "an";
  return /^[aeiou]/.test(raw) ? "an" : "a";
}

function hasWebsite(data: DiagnosticResponse): boolean {
  return Boolean(String(data.website || "").trim());
}

export function generateOpportunityHeading(signal: string | null | undefined): string {
  const normalized = String(signal || "").trim();
  if (!normalized) return "Why Neyma says this is an opportunity";
  return `Why Neyma says this is ${articleFor(normalized)} ${normalized.toLowerCase()} opportunity`;
}

export function generateOpportunityFocus(data: DiagnosticResponse, preferredFocus?: string | null): string {
  if (!hasWebsite(data)) return "No website presence";

  const serviceGaps = getServiceCoverageGaps(data);
  const missing = serviceGaps.missing;
  const mentionOnly = serviceGaps.mentionOnly;
  const suppressService = isServiceSuppressed(data);
  const suppressConv = isConversionSuppressed(data);

  const reviews = parseNum(data.brief?.market_position?.reviews);
  const local = parseNum(data.brief?.market_position?.local_avg);
  const marketDensity = String(data.brief?.market_position?.market_density || data.market_density || "").toLowerCase();

  const conversion = data.brief?.conversion_infrastructure || data.conversion_infrastructure;
  const bookingFlowType = conversion?.booking_flow_type || conversion?.capture_verification?.booking_flow?.value;
  const schedulingCtaDetected = conversion?.scheduling_cta_detected === true;

  const competitorReviews = parseNum(data.brief?.strategic_gap?.competitor_reviews ?? data.brief?.competitive_service_gap?.competitor_reviews);
  const competitorDistance = parseNum(data.brief?.strategic_gap?.distance_miles ?? data.brief?.competitive_service_gap?.distance_miles);
  const normalizedPreferred = String(preferredFocus || "").trim();

  if (normalizedPreferred) return normalizedPreferred;
  if (!suppressService && missing.length > 0) {
    return `${serviceList(missing.slice(0, 2))} gap`;
  }
  if (!suppressService && mentionOnly.length > 0) {
    return `${serviceList(mentionOnly.slice(0, 2))} depth gap`;
  }
  if (reviews != null && local != null && reviews < local) {
    return "Review authority gap";
  }
  if (competitorReviews != null && reviews != null && competitorDistance != null && competitorDistance <= 5 && competitorReviews - reviews >= 100) {
    return "Competitive pressure";
  }
  if (!suppressConv && bookingFlowType === "appointment_request_form") {
    return "Request-only booking path";
  }
  if (!suppressConv && schedulingCtaDetected) {
    return "Unverified self-scheduling";
  }
  if (marketDensity.includes("high")) return "Dense market";
  return "Growth gap";
}

export function generateWhyNow(data: DiagnosticResponse, focusHint?: string | null): string {
  const serviceGaps = getServiceCoverageGaps(data);
  const missing = serviceGaps.missing;
  const mentionOnly = serviceGaps.mentionOnly;
  const suppressService = isServiceSuppressed(data);
  const suppressConv = isConversionSuppressed(data);

  const reviews = parseNum(data.brief?.market_position?.reviews);
  const local = parseNum(data.brief?.market_position?.local_avg);
  const marketDensity = String(data.brief?.market_position?.market_density || data.market_density || "").toLowerCase();

  const conversion = data.brief?.conversion_infrastructure || data.conversion_infrastructure;
  const bookingFlowType = conversion?.booking_flow_type || conversion?.capture_verification?.booking_flow?.value;
  const schedulingCtaDetected = conversion?.scheduling_cta_detected === true;

  const paidActive = isPaidActive(data);
  const competitorReviews = parseNum(data.brief?.strategic_gap?.competitor_reviews ?? data.brief?.competitive_service_gap?.competitor_reviews);
  const competitorDistance = parseNum(data.brief?.strategic_gap?.distance_miles ?? data.brief?.competitive_service_gap?.distance_miles);
  const focusTopic = normalizeFocusTopic(focusHint);

  if (!hasWebsite(data)) {
    if (paidActive) {
      return "Paid demand is active, but no website was available to capture or convert it.";
    }
    if (reviews != null && local != null && reviews < local) {
      return "No website was available, and review authority still trails the local market baseline.";
    }
    return "No website was available, so service depth and conversion paths could not be verified.";
  }

  if (!suppressService && paidActive && missing.length > 0) {
    return `Paid traffic is active while service depth is still limited around ${focusTopic || serviceList(missing.slice(0, 2))}.`;
  }
  if (reviews != null && local != null && reviews < local && marketDensity.includes("high")) {
    return "Review authority is well below the local market in a dense market.";
  }
  if (reviews != null && local != null && reviews < local) {
    return "Review authority is still below the local market baseline.";
  }
  if (competitorReviews != null && reviews != null && competitorDistance != null && competitorDistance <= 5 && competitorReviews - reviews >= 100) {
    return `A nearby competitor has a materially stronger review position within ${competitorDistance} miles.`;
  }
  if (!suppressService && missing.length > 0) {
    return `High-value service depth is still limited around ${focusTopic || serviceList(missing.slice(0, 2))}.`;
  }
  if (!suppressService && mentionOnly.length > 0) {
    return `High-value service coverage exists, but depth is still thin around ${focusTopic || serviceList(mentionOnly.slice(0, 2))}.`;
  }
  if (!suppressConv && paidActive && bookingFlowType === "appointment_request_form") {
    return "Paid traffic is active while booking remains request-only.";
  }
  if (!suppressConv && paidActive && schedulingCtaDetected) {
    return "Paid traffic is active while self-scheduling was not verified.";
  }
  return "Evidence points to a real growth gap worth reviewing.";
}

export function generateObservationBullets(data: DiagnosticResponse, focusHint?: string | null): string[] {
  const bullets: string[] = [];

  const serviceGaps = getServiceCoverageGaps(data);
  const missing = serviceGaps.missing;
  const mentionOnly = serviceGaps.mentionOnly;
  const suppressService = isServiceSuppressed(data);
  const suppressConv = isConversionSuppressed(data);

  const reviews = parseNum(data.brief?.market_position?.reviews);
  const local = parseNum(data.brief?.market_position?.local_avg);
  const marketDensity = String(data.brief?.market_position?.market_density || data.market_density || "").toLowerCase();

  const conversion = data.brief?.conversion_infrastructure || data.conversion_infrastructure;
  const form = conversion?.contact_form;
  const booking = conversion?.online_booking;
  const bookingFlowType = conversion?.booking_flow_type || conversion?.capture_verification?.booking_flow?.value;
  const schedulingCtaDetected = conversion?.scheduling_cta_detected === true;
  const paidActive = isPaidActive(data);
  const focusTopic = normalizeFocusTopic(focusHint);

  if (!hasWebsite(data)) {
    bullets.push("No website was detected, which is itself a conversion and trust gap.");

    if (paidActive && bullets.length < 3) {
      bullets.push("Paid ads activity is present, but there is no owned site available to capture that demand.");
    }

    if (bullets.length < 3 && reviews != null && local != null && reviews < local) {
      bullets.push(`Off-site authority is also below the market baseline (${reviews} reviews versus a local average of ${local}).`);
    } else if (bullets.length < 3 && marketDensity.includes("high")) {
      bullets.push("This market is dense, so missing digital presence is more costly.");
    }

    if (bullets.length < 3) {
      bullets.push("Service depth, booking flow, and contact capture could not be verified without a website.");
    }

    return bullets.slice(0, 3);
  }

  if (bullets.length < 3 && reviews != null && local != null && reviews < local) {
    const gapText = `${reviews} reviews versus a local average of ${local}`;
    if (marketDensity.includes("high")) {
      bullets.push(`Review authority is well below the market (${gapText}), which matters more in a dense market.`);
    } else {
      bullets.push(`Review authority is still below the market baseline (${gapText}).`);
    }
  } else if (bullets.length < 3 && marketDensity.includes("high")) {
    bullets.push("This is a dense market, so visibility and conversion gaps are more expensive.");
  }

  const competitorReviews = parseNum(data.brief?.strategic_gap?.competitor_reviews ?? data.brief?.competitive_service_gap?.competitor_reviews);
  const competitorDistance = parseNum(data.brief?.strategic_gap?.distance_miles ?? data.brief?.competitive_service_gap?.distance_miles);
  if (bullets.length < 3 && competitorReviews != null && reviews != null && competitorDistance != null && competitorDistance <= 5 && competitorReviews - reviews >= 100) {
    bullets.push(`A nearby competitor has a materially stronger review position within ${competitorDistance} miles.`);
  }

  if (bullets.length < 3 && !suppressService && paidActive && missing.length > 0) {
    bullets.push(`Paid traffic is active while service depth is still limited around ${focusTopic || serviceList(missing.slice(0, 2))}.`);
  } else if (bullets.length < 3 && !suppressService && missing.length > 0) {
    bullets.push(`High-value service depth is still limited around ${focusTopic || serviceList(missing.slice(0, 2))}.`);
  } else if (bullets.length < 3 && !suppressService && mentionOnly.length > 0) {
    bullets.push(`High-value service coverage exists, but depth is still thin around ${focusTopic || serviceList(mentionOnly.slice(0, 2))}.`);
  }

  if (bullets.length < 3 && !suppressConv && paidActive) {
    const bookingFlow = bookingFlowLabel(bookingFlowType);
    if (bookingFlowType === "appointment_request_form") {
      bullets.push("Paid traffic is active while booking remains request-only.");
    } else if (!bookingFlow && schedulingCtaDetected) {
      bullets.push("Paid traffic is active while self-scheduling was not verified.");
    } else if (!bookingFlow && booking === false && form !== true) {
      bullets.push("Paid traffic is active while verified capture paths remain limited.");
    }
  }

  if (!bullets.length) {
    bullets.push("No major leverage signals were generated from this scan.");
  }

  return bullets.slice(0, 3);
}

export const generatePitchBullets = generateObservationBullets;
