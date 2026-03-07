import type { DiagnosticResponse } from "./types";
import { parseNum, isPaidActive, getMissingServices, isServiceSuppressed, isConversionSuppressed } from "./signals";

export function generatePitchBullets(data: DiagnosticResponse): string[] {
  const bullets: string[] = [];

  const missing = getMissingServices(data);
  const suppressService = isServiceSuppressed(data);
  const suppressConv = isConversionSuppressed(data);

  const reviews = parseNum(data.brief?.market_position?.reviews);
  const local = parseNum(data.brief?.market_position?.local_avg);
  const marketDensity = String(data.brief?.market_position?.market_density || data.market_density || "").toLowerCase();

  const form = data.brief?.conversion_infrastructure?.contact_form ?? data.conversion_infrastructure?.contact_form;
  const booking = data.brief?.conversion_infrastructure?.online_booking ?? data.conversion_infrastructure?.online_booking;

  // Verified signal first: review deficit in competitive market
  if (bullets.length < 3 && reviews != null && local != null && reviews < local && marketDensity.includes("high")) {
    bullets.push(`In a high-density market, you're at ${reviews} reviews vs local average ${local}, which suppresses trust and conversion.`);
  }

  // Verified signal: conversion gaps
  if (bullets.length < 3 && !suppressConv && (form === false || booking === false)) {
    bullets.push("Lead capture is leaking — booking and form paths are incomplete on key pages.");
  }

  // Crawl-derived observation (hedged)
  if (!suppressService && isPaidActive(data) && missing.length > 0) {
    bullets.push(`You're investing in paid traffic, but our scan didn't find dedicated landing pages for ${missing.slice(0, 2).join(" or ")}.`);
  } else if (!suppressService && missing.length > 0 && bullets.length < 3) {
    bullets.push(`Our scan didn't find dedicated pages for ${missing.slice(0, 2).join(" or ")} — common high-value services in this market.`);
  }

  // Verified signal: competitor advantage
  const competitorReviews = parseNum(data.brief?.strategic_gap?.competitor_reviews ?? data.brief?.competitive_service_gap?.competitor_reviews);
  const competitorDistance = parseNum(data.brief?.strategic_gap?.distance_miles ?? data.brief?.competitive_service_gap?.distance_miles);
  if (bullets.length < 3 && competitorReviews != null && reviews != null && competitorDistance != null && competitorDistance <= 5 && competitorReviews - reviews >= 100) {
    bullets.push("A nearby competitor holds a visible review advantage, pulling high-intent local traffic away.");
  }

  if (!bullets.length) {
    bullets.push("This account is worth a light-touch review, but urgency is currently moderate.");
  }

  return bullets.slice(0, 3);
}
