import type { DiagnosticResponse } from "./types";
import { parseNum, isPaidActive, getMissingServices, getTopGapService, isServiceSuppressed, isRevenueSuppressed, isMarketHigh, hasConversionGap } from "./signals";

export type Verdict = "GO" | "SOFT_PASS" | "PASS";

function maxMoneyFromText(v: unknown): number {
  const text = String(v || "");
  const nums = text
    .replace(/[$,]/g, "")
    .split(/[^0-9.]+/)
    .map((x) => Number(x))
    .filter((x) => Number.isFinite(x) && x > 0);
  if (!nums.length) return 0;
  return Math.max(...nums);
}

function reviewStrength(data: DiagnosticResponse): boolean {
  const reviews = parseNum(data.brief?.market_position?.reviews);
  const local = parseNum(data.brief?.market_position?.local_avg);
  if (reviews == null || local == null || local <= 0) return false;
  return reviews >= 0.6 * local;
}

function reviewDeficit(data: DiagnosticResponse): boolean {
  const reviews = parseNum(data.brief?.market_position?.reviews);
  const local = parseNum(data.brief?.market_position?.local_avg);
  if (reviews == null || local == null || local <= 0) return false;
  return reviews < 0.6 * local;
}

function opportunityHigh(data: DiagnosticResponse): boolean {
  const fromBrief = maxMoneyFromText(data.brief?.executive_diagnosis?.modeled_revenue_upside);
  const fromProfile = maxMoneyFromText(data.opportunity_profile);
  const fromRange = Math.max(
    maxMoneyFromText(data.brief?.revenue_upside_capture_gap?.annual_high),
    maxMoneyFromText(data.brief?.revenue_upside_capture_gap?.annual_low),
  );
  const top = Math.max(fromBrief, fromProfile, fromRange);
  return top >= 300000;
}

export function computeVerdict(data: DiagnosticResponse): {
  verdict: Verdict;
  label: string;
  reasons: string[];
  topGap: string;
} {
  const crawl = String(data.service_intelligence?.crawl_confidence || "").toLowerCase();
  const highConfCrawl = crawl === "high";
  const suppressService = isServiceSuppressed(data);
  const suppressRevenue = isRevenueSuppressed(data);

  const paid = isPaidActive(data);
  const miss = getMissingServices(data).length;
  const oppHigh = !suppressRevenue && opportunityHigh(data);
  const revStrong = reviewStrength(data);
  const revDeficit = reviewDeficit(data);
  const highDensity = isMarketHigh(data);
  const convGap = hasConversionGap(data);

  // Crawl-derived gap signals only count toward GO when crawl confidence is high
  const verifiedGap = convGap;
  const crawlGap = !suppressService && miss > 0;
  const clearGap = verifiedGap || (crawlGap && highConfCrawl);

  const goSignals = [
    paid && crawlGap && highConfCrawl,
    oppHigh,
    revStrong,
    highDensity && clearGap,
  ].filter(Boolean).length;

  let verdict: Verdict = "PASS";
  if (goSignals >= 2) verdict = "GO";
  else if ((revDeficit && highDensity) || (paid && !oppHigh) || clearGap || (crawlGap && !highConfCrawl)) verdict = "SOFT_PASS";

  if ((crawl === "low" || suppressService) && verdict === "GO" && !(paid && revDeficit)) {
    verdict = "SOFT_PASS";
  }

  // Reasons: prefer verified signals first
  const reasons: string[] = [];
  if (revDeficit) reasons.push("Review authority below local average");
  if (highDensity) reasons.push("Competitive high-density market");
  if (convGap) reasons.push("Conversion capture gaps detected on site");
  if (oppHigh && !suppressRevenue) reasons.push("High modeled upside range");
  if (paid && crawlGap && !suppressService) reasons.push("Paid traffic with limited service page coverage");
  if (!reasons.length) reasons.push("No urgent monetizable gap detected");

  // Top Gap: prefer verified signals; crawl-derived gets observational language
  let topGap: string;
  if (revDeficit) {
    topGap = "Review authority below local average";
  } else if (convGap) {
    topGap = "Conversion capture gaps on site";
  } else if (highDensity && !revStrong) {
    topGap = "Competitive market, limited differentiation";
  } else {
    const topGapSvc = !suppressService ? getTopGapService(data) : null;
    if (topGapSvc) {
      topGap = topGapSvc.status === "mention_only"
        ? `${topGapSvc.name} — mentioned, no dedicated page found`
        : `${topGapSvc.name} — no dedicated page found`;
    } else if (suppressService && paid) {
      topGap = "Paid demand active, limited scan data";
    } else if (suppressService) {
      topGap = "Limited scan data";
    } else {
      topGap = "Review & visibility positioning";
    }
  }

  return {
    verdict,
    label: verdict === "GO" ? "STRONG LEAD" : verdict === "SOFT_PASS" ? "SOFT PASS" : "PASS",
    reasons: reasons.slice(0, 3).map((r) => r.split(" ").slice(0, 10).join(" ")),
    topGap,
  };
}
