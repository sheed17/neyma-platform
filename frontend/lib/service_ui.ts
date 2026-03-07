export type CanonicalServiceStatus = "dedicated" | "mention_only" | "missing" | "unknown";

export function canonicalStatus(raw: string | undefined | null): CanonicalServiceStatus {
  const s = String(raw || "").toLowerCase().trim();
  if (s === "dedicated") return "dedicated";
  if (s === "mention_only") return "mention_only";
  if (s === "missing") return "missing";
  return "unknown";
}

export function statusBadge(status: CanonicalServiceStatus): {
  marker: string;
  label: string;
  color: string;
} {
  switch (status) {
    case "dedicated":
      return { marker: "🟢", label: "Dedicated Page", color: "green" };
    case "mention_only":
      return { marker: "🟡", label: "Mentioned on Site", color: "yellow" };
    case "missing":
      return { marker: "⚪", label: "Not Found in Scan", color: "gray" };
    case "unknown":
    default:
      return { marker: "⚪", label: "Not Scanned", color: "gray" };
  }
}

export function evidenceLabel(status: CanonicalServiceStatus, url?: string): string {
  switch (status) {
    case "dedicated":
      return url ? `Dedicated page found at ${url}` : "Dedicated page found";
    case "mention_only":
      return "Referenced on site — no standalone page found";
    case "missing":
      return "No dedicated page found in this scan";
    case "unknown":
      return "Could not be evaluated (limited scan access)";
  }
}

export function scanQualityLabel(crawlConfidence: string, pagesCrawled?: number): {
  label: string;
  note: string;
} {
  const c = String(crawlConfidence || "").toLowerCase();
  const n = pagesCrawled ?? 0;
  if (c === "high") {
    return {
      label: "Thorough Scan",
      note: n ? `${n} pages scanned` : "Comprehensive site access",
    };
  }
  if (c === "medium") {
    return {
      label: "Partial Scan",
      note: n
        ? `${n} pages scanned — some pages may not have been reachable`
        : "Some pages may not have been reachable",
    };
  }
  return {
    label: "Limited Scan",
    note: "Very limited access — results may be incomplete",
  };
}
