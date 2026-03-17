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
      return { marker: "⚪", label: "Not Found in Review", color: "gray" };
    case "unknown":
    default:
      return { marker: "⚪", label: "Not Evaluated", color: "gray" };
  }
}

export function evidenceLabel(status: CanonicalServiceStatus, url?: string): string {
  switch (status) {
    case "dedicated":
      return url ? `Dedicated page found at ${url}` : "Dedicated page found";
    case "mention_only":
      return "Referenced on site — no standalone page found";
    case "missing":
      return "No dedicated page found in the pages checked";
    case "unknown":
      return "Could not be evaluated with the page coverage available";
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
      label: "Thorough Review",
      note: n ? `${n} pages checked` : "Strong page coverage",
    };
  }
  if (c === "medium") {
    return {
      label: "Partial Review",
      note: n
        ? `${n} pages checked — some areas may not have been reached`
        : "Some areas may not have been reached",
    };
  }
  return {
    label: "Limited Review",
    note: "Limited page coverage — results may be incomplete",
  };
}
