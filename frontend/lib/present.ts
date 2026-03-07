export function dedupeStrings(items: string[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const raw of items) {
    const item = String(raw || "").trim();
    if (!item) continue;
    const key = item.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }
  return out;
}

export function isUnknownLike(value: unknown): boolean {
  const s = String(value ?? "").trim().toLowerCase();
  if (!s) return true;
  return s === "unknown" || s === "—" || s.includes("not evaluated");
}

export function isSchemaRelated(text: string): boolean {
  const s = String(text || "").toLowerCase();
  return (
    s.includes("schema")
    || s.includes("localbusiness")
    || s.includes("faqpage")
    || s.includes("structured trust")
    || s.includes("structured_trust")
    || s.includes("schema_coverage")
  );
}

export function cleanWebsiteDisplay(url: string | null | undefined): string {
  const raw = String(url || "").trim();
  if (!raw) return "";
  try {
    const normalized = raw.startsWith("http://") || raw.startsWith("https://") ? raw : `https://${raw}`;
    const u = new URL(normalized);
    return `${u.hostname}${u.pathname === "/" ? "" : u.pathname}`;
  } catch {
    return raw.replace(/^https?:\/\//i, "").split("?")[0];
  }
}

export function categoryEvidence(items: string[]): {
  reputation: string[];
  capture: string[];
  market: string[];
  crawl: string[];
} {
  const out = { reputation: [] as string[], capture: [] as string[], market: [] as string[], crawl: [] as string[] };
  for (const item of dedupeStrings(items)) {
    const s = item.toLowerCase();
    if (isSchemaRelated(s)) continue;
    if (s.includes("review") || s.includes("rating") || s.includes("authority")) out.reputation.push(item);
    else if (s.includes("booking") || s.includes("contact") || s.includes("phone") || s.includes("capture") || s.includes("cta")) out.capture.push(item);
    else if (s.includes("market") || s.includes("competitor") || s.includes("density")) out.market.push(item);
    else out.crawl.push(item);
  }
  return out;
}
