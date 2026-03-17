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

export function clientFacingBriefError(message: string | null | undefined, fallback = "We couldn't complete the brief right now. Please try again."): string {
  const raw = String(message || "").trim();
  if (!raw) return fallback;

  const normalized = raw.toLowerCase();
  if (normalized.includes("valid 2-letter us state code")) return raw;
  if (normalized.includes("timed out")) return "The brief took too long to finish. Please try again.";
  if (normalized.includes("no job id") || normalized.includes("did not start") || normalized.includes("no diagnostic id")) {
    return "We couldn't start the brief right now. Please try again.";
  }
  if (
    normalized.includes("playwright")
    || normalized.includes("crawl")
    || normalized.includes("crawler")
    || normalized.includes("pages_crawled")
    || normalized.includes("rendered")
  ) {
    return "We couldn't complete the page checks for this brief. Please try again.";
  }
  if (
    normalized.includes("diagnostic failed")
    || normalized.includes("brief build failed")
    || normalized.includes("failed to build brief")
  ) {
    return fallback;
  }

  return raw;
}

export function clientFacingAppError(
  message: string | null | undefined,
  fallback = "Something didn't come through on our side. Please try again."
): string {
  const raw = String(message || "").trim();
  if (!raw) return fallback;

  const normalized = raw.toLowerCase();

  if (normalized.includes("valid 2-letter us state code")) return raw;
  if (normalized.includes("enter a city")) return raw;
  if (normalized.includes("enter a business name")) return raw;
  if (normalized.includes("select a list")) return raw;
  if (normalized.includes("list name is required")) return "Enter a name for the list.";

  if (
    normalized.includes("unable to reach the api")
    || normalized.includes("networkerror")
    || normalized.includes("failed to fetch")
    || normalized.includes("load failed")
  ) {
    return "We couldn't reach Neyma right now. Please try again in a moment.";
  }

  if (
    normalized.includes("timed out")
    || normalized.includes("job timed out")
  ) {
    return "This is taking longer than expected. Please try again.";
  }

  if (
    normalized.includes("job did not start")
    || normalized.includes("did not start")
    || normalized.includes("no job id")
    || normalized.includes("no diagnostic id")
    || normalized.includes("prospect id missing")
    || normalized.includes("no diagnostic selected")
    || normalized.includes("no prospect selected")
  ) {
    return "We couldn't start that step right now. Please try again.";
  }

  if (
    normalized.includes("unauthorized")
    || normalized.includes("forbidden")
    || normalized.includes("auth required")
  ) {
    return "Sign in to continue with this action.";
  }

  if (
    normalized.includes("free plan")
    || normalized.includes("guest")
    || normalized.includes("limit reached")
  ) {
    return raw;
  }

  if (
    normalized.includes("openai")
    || normalized.includes("supabase")
    || normalized.includes("railway")
    || normalized.includes("playwright")
    || normalized.includes("crawl")
    || normalized.includes("crawler")
    || normalized.includes("rendered")
    || normalized.includes("traceback")
    || normalized.includes("exception")
  ) {
    return fallback;
  }

  if (
    normalized.includes("failed")
    || normalized.includes("error")
    || normalized.includes("could not")
  ) {
    return fallback;
  }

  return raw;
}

export function clientFacingAuditText(text: string | null | undefined): string {
  const raw = String(text || "").trim();
  if (!raw) return "";

  return raw
    .replace(/Not Evaluated\s*\(Low Crawl Confidence\)/gi, "Not evaluated in this review")
    .replace(/Low Crawl Confidence/gi, "limited page coverage")
    .replace(/crawl confidence/gi, "coverage confidence")
    .replace(/pages crawled/gi, "pages checked")
    .replace(/crawled/gi, "checked")
    .replace(/scanned pages/gi, "pages checked")
    .replace(/crawl metadata/gi, "verification details")
    .replace(/playwright/gi, "enhanced page checks")
    .replace(/crawling/gi, "checking pages")
    .replace(/crawl/gi, "page check");
}
