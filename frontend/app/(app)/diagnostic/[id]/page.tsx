"use client";

import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import { useParams, useRouter, useSearchParams } from "next/navigation";
import {
  addProspectsToList,
  createDiagnosticShareLink,
  createProspectList,
  deleteDiagnostic,
  getDiagnostic,
  getDiagnosticBriefPdfUrl,
  getProspectLists,
  pollUntilDone,
  recordOutcome,
  submitDiagnostic,
} from "@/lib/api";
import type { DiagnosticResponse, ProspectList } from "@/lib/types";
import { computeVerdict } from "@/lib/verdict";
import { generatePitchBullets } from "@/lib/pitch";
import { cleanWebsiteDisplay, isSchemaRelated } from "@/lib/present";
import Button from "@/app/components/ui/Button";
import { Card } from "@/app/components/ui/Card";
import ListPickerModal from "@/app/components/ListPickerModal";

type SignalTab = "overview" | "competitors" | "siteGaps" | "fullAudit";
type OutreachAction = "Called" | "Left VM" | "Emailed" | "No Answer";

type CompetitorRow = {
  name: string;
  reviews: number;
  rating?: number | null;
  distance: string;
  note: string;
  isYou?: boolean;
};

type CtaTypeRow = {
  type: "Book" | "Schedule" | "Contact" | "Call";
  count: number;
  pages: string[];
  clickableCount?: number;
};

function firstNumber(value: unknown): number | null {
  if (value == null) return null;
  const match = String(value).match(/-?\d+(\.\d+)?/);
  if (!match) return null;
  const num = Number(match[0]);
  return Number.isFinite(num) ? num : null;
}

function formatMiles(value: unknown): string {
  if (value == null) return "—";
  const num = Number(value);
  if (!Number.isFinite(num)) return `${String(value)} mi`;
  return Number.isInteger(num) ? `${num} mi` : `${num.toFixed(1)} mi`;
}

function paidStatusLabel(raw: string): "Active" | "Not detected" | "Unknown" {
  const s = (raw || "").toLowerCase();
  if (!s || s === "—" || s.includes("unknown")) return "Unknown";
  if (s.includes("active") || s.includes("running")) return "Active";
  return "Not detected";
}

function boolLabel(value: boolean | null | undefined): string {
  if (value === true) return "Detected";
  if (value === false) return "Not detected in this scan";
  return "Not evaluated";
}

function boolIcon(value: boolean | null | undefined): string {
  if (value === true) return "✓";
  if (value === false) return "✕";
  return "—";
}

function crawlMethodLabel(raw: unknown): string {
  const v = String(raw || "").trim().toLowerCase();
  if (v === "hybrid_playwright_landing_only") return "Hybrid (Playwright on landing pages)";
  if (v === "playwright") return "Playwright (JS-rendered)";
  if (v === "requests") return "Requests (static)";
  if (v === "requests_fallback_playwright_unavailable") return "Requests fallback (Playwright unavailable)";
  return "Unknown";
}

function urlTitle(url: string): string {
  try {
    const u = new URL(url);
    const part = (u.pathname || "/").split("/").filter(Boolean).pop() || "homepage";
    return part.replace(/[-_]+/g, " ").replace(/\b\w/g, (m) => m.toUpperCase());
  } catch {
    return "Page";
  }
}

function normalizeUrlForMatch(value: string): string {
  const raw = String(value || "").trim();
  if (!raw) return "";
  const withProtocol = raw.startsWith("http://") || raw.startsWith("https://") ? raw : `https://${raw}`;
  try {
    const parsed = new URL(withProtocol);
    const host = parsed.hostname.replace(/^www\./, "");
    const path = parsed.pathname.replace(/\/+$/, "");
    return `${host}${path}`.toLowerCase();
  } catch {
    return raw.replace(/^https?:\/\//, "").replace(/^www\./, "").replace(/\/+$/, "").toLowerCase();
  }
}

function urlsMatch(a: string, b: string): boolean {
  const left = normalizeUrlForMatch(a);
  const right = normalizeUrlForMatch(b);
  if (!left || !right) return false;
  return left === right || left.includes(right) || right.includes(left);
}

function parseCompetitorLine(value: string): { name: string; reviews: number; distance: string; notes: string } {
  const parts = value
    .split(/[—-]/g)
    .map((p) => p.trim())
    .filter(Boolean);
  const name = parts[0] || value.trim();
  const reviewsMatch = value.match(/(\d+(?:\.\d+)?)\s*reviews?/i);
  const distanceMatch = value.match(/(\d+(?:\.\d+)?)\s*mi\b/i);
  const reviews = reviewsMatch ? Number(reviewsMatch[1]) : 0;
  const distance = distanceMatch ? `${distanceMatch[1]} mi` : "—";
  const notes = parts.slice(1).join(" — ") || "Competitive context";
  return { name, reviews, distance, notes };
}

function pct(n: number, d: number): number {
  if (d <= 0) return 0;
  return Math.max(0, Math.min(100, Math.round((n / d) * 100)));
}

function inferCtaTypeRows(totalCtas: number, pages: string[], evidenceText: string): CtaTypeRow[] {
  const types: Array<{ type: CtaTypeRow["type"]; tokens: RegExp[] }> = [
    { type: "Book", tokens: [/\bbook\b/gi] },
    { type: "Schedule", tokens: [/\bschedule\b/gi, /\bappointment\b/gi] },
    { type: "Contact", tokens: [/\bcontact\b/gi, /\bget in touch\b/gi] },
    { type: "Call", tokens: [/\bcall\b/gi, /\btel:\b/gi, /\bphone\b/gi] },
  ];

  const rawCounts = types.map((t) =>
    t.tokens.reduce((acc, rgx) => acc + (evidenceText.match(rgx)?.length || 0), 0),
  );
  const rawSum = rawCounts.reduce((a, b) => a + b, 0);

  let counts = rawCounts;
  if (totalCtas > 0 && rawSum > 0) {
    counts = rawCounts.map((c) => Math.round((c / rawSum) * totalCtas));
    const diff = totalCtas - counts.reduce((a, b) => a + b, 0);
    if (diff !== 0) counts[0] += diff;
  }

  return types.map((t, i) => {
    const token = t.type.toLowerCase();
    const pageMatches = pages.filter((p) => p.toLowerCase().includes(token)).slice(0, 5);
    return {
      type: t.type,
      count: counts[i] || 0,
      pages: pageMatches,
      clickableCount: undefined,
    };
  });
}

export default function DiagnosticDetailPage() {
  const params = useParams();
  const router = useRouter();
  const searchParams = useSearchParams();
  const id = Number(params.id);
  const invalidId = !id || Number.isNaN(id);
  const from = String(searchParams.get("from") || "").toLowerCase();
  const scanId = searchParams.get("scanId") || "";
  const listId = searchParams.get("listId") || "";

  const [result, setResult] = useState<DiagnosticResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [noticeHref, setNoticeHref] = useState<string | null>(null);
  const [noticeCta, setNoticeCta] = useState<string | null>(null);
  const [rerunning, setRerunning] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [sharing, setSharing] = useState(false);
  const [shareUrl, setShareUrl] = useState<string | null>(null);

  const [lists, setLists] = useState<ProspectList[]>([]);
  const [addListOpen, setAddListOpen] = useState(false);
  const [addingToList, setAddingToList] = useState(false);

  const [activeTab, setActiveTab] = useState<SignalTab>("overview");
  const [showMobileFooter, setShowMobileFooter] = useState(false);
  const [outreachOpen, setOutreachOpen] = useState(false);
  const [outreachAction, setOutreachAction] = useState<OutreachAction>("Called");
  const [outreachNote, setOutreachNote] = useState("");
  const [loggingOutreach, setLoggingOutreach] = useState(false);
  const [activeCtaType, setActiveCtaType] = useState<CtaTypeRow["type"] | null>(null);
  const [ctaDrawerOpen, setCtaDrawerOpen] = useState(false);

  const headerRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (invalidId) return;
    getDiagnostic(id)
      .then(setResult)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [id, invalidId]);

  useEffect(() => {
    let cancelled = false;
    async function loadLists() {
      const data = await getProspectLists().catch(() => ({ items: [] }));
      if (!cancelled) setLists(data.items);
    }
    void loadLists();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    function onScrollOrResize() {
      const isMobile = window.innerWidth < 1024;
      const heroBottom = headerRef.current?.getBoundingClientRect().bottom ?? Number.POSITIVE_INFINITY;
      setShowMobileFooter(isMobile || heroBottom < 0);
    }
    onScrollOrResize();
    window.addEventListener("scroll", onScrollOrResize, { passive: true });
    window.addEventListener("resize", onScrollOrResize);
    return () => {
      window.removeEventListener("scroll", onScrollOrResize);
      window.removeEventListener("resize", onScrollOrResize);
    };
  }, []);

  const navContext = (() => {
    if (from === "ask") return { crumbLabel: "Ask Neyma", backLabel: "Back to results", backHref: "/ask" };
    if (from === "territory" && scanId) {
      return { crumbLabel: "Territory Scan", backLabel: "Back to territory scan", backHref: `/territory/${encodeURIComponent(scanId)}` };
    }
    if (from === "list" && listId) {
      return { crumbLabel: "List", backLabel: "Back to list", backHref: `/lists/${encodeURIComponent(listId)}` };
    }
    return { crumbLabel: "Workspace", backLabel: "Back to workspace", backHref: "/dashboard" };
  })();

  async function handleRerun(deepAudit = false) {
    if (!result) return;
    setError(null);
    setNoticeHref(null);
    setNoticeCta(null);
    setRerunning(true);
    try {
      const { job_id } = await submitDiagnostic({
        business_name: result.business_name,
        city: result.city,
        state: result.state || "",
        ...(result.website ? { website: result.website } : {}),
        deep_audit: deepAudit,
        source_diagnostic_id: id,
      });
      setNotice(deepAudit ? "Deep audit started..." : "Brief refresh started...");
      const job = await pollUntilDone(job_id, undefined, 2000, deepAudit ? 300 : 150);
      if (job.status === "completed" && job.diagnostic_id) {
        if (Number(job.diagnostic_id) === Number(id)) {
          router.replace(`/diagnostic/${job.diagnostic_id}?refresh=${Date.now()}`);
          setNotice("Results refreshed.");
          setRerunning(false);
          return;
        }
        router.push(`/diagnostic/${job.diagnostic_id}`);
      } else {
        setError(job.error || "Re-run failed");
        setRerunning(false);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Re-run failed");
      setRerunning(false);
    }
  }

  async function handleDelete() {
    if (!confirm("Delete this brief?")) return;
    setDeleting(true);
    try {
      await deleteDiagnostic(id);
      router.push("/dashboard");
    } catch {
      setDeleting(false);
    }
  }

  async function handleShare() {
    setSharing(true);
    try {
      const res = await createDiagnosticShareLink(id);
      const uiUrl = `${window.location.origin}/brief/s/${res.token}`;
      setShareUrl(uiUrl);
      await navigator.clipboard.writeText(uiUrl);
      setNotice("Share link copied");
      setNoticeHref(null);
      setNoticeCta(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to create share link");
    } finally {
      setSharing(false);
    }
  }

  async function handleAddToList(payload: { listId?: number; newListName?: string }) {
    setAddingToList(true);
    try {
      let listId = payload.listId;
      let createdNew = false;
      if (!listId) {
        if (!payload.newListName) throw new Error("List name is required");
        const created = await createProspectList(payload.newListName);
        listId = created.id;
        createdNew = true;
      }
      await addProspectsToList(listId, [id]);
      const updated = await getProspectLists().catch(() => ({ items: [] }));
      setLists(updated.items);
      setNotice(`Added ${result?.business_name || "prospect"} to ${createdNew ? "new list" : "list"}.`);
      setNoticeHref(`/lists/${listId}`);
      setNoticeCta("Open list");
      setAddListOpen(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to add to list");
    } finally {
      setAddingToList(false);
    }
  }

  async function handleLogOutreach() {
    if (!result) return;
    setLoggingOutreach(true);
    try {
      await recordOutcome({
        diagnostic_id: id,
        outcome_type: "outreach_log",
        outcome_data: {
          action: outreachAction,
          note: outreachNote,
          logged_at: new Date().toISOString(),
        },
      });
      setNotice(`Outreach logged: ${outreachAction}`);
      setNoticeHref(null);
      setNoticeCta(null);
      setOutreachOpen(false);
      setOutreachNote("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to log outreach");
    } finally {
      setLoggingOutreach(false);
    }
  }

  if (invalidId) return <div className="mx-auto max-w-6xl px-2 py-10"><p className="text-red-600">Invalid brief ID</p></div>;
  if (loading) {
    return (
      <div className="mx-auto max-w-6xl px-2 py-10 text-center">
        <div className="inline-block h-6 w-6 animate-spin rounded-full border-2 border-zinc-300 border-t-zinc-600" />
        <p className="mt-3 text-sm text-zinc-500">Loading brief...</p>
      </div>
    );
  }
  if (!result) return <div className="mx-auto max-w-6xl px-2 py-10"><p className="text-red-600">{error || "Brief not found"}</p></div>;

  const b = result.brief || {};
  const ed = b?.executive_diagnosis || {};
  const mp = b?.market_position || {};
  const ds = b?.demand_signals || {};
  const sg = b?.strategic_gap || {};
  const csg = b?.competitive_service_gap || {};
  const conv = b?.conversion_infrastructure || result.conversion_infrastructure || {};
  const convStruct = b?.conversion_structure || {};
  const reviewIntel = b?.review_intelligence || result.review_intelligence || {};
  const svc = result.service_intelligence || {};
  const spaV2 = (b?.service_page_analysis?.v2 as Record<string, unknown> | undefined)
    || (svc.service_page_analysis_v2 as Record<string, unknown> | undefined)
    || {};

  const verdict = computeVerdict(result);
  const websiteHref = result.website
    ? (result.website.startsWith("http://") || result.website.startsWith("https://") ? result.website : `https://${result.website}`)
    : null;
  const websiteLabel = cleanWebsiteDisplay(result.website);
  const phoneLabel = result.phone ? String(result.phone).trim() : "";
  const phoneHref = phoneLabel ? `tel:${phoneLabel}` : null;
  const paidStatus = paidStatusLabel(String(ds?.google_ads_line || result.paid_status || "Unknown"));
  const reviewCount = firstNumber(mp?.reviews) || firstNumber(result.review_position) || 0;
  const rating = firstNumber(result.review_position);
  const localAvgReviews = firstNumber(mp?.local_avg) || firstNumber((result.evidence || []).find((e) => String(e.label || "").toLowerCase().includes("review"))?.value) || 0;

  const opportunityBand = String(
    ed?.modeled_revenue_upside
    || (b?.revenue_upside_capture_gap?.annual_low != null
      ? `$${Number(b.revenue_upside_capture_gap.annual_low).toLocaleString()}-$${Number(b.revenue_upside_capture_gap.annual_high ?? 0).toLocaleString()}`
      : result.opportunity_profile || "—"),
  );
  const opportunityLabel = String(ed?.opportunity_profile?.label || verdict.label || "Opportunity Signal");
  const topGap = String(verdict.topGap || csg?.service || sg?.service || "—");

  const crawlMethodRaw = String(svc.crawl_method || "").toLowerCase();
  const usesPlaywrightPath = crawlMethodRaw.includes("playwright");
  const crawlWarning = String(svc.crawl_warning || "").trim();
  const jsDetected = Boolean(svc.js_detected);
  const pagesCrawled = Number(svc.pages_crawled || 0);
  const geoIntentPages = Number(
    (svc.geo_intent_pages?.length ?? 0)
    || (spaV2 as Record<string, Record<string, unknown>>).local_intent_coverage?.city_or_near_me_pages
    || svc.city_or_near_me_page_count
    || 0,
  );
  const servicePages = Number(svc.service_page_count || 0);
  const ctaElements = Array.isArray(svc.cta_elements) ? svc.cta_elements : [];
  const ctaCount = Number(
    ctaElements.reduce((sum, row) => sum + Number(row?.count || 0), 0)
    || convStruct.cta_count
    || 0,
  );

  const talkingPoints = generatePitchBullets(result).slice(0, 3);
  const revenueDrivers = verdict.reasons.slice(0, 4);

  const rawRisks = ((b?.risk_flags || result.risk_flags || []) as string[])
    .filter((flag) => {
      const text = String(flag || "").toLowerCase();
      if (isSchemaRelated(text)) return false;
      if (!usesPlaywrightPath && (text.includes("landing page") || text.includes("high-value service pages"))) return false;
      return true;
    });

  const competitorRows: CompetitorRow[] = [];
  competitorRows.push({
    name: result.business_name,
    reviews: reviewCount,
    rating: rating ?? null,
    distance: "—",
    note: "You",
    isYou: true,
  });

  if (Array.isArray(result.competitors) && result.competitors.length) {
    for (const comp of result.competitors) {
      const n = String(comp?.name || "").trim();
      if (!n) continue;
      if (competitorRows.some((r) => r.name.toLowerCase() === n.toLowerCase())) continue;
      competitorRows.push({
        name: n,
        reviews: Number(comp?.reviews || 0),
        rating: comp?.rating ?? null,
        distance: String(comp?.distance || "—"),
        note: String(comp?.note || "Nearby competitor"),
      });
    }
  }

  const addRow = (name: unknown, reviewsVal: unknown, distanceVal: unknown, note: string) => {
    const n = String(name || "").trim();
    if (!n) return;
    if (competitorRows.some((r) => r.name.toLowerCase() === n.toLowerCase())) return;
    competitorRows.push({
      name: n,
      reviews: Number(firstNumber(reviewsVal) || 0),
      distance: formatMiles(distanceVal),
      note,
    });
  };

  addRow(sg?.competitor_name, sg?.competitor_reviews, sg?.distance_miles, "Nearest competitor");
  addRow(csg?.competitor_name, csg?.competitor_reviews, csg?.distance_miles, "Service-gap competitor");

  const line3 = Array.isArray((b?.competitive_context as Record<string, unknown> | undefined)?.line3_items)
    ? ((b?.competitive_context as Record<string, unknown>).line3_items as string[])
    : [];
  for (const item of line3) {
    const p = parseCompetitorLine(item);
    addRow(p.name, p.reviews, p.distance, p.notes);
  }

  const maxReviews = Math.max(...competitorRows.map((r) => r.reviews), 1);
  const nearestReviews = Number(firstNumber(sg?.competitor_reviews) || firstNumber(csg?.competitor_reviews) || 0);
  const reviewDelta = nearestReviews > 0 ? reviewCount - nearestReviews : 0;

  const cityTokens = String(result.city || "").toLowerCase().split(/\s+/).filter(Boolean);
  const geoExamples = (
    ((spaV2 as Record<string, Record<string, unknown>>).local_intent_coverage?.examples as string[] | undefined)
    || (svc.geo_page_examples as string[] | undefined)
    || []
  ).filter(Boolean);

  const detectedGeoPages = (Array.isArray(svc.geo_intent_pages) && svc.geo_intent_pages.length
    ? svc.geo_intent_pages.map((row) => ({
      url: String(row.url || ""),
      title: String(row.title || urlTitle(String(row.url || ""))),
      signals: ((row.signals || []) as Array<"city" | "near-me" | "schema" | "meta">),
      hasCTA: Boolean(row.hasCTA),
    }))
    : geoExamples.map((url) => {
    const lower = String(url).toLowerCase();
    const hasCity = cityTokens.some((t) => t.length >= 3 && lower.includes(t));
    const hasNearMe = lower.includes("near") || lower.includes("near-me") || lower.includes("nearme");
    const hasCTA = /(book|schedule|appointment|contact|call|tel:)/i.test(lower) || ctaCount > 0;
    const signals = [hasCity ? "city" : "", hasNearMe ? "near-me" : ""].filter(Boolean) as Array<"city" | "near-me">;
    return {
      url,
      title: urlTitle(url),
      signals,
      hasCTA,
    };
  }));

  const missingGeoPages = (Array.isArray(svc.missing_geo_pages) && svc.missing_geo_pages.length
    ? svc.missing_geo_pages
      .filter((row) => String(row.priority || "low").toLowerCase() === "high")
      .map((row) => ({
        slug: String(row.slug || ""),
        title: String(row.title || "Service Page"),
        reason: String(row.reason || "No crawled URL matched expected service page slug."),
      }))
    : []);
  const verifiedServiceSignals = (Array.isArray(svc.signal_verification?.services) ? svc.signal_verification?.services : [])
    .map((row) => ({
      service: String(row?.display_name || row?.service || "Service"),
      verdict: String(row?.final_verdict || row?.deterministic_verdict || "not_evaluated").toLowerCase(),
      confidence: String(row?.final_confidence || row?.deterministic_confidence || "low").toLowerCase(),
      reason: String(row?.reason || "No reason captured."),
      url: row?.url ? String(row.url) : "",
      aiVerdict: row?.ai_validation?.enabled ? String(row?.ai_validation?.verdict || "").toLowerCase() : "",
      aiConfidence: row?.ai_validation?.enabled ? String(row?.ai_validation?.confidence || "").toLowerCase() : "",
      aiReason: row?.ai_validation?.enabled ? String(row?.ai_validation?.reason || "") : "",
    }));

  const crawlPages = (((svc as unknown as { service_page_detection_debug?: { pages_crawled?: string[] } }).service_page_detection_debug?.pages_crawled) || []).map(String);
  const evidenceText = `${(b?.evidence_bullets || []).join(" ")} ${(result.evidence || []).map((e) => `${e.label}: ${e.value}`).join(" ")}`;
  const ctaRows = ctaElements.length
    ? ctaElements.map((row) => ({
      type: String(row.type || "Contact") as CtaTypeRow["type"],
      count: Number(row.count || 0),
      pages: Array.isArray(row.pages) ? row.pages.map(String).slice(0, 5) : [],
      clickableCount: Number(row.clickable_count || 0),
    }))
    : inferCtaTypeRows(ctaCount, crawlPages, evidenceText);
  const clickableCtaCount = Number(
    svc.cta_clickable_count
    ?? ctaRows.reduce((sum, row) => sum + Number(row.clickableCount || 0), 0)
    ?? 0,
  );
  const activeCtaRow = activeCtaType ? ctaRows.find((row) => row.type === activeCtaType) || null : null;
  const activeCtaPages = activeCtaRow?.pages || [];
  const activeCtaPageCount = activeCtaPages.length;

  const statusBadge = verdict.verdict === "HIGH_LEVERAGE" ? "STRONG LEAD" : "QUALIFIED LEAD";

  return (
    <div className="mx-auto max-w-7xl px-2 pb-24 md:px-4">
      <main className="space-y-4">
        <p className="text-sm text-[var(--text-muted)]">
          <Link href={navContext.backHref} className="app-link">{navContext.crumbLabel}</Link>
          <span> → </span>
          <span>{result.business_name}</span>
        </p>

        {(notice || error) && (
          <Card className="p-3">
            {notice && (
              <div className="flex flex-wrap items-center gap-2 text-sm text-emerald-700">
                <span>{notice}</span>
                {noticeHref && noticeCta ? <Link href={noticeHref} className="app-link font-medium">{noticeCta}</Link> : null}
              </div>
            )}
            {error && <p className="text-sm text-red-600">{error}</p>}
          </Card>
        )}

        <Card ref={headerRef} className="sticky top-2 z-20 border border-slate-200 bg-white p-4 shadow-sm">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="space-y-1">
              <h1 className="text-lg font-semibold tracking-tight text-slate-900">{result.business_name}</h1>
              <div className="flex flex-wrap items-center gap-2 text-xs text-slate-600">
                <span>{result.city}{result.state ? `, ${result.state}` : ""}</span>
                {(websiteHref || phoneHref) ? <span>•</span> : null}
                {websiteHref ? (
                  <a href={websiteHref} target="_blank" rel="noreferrer" className="font-medium text-slate-700 hover:underline">
                    {websiteLabel || websiteHref}
                  </a>
                ) : <span>Website not available</span>}
                {phoneHref ? (
                  <>
                    <span>•</span>
                    <a href={phoneHref} className="font-medium text-slate-700 hover:underline">
                      {phoneLabel}
                    </a>
                  </>
                ) : null}
              </div>
            </div>
            <div className="flex items-center gap-2">
              <span className="inline-flex rounded-md border border-emerald-300 bg-emerald-50 px-2 py-1 text-xs font-semibold text-emerald-700">
                {statusBadge}
              </span>
              <Button onClick={() => setAddListOpen(true)}>Add to Pipeline</Button>
              <Button onClick={() => setOutreachOpen(true)} className="border-[var(--border-default)]">Log Outreach</Button>
            </div>
          </div>

          <div className="mt-3 grid gap-2 sm:grid-cols-2 lg:grid-cols-5">
            <QuickStat label="Opportunity" value={opportunityBand} />
            <QuickStat label="Reviews / Rating" value={`${reviewCount || "—"}${rating ? ` / ${rating.toFixed(1)}` : ""}`} />
            <QuickStat label="Paid Ads" value={paidStatus} />
            <QuickStat label="Top Gap" value={topGap} />
            <QuickStat label="Market Density" value={String(mp?.market_density || result.market_density || "—")} />
          </div>
        </Card>

        <Card className="p-4">
          <div className="mb-3 flex flex-wrap gap-2">
            <TabButton active={activeTab === "overview"} onClick={() => setActiveTab("overview")}>Overview</TabButton>
            <TabButton active={activeTab === "competitors"} onClick={() => setActiveTab("competitors")}>Competitors</TabButton>
            <TabButton active={activeTab === "siteGaps"} onClick={() => setActiveTab("siteGaps")}>Site Gaps</TabButton>
            <TabButton active={activeTab === "fullAudit"} onClick={() => setActiveTab("fullAudit")}>Full Audit</TabButton>
          </div>
          {activeCtaRow && (
            <div className="mb-3 flex flex-wrap items-center gap-2 rounded-[20px] border border-[#4f79c7]/16 bg-[linear-gradient(135deg,rgba(79,121,199,0.08)_0%,rgba(242,191,47,0.08)_100%)] px-3 py-3">
              <span className="text-xs font-semibold uppercase tracking-[0.16em] text-[var(--text-muted)]">Active filter</span>
              <span className="inline-flex items-center gap-2 rounded-full border border-black/8 bg-white px-3 py-1.5 text-xs font-medium text-[var(--text-primary)]">
                CTA: {activeCtaRow.type}
                <button
                  type="button"
                  onClick={() => {
                    setActiveCtaType(null);
                    setCtaDrawerOpen(false);
                  }}
                  className="inline-flex h-4 w-4 items-center justify-center rounded-full bg-black/6 text-[10px] text-[var(--text-secondary)] hover:bg-black/10"
                  aria-label="Clear CTA filter"
                >
                  ×
                </button>
              </span>
              <span className="text-xs text-[var(--text-secondary)]">
                Highlighting {activeCtaPageCount} related page{activeCtaPageCount === 1 ? "" : "s"} in this brief.
              </span>
            </div>
          )}

          {activeTab === "overview" && (
            <div className="space-y-3 text-sm">
              <div className="grid gap-3 lg:grid-cols-2">
                <Card className="border border-slate-200 p-3">
                  <h3 className="text-sm font-semibold text-slate-900">Revenue Signal</h3>
                  <div className="mt-2 space-y-2">
                    <p><span className="text-xs text-slate-500">Opportunity Band:</span> <span className="font-semibold">{opportunityBand}</span></p>
                    <p><span className="text-xs text-slate-500">Opportunity Label:</span> <span className="font-semibold">{opportunityLabel}</span></p>
                    <div>
                      <p className="text-xs text-slate-500">Revenue Drivers</p>
                      <ul className="mt-1 list-disc space-y-1 pl-5 text-slate-700">
                        {revenueDrivers.length ? revenueDrivers.map((r, i) => <li key={i}>{r}</li>) : <li>No urgent revenue drivers detected.</li>}
                      </ul>
                    </div>
                  </div>
                </Card>

                <Card className="border border-slate-200 p-3">
                  <h3 className="text-sm font-semibold text-slate-900">Talking Points</h3>
                  <ol className="mt-2 list-decimal space-y-1 pl-5 text-slate-700">
                    {talkingPoints.length ? talkingPoints.map((t, i) => <li key={i}>{t}</li>) : <li>No talking points generated.</li>}
                  </ol>
                </Card>
              </div>

              {rawRisks.length > 0 ? (
                <div className="rounded-md border border-amber-300 bg-amber-50 px-3 py-2 text-amber-900">
                  <p className="text-xs font-semibold uppercase tracking-wide">Risk Flags</p>
                  <div className="mt-1 flex flex-wrap gap-2">
                    {rawRisks.slice(0, 5).map((flag, idx) => (
                      <span key={idx} className="rounded border border-amber-300 bg-white px-2 py-1 text-xs">{flag}</span>
                    ))}
                  </div>
                </div>
              ) : null}
            </div>
          )}

          {activeTab === "competitors" && (
            <div className="space-y-3 text-sm">
              <div className="grid gap-3 md:grid-cols-3">
                <MetricCard label="Market Density" value={String(mp?.market_density || result.market_density || "—")} />
                <MetricCard label="Review Delta vs Nearest" value={nearestReviews > 0 ? `${reviewDelta >= 0 ? "+" : ""}${reviewDelta}` : "—"} />
                <MetricCard label="Local Avg Reviews" value={localAvgReviews ? String(localAvgReviews) : "—"} />
              </div>

              <div className="overflow-x-auto rounded-md border border-slate-200">
                <table className="min-w-full text-left text-xs sm:text-sm">
                  <thead className="bg-slate-50 text-slate-600">
                    <tr>
                      <th className="px-3 py-2 font-medium">Practice</th>
                      <th className="px-3 py-2 font-medium">Reviews</th>
                      <th className="px-3 py-2 font-medium">Distance</th>
                      <th className="px-3 py-2 font-medium">Notes</th>
                    </tr>
                  </thead>
                  <tbody>
                    {competitorRows.map((row, idx) => (
                      <tr key={`${row.name}-${idx}`} className={`border-t border-slate-200 ${row.isYou ? "bg-blue-50" : "bg-white"}`}>
                        <td className="px-3 py-2 font-medium text-slate-900">{row.name}{row.isYou ? " (You)" : ""}</td>
                        <td className="px-3 py-2">
                          <div className="flex items-center gap-2">
                            <span className="w-10 text-right tabular-nums">{row.reviews || "—"}</span>
                            <div className="h-2 w-28 rounded bg-slate-100">
                              <div className={`h-2 rounded ${row.isYou ? "bg-blue-600" : "bg-slate-400"}`} style={{ width: `${pct(row.reviews, maxReviews)}%` }} />
                            </div>
                          </div>
                        </td>
                        <td className="px-3 py-2">{row.distance}</td>
                        <td className="px-3 py-2 text-slate-700">{row.note}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <div className="rounded-md border border-slate-200 bg-slate-50 p-3 text-slate-700">
                <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">Key Insight</p>
                <p className="mt-1">{verdict.reasons[0] || "Competitive pressure appears moderate; prioritize converting existing demand."}</p>
              </div>
            </div>
          )}

          {activeTab === "siteGaps" && (
            <div className="space-y-3 text-sm">
              <div className="grid gap-3 md:grid-cols-4">
                <MetricCard label="Pages Crawled" value={String(pagesCrawled || "—")} />
                <MetricCard label="Geo Intent Pages" value={String(geoIntentPages || 0)} />
                <MetricCard label="Service Pages" value={String(servicePages || 0)} tone={servicePages < 3 ? "danger" : "default"} />
                <MetricCard label="CTA Elements" value={String(ctaCount || 0)} />
              </div>

              <Card className="border border-slate-200 p-3">
                <h3 className="text-sm font-semibold text-slate-900">Capture Readiness</h3>
                <div className="mt-2 space-y-2">
                  <CaptureReadinessRow
                    label="Online Scheduling"
                    status={boolLabel(conv.online_booking as boolean | null | undefined)}
                    icon={boolIcon(conv.online_booking as boolean | null | undefined)}
                    note="Booking path detection from rendered page content."
                  />
                  <CaptureReadinessRow
                    label="Contact Form"
                    status={boolLabel(conv.contact_form as boolean | null | undefined)}
                    icon={boolIcon(conv.contact_form as boolean | null | undefined)}
                    note={convStruct.form_single_or_multi_step ? `Form structure: ${String(convStruct.form_single_or_multi_step)}` : "No form structure signal found."}
                  />
                  <CaptureReadinessRow
                    label="Phone Prominent"
                    status={boolLabel(conv.phone_prominent as boolean | null | undefined)}
                    icon={boolIcon(conv.phone_prominent as boolean | null | undefined)}
                    note="Phone prominence from homepage and key page scan."
                  />
                  <CaptureReadinessRow
                    label="Clickable Phone (tel:)"
                    status={boolLabel(convStruct.phone_clickable as boolean | null | undefined)}
                    icon={boolIcon(convStruct.phone_clickable as boolean | null | undefined)}
                    note="Homepage tap-to-call detection."
                  />
                  <CaptureReadinessRow
                    label="JS Rendering"
                    status={usesPlaywrightPath ? "Enabled" : "Unavailable"}
                    icon={usesPlaywrightPath ? "✓" : "✕"}
                    note={crawlMethodLabel(svc.crawl_method)}
                  />
                </div>
              </Card>

              {!usesPlaywrightPath ? (
                <div className="rounded-md border border-amber-300 bg-amber-50 p-3 text-amber-900">
                  <p className="text-xs font-semibold uppercase tracking-wide">Accuracy Note</p>
                  <p className="mt-1 text-sm">{crawlWarning || "Playwright was unavailable; this run used static crawl signals only."}</p>
                </div>
              ) : null}
            </div>
          )}

          {activeTab === "fullAudit" && (
            <div className="space-y-4 text-sm">
              <div className="rounded-md border border-slate-200 bg-slate-50 p-3">
                <div className="flex flex-wrap items-center gap-3">
                  <MetaChip label="Pages" value={String(pagesCrawled || 0)} />
                  <MetaChip label="JS Detected" value={jsDetected ? "Yes" : "No"} tone="badge" />
                  <MetaChip label="Method" value={crawlMethodLabel(svc.crawl_method)} />
                </div>
                {!usesPlaywrightPath ? (
                  <p className="mt-2 rounded border border-amber-300 bg-amber-50 px-2 py-1 text-xs text-amber-900">
                    Static-only crawl warning: {crawlWarning || "Playwright unavailable for this run."}
                  </p>
                ) : null}
              </div>

              <section className="grid gap-3 lg:grid-cols-2">
                <Card className="border border-slate-200 p-3">
                  <h3 className="text-sm font-semibold text-slate-900">Reviews & Reputation</h3>
                  <ul className="mt-2 space-y-1 text-slate-700">
                    <li>Your reviews vs avg: <strong>{reviewCount || "—"} vs {localAvgReviews || "—"}</strong></li>
                    <li>Last review date: <strong>{(ds as Record<string, unknown>).last_review_days_ago != null ? `${String((ds as Record<string, unknown>).last_review_days_ago)} days ago` : "—"}</strong></li>
                    <li>Competitors sampled: <strong>{String((result.evidence || []).find((e) => String(e.label || "").toLowerCase().includes("reviews vs market")) ? competitorRows.length - 1 : competitorRows.length - 1)}</strong></li>
                  </ul>
                  <div className="mt-2 rounded border border-slate-200 bg-slate-50 p-2">
                    <p className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Review Summary</p>
                    <p className="mt-1 text-xs text-slate-700">
                      {String((reviewIntel as Record<string, unknown>).summary || "No review-theme summary available for this lead yet.")}
                    </p>
                  </div>
                </Card>

                <Card className="border border-slate-200 p-3">
                  <h3 className="text-sm font-semibold text-slate-900">Market Context</h3>
                  <ul className="mt-2 space-y-1 text-slate-700">
                    <li>Density: <strong>{String(mp?.market_density || result.market_density || "—")}</strong></li>
                    <li>Visibility gap: <strong>{String(svc.visibility_gap || svc.high_value_service_leverage || "—")}</strong></li>
                    <li>Service pages found: <strong>{servicePages}</strong></li>
                    <li>Form structure: <strong>{String(convStruct.form_single_or_multi_step || "unknown")}</strong></li>
                    <li>Visibility channel: <strong>{result.website ? "Website present" : "No website"}</strong></li>
                  </ul>
                </Card>
              </section>

              <section className="space-y-2">
                <h3 className="text-sm font-semibold text-slate-900">Geo Intent Coverage</h3>
                <div className="rounded-md border border-slate-200 p-3">
                  <div className="mb-2 flex items-center justify-between text-xs text-slate-600">
                    <span>{geoIntentPages} of 30 pages</span>
                    <span>{pct(geoIntentPages, 30)}%</span>
                  </div>
                  <div className="h-2 rounded bg-slate-100">
                    <div className="h-2 rounded bg-blue-600" style={{ width: `${pct(geoIntentPages, 30)}%` }} />
                  </div>
                </div>

                <Card className="border border-slate-200 p-3">
                  <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">Detected</p>
                  <div className="overflow-x-auto">
                    <table className="min-w-full text-left text-xs sm:text-sm">
                      <thead className="bg-slate-50 text-slate-600">
                        <tr>
                          <th className="px-2 py-1.5">Page Title</th>
                          <th className="px-2 py-1.5">URL</th>
                          <th className="px-2 py-1.5">Signal Tags</th>
                          <th className="px-2 py-1.5">CTA present</th>
                        </tr>
                      </thead>
                      <tbody>
                        {detectedGeoPages.length ? detectedGeoPages.map((row, idx) => {
                          const highlighted = activeCtaPages.some((page) => urlsMatch(page, row.url));
                          return (
                          <tr key={`${row.url}-${idx}`} className={`border-t border-slate-200 ${highlighted ? "bg-amber-50/70" : ""}`}>
                            <td className="px-2 py-1.5 text-slate-900">{row.title}</td>
                            <td className={`px-2 py-1.5 font-mono text-[11px] ${highlighted ? "text-amber-900" : "text-slate-700"}`}>{row.url}</td>
                            <td className="px-2 py-1.5">
                              <div className="flex flex-wrap gap-1">
                                {row.signals.includes("city") ? <span className="rounded bg-blue-100 px-1.5 py-0.5 text-[10px] text-blue-700">📍 city</span> : null}
                                {row.signals.includes("near-me") ? <span className="rounded bg-emerald-100 px-1.5 py-0.5 text-[10px] text-emerald-700">🔍 near-me</span> : null}
                                {row.signals.includes("schema") ? <span className="rounded bg-violet-100 px-1.5 py-0.5 text-[10px] text-violet-700">🏷 schema</span> : null}
                                {row.signals.includes("meta") ? <span className="rounded bg-amber-100 px-1.5 py-0.5 text-[10px] text-amber-800">📝 meta</span> : null}
                                {!row.signals.length ? <span className="text-slate-400">—</span> : null}
                              </div>
                            </td>
                            <td className="px-2 py-1.5">{row.hasCTA ? "Yes" : "No"}</td>
                          </tr>
                        );}) : (
                          <tr><td className="px-2 py-2 text-slate-500" colSpan={4}>No geo-intent pages captured.</td></tr>
                        )}
                      </tbody>
                    </table>
                  </div>
                </Card>

                <Card className="border border-red-200 bg-red-50 p-3">
                  <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-red-700">Missing Geo Intent</p>
                  {missingGeoPages.length ? (
                    <ul className="space-y-1.5 text-red-900">
                      {missingGeoPages.map((row, idx) => (
                        <li key={`${row.slug}-${idx}`} className="rounded border border-red-200 bg-white px-2 py-1.5">
                          <p className="font-medium">{row.title}</p>
                          <p className="font-mono text-[11px]">/{row.slug}</p>
                          <p className="text-xs">{row.reason}</p>
                        </li>
                      ))}
                    </ul>
                  ) : <p className="text-xs text-red-800">No high-value pages were flagged as missing geo intent in this run.</p>}
                </Card>

                {verifiedServiceSignals.length ? (
                  <Card className="border border-slate-200 p-3">
                    <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">Service Signal Verification</p>
                    <div className="space-y-1.5">
                      {verifiedServiceSignals.slice(0, 8).map((row, idx) => {
                        const verdictLabel = row.verdict === "missing" ? "Missing" : row.verdict === "present" ? "Present" : "Not evaluated";
                        const verdictTone = row.verdict === "missing" ? "bg-red-100 text-red-700" : row.verdict === "present" ? "bg-emerald-100 text-emerald-700" : "bg-slate-200 text-slate-700";
                        const confTone = row.confidence === "high" ? "bg-emerald-100 text-emerald-700" : row.confidence === "medium" ? "bg-amber-100 text-amber-800" : "bg-slate-200 text-slate-700";
                        const highlighted = row.url ? activeCtaPages.some((page) => urlsMatch(page, row.url)) : false;
                        return (
                          <div key={`${row.service}-${idx}`} className={`rounded border px-2 py-1.5 ${highlighted ? "border-amber-300 bg-amber-50/70" : "border-slate-200 bg-white"}`}>
                            <div className="flex flex-wrap items-center gap-1.5">
                              <span className="font-medium text-slate-900">{row.service}</span>
                              <span className={`rounded px-1.5 py-0.5 text-[10px] font-semibold ${verdictTone}`}>{verdictLabel}</span>
                              <span className={`rounded px-1.5 py-0.5 text-[10px] font-semibold ${confTone}`}>{row.confidence}</span>
                              {row.aiVerdict ? <span className="rounded bg-blue-100 px-1.5 py-0.5 text-[10px] font-semibold text-blue-700">AI: {row.aiVerdict}</span> : null}
                            </div>
                            <p className="mt-1 text-xs text-slate-700">{row.reason}</p>
                            {row.aiReason ? <p className="mt-0.5 text-[11px] text-slate-500">AI note: {row.aiReason}</p> : null}
                            {row.url ? <p className={`mt-0.5 font-mono text-[10px] ${highlighted ? "text-amber-900" : "text-slate-500"}`}>{row.url}</p> : null}
                          </div>
                        );
                      })}
                    </div>
                  </Card>
                ) : null}
              </section>

              <section className="space-y-2">
                <h3 className="text-sm font-semibold text-slate-900">CTA Elements</h3>
                <Card className="border border-slate-200 p-3">
                  <p className="text-xs text-slate-500">Total CTA elements detected</p>
                  <p className="text-2xl font-semibold text-slate-900">{ctaCount}</p>
                  <p className="mt-1 text-xs text-slate-500">Of these, {clickableCtaCount} are clickable links/buttons.</p>
                  <div className="mt-3 space-y-2">
                    {ctaRows.map((row) => (
                      <button
                        key={row.type}
                        type="button"
                        onClick={() => {
                          setActiveCtaType(row.type);
                          setCtaDrawerOpen(true);
                        }}
                        className={`block w-full rounded border p-2 text-left transition ${activeCtaType === row.type ? "border-amber-300 bg-amber-50/70" : activeCtaType && activeCtaType !== row.type ? "border-slate-200 bg-white opacity-40 hover:opacity-70" : "border-slate-200 bg-white hover:border-slate-300 hover:bg-slate-50"}`}
                      >
                        <div className="mb-1 flex items-center justify-between">
                          <span className={`rounded px-1.5 py-0.5 text-[10px] font-semibold ${row.type === "Book" ? "bg-blue-100 text-blue-700" : row.type === "Schedule" ? "bg-emerald-100 text-emerald-700" : row.type === "Contact" ? "bg-amber-100 text-amber-800" : "bg-slate-200 text-slate-700"}`}>{row.type}</span>
                          <span className="text-xs font-semibold text-slate-900">{row.count}</span>
                        </div>
                        <p className="mb-1 text-[11px] text-slate-500">{Number(row.clickableCount || 0)} clickable</p>
                        <div className="h-1.5 rounded bg-slate-100">
                          <div className="h-1.5 rounded bg-slate-600" style={{ width: `${pct(row.count, Math.max(ctaCount, 1))}%` }} />
                        </div>
                        <div className="mt-1 flex flex-wrap gap-1">
                          {row.pages.length ? row.pages.map((u, idx) => (
                            <span key={`${u}-${idx}`} className={`rounded px-1.5 py-0.5 font-mono text-[10px] ${activeCtaType === row.type ? "bg-amber-100 text-amber-900" : "bg-slate-100 text-slate-700"}`}>{u}</span>
                          )) : <span className="text-[11px] text-slate-400">No explicit page-level URL match captured.</span>}
                        </div>
                      </button>
                    ))}
                  </div>
                </Card>
              </section>

              <section>
                <h3 className="mb-2 text-sm font-semibold text-slate-900">Risk Flags</h3>
                <div className="rounded-md border border-amber-300 bg-amber-50 p-3">
                  <div className="flex flex-wrap gap-2">
                    {rawRisks.length ? rawRisks.map((flag, idx) => (
                      <span key={idx} className="rounded border border-amber-300 bg-white px-2 py-1 text-xs text-amber-900">{flag}</span>
                    )) : <span className="text-xs text-amber-900">No high-priority risks detected for this run.</span>}
                  </div>
                </div>
              </section>
            </div>
          )}
        </Card>

        <Card className="p-3 text-xs text-[var(--text-secondary)]">
          <div className="flex flex-wrap gap-2">
            <Link href={navContext.backHref} className="app-link">{navContext.backLabel}</Link>
            <Button onClick={handleShare} disabled={sharing}>{sharing ? "Sharing..." : "Share"}</Button>
            <a
              href={getDiagnosticBriefPdfUrl(id)}
              target="_blank"
              rel="noreferrer"
              className="inline-flex h-9 items-center rounded-[var(--radius-md)] border border-[var(--border-default)] bg-[var(--bg-card)] px-3 text-sm font-medium text-[var(--text-secondary)] hover:bg-slate-50"
            >
              Download PDF
            </a>
            <Button onClick={() => handleRerun(false)} disabled={rerunning}>{rerunning ? "Refreshing..." : "Refresh brief"}</Button>
            <Button onClick={handleDelete} disabled={deleting} className="border-rose-200 text-rose-600 hover:bg-rose-50">Delete</Button>
          </div>
          {shareUrl ? <p className="mt-2 break-all">Share URL: {shareUrl}</p> : null}
        </Card>
      </main>

      {showMobileFooter && (
        <div className="fixed inset-x-0 bottom-0 z-50 border-t border-[var(--border-default)] bg-[var(--bg-card)]/95 p-3 backdrop-blur lg:hidden">
          <div className="mx-auto flex max-w-6xl items-center justify-between gap-2">
            <div className="min-w-0">
              <p className="truncate text-xs font-semibold">{result.business_name}</p>
              <p className="truncate text-[11px] text-[var(--text-muted)]">{opportunityBand || "Opportunity: —"}</p>
            </div>
            <div className="flex gap-2">
              <Button onClick={() => setAddListOpen(true)}>Add to Pipeline</Button>
              <Button onClick={() => setOutreachOpen(true)} className="border-[var(--border-default)]">Log Outreach</Button>
            </div>
          </div>
        </div>
      )}

      {addListOpen && (
        <ListPickerModal
          open={addListOpen}
          title="Add this prospect to pipeline"
          lists={lists}
          busy={addingToList}
          onClose={() => {
            if (addingToList) return;
            setAddListOpen(false);
          }}
          onConfirm={handleAddToList}
        />
      )}

      {outreachOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
          <div className="w-full max-w-md rounded-[var(--radius-md)] border border-[var(--border-default)] bg-[var(--bg-card)] p-4">
            <h3 className="text-sm font-semibold">Log Outreach</h3>
            <p className="mt-1 text-xs text-[var(--text-muted)]">Record latest rep action and outcome.</p>

            <div className="mt-3 space-y-2">
              <label className="text-xs font-medium text-[var(--text-muted)]">Outcome</label>
              <select
                className="w-full rounded-md border border-[var(--border-default)] bg-white px-2 py-2 text-sm"
                value={outreachAction}
                onChange={(e) => setOutreachAction(e.target.value as OutreachAction)}
              >
                <option>Called</option>
                <option>Left VM</option>
                <option>Emailed</option>
                <option>No Answer</option>
              </select>

              <label className="text-xs font-medium text-[var(--text-muted)]">Notes</label>
              <textarea
                className="h-24 w-full rounded-md border border-[var(--border-default)] bg-white px-2 py-2 text-sm"
                value={outreachNote}
                onChange={(e) => setOutreachNote(e.target.value)}
                placeholder="Optional call notes"
              />
            </div>

            <div className="mt-4 flex justify-end gap-2">
              <Button onClick={() => setOutreachOpen(false)} className="border-[var(--border-default)]">Cancel</Button>
              <Button onClick={handleLogOutreach} disabled={loggingOutreach}>{loggingOutreach ? "Saving..." : "Save"}</Button>
            </div>
          </div>
        </div>
      )}

      {activeCtaRow && ctaDrawerOpen && (
        <div className="fixed inset-y-0 right-0 z-50 w-full max-w-md border-l border-[var(--border-default)] bg-[var(--bg-card)] shadow-[0_20px_60px_rgba(23,20,17,0.18)]">
          <div className="flex h-full flex-col">
            <div className="border-b border-[var(--border-default)] px-4 py-4">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <p className="text-xs font-semibold uppercase tracking-[0.16em] text-[var(--text-muted)]">CTA Drill-Down</p>
                  <h3 className="mt-1 text-lg font-semibold text-[var(--text-primary)]">{activeCtaRow.type}</h3>
                  <p className="mt-1 text-sm text-[var(--text-secondary)]">
                    {activeCtaRow.count} instances found across {activeCtaPageCount || 0} matched page{activeCtaPageCount === 1 ? "" : "s"}.
                  </p>
                </div>
                <button
                  type="button"
                  onClick={() => setCtaDrawerOpen(false)}
                  className="inline-flex h-8 w-8 items-center justify-center rounded-full border border-[var(--border-default)] text-[var(--text-secondary)] hover:bg-slate-50"
                  aria-label="Close CTA drawer"
                >
                  ×
                </button>
              </div>
            </div>

            <div className="flex-1 space-y-4 overflow-y-auto px-4 py-4">
              <div className="rounded-[22px] border border-black/6 bg-[#fbfaf7] p-4">
                <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-[var(--text-muted)]">Active focus</p>
                <div className="mt-2 flex flex-wrap items-center gap-2">
                  <span className={`rounded px-2 py-1 text-xs font-semibold ${activeCtaRow.type === "Book" ? "bg-blue-100 text-blue-700" : activeCtaRow.type === "Schedule" ? "bg-emerald-100 text-emerald-700" : activeCtaRow.type === "Contact" ? "bg-amber-100 text-amber-800" : "bg-slate-200 text-slate-700"}`}>
                    {activeCtaRow.type}
                  </span>
                  <span className="text-xs text-[var(--text-secondary)]">
                    {Number(activeCtaRow.clickableCount || 0)} clickable
                  </span>
                </div>
              </div>

              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.16em] text-[var(--text-muted)]">Matched pages</p>
                <div className="mt-2 space-y-2">
                  {activeCtaPages.length ? activeCtaPages.map((page, idx) => (
                    <div key={`${page}-${idx}`} className="rounded-[20px] border border-black/6 bg-white px-3 py-3">
                      <p className="text-sm font-medium text-[var(--text-primary)]">{urlTitle(page)}</p>
                      <p className="mt-1 break-all font-mono text-[11px] text-[var(--text-muted)]">{page}</p>
                    </div>
                  )) : (
                    <div className="rounded-[20px] border border-black/6 bg-white px-3 py-3 text-sm text-[var(--text-secondary)]">
                      No explicit page-level matches were captured for this CTA in this run.
                    </div>
                  )}
                </div>
              </div>
            </div>

            <div className="border-t border-[var(--border-default)] px-4 py-3">
              <div className="flex gap-2">
                <Button
                  onClick={() => {
                    setActiveCtaType(null);
                    setCtaDrawerOpen(false);
                  }}
                  className="border-[var(--border-default)] bg-[var(--bg-card)] text-[var(--text-secondary)]"
                >
                  Clear filter
                </Button>
                <Button onClick={() => setCtaDrawerOpen(false)}>Keep highlights</Button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function TabButton({ active, onClick, children }: { active: boolean; onClick: () => void; children: React.ReactNode }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`rounded-full border px-3 py-1.5 text-xs font-semibold ${active
        ? "border-slate-900 bg-slate-900 text-white"
        : "border-[var(--border-default)] bg-[var(--bg-card)] text-[var(--text-secondary)] hover:bg-slate-50"}`}
    >
      {children}
    </button>
  );
}

function QuickStat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-md border border-slate-200 bg-slate-50 px-2 py-1.5">
      <p className="text-[10px] uppercase tracking-wide text-slate-500">{label}</p>
      <p className="truncate text-xs font-semibold text-slate-900">{value || "—"}</p>
    </div>
  );
}

function MetricCard({ label, value, status, tone = "default" }: { label: string; value: string; status?: string; tone?: "default" | "danger" }) {
  const toneClass = tone === "danger" ? "border-red-200 bg-red-50" : "border-[var(--border-default)] bg-[var(--bg-card)]";
  return (
    <div className={`rounded-[var(--radius-md)] border p-3 ${toneClass}`}>
      <p className="text-xs uppercase tracking-wide text-[var(--text-muted)]">{label}</p>
      <p className="mt-1 text-base font-semibold text-[var(--text-primary)]">{value || "—"}</p>
      {status ? <p className="mt-1 text-xs text-[var(--text-secondary)]">{status}</p> : null}
    </div>
  );
}

function CaptureReadinessRow({ label, status, icon, note }: { label: string; status: string; icon: string; note: string }) {
  const ok = icon === "✓";
  return (
    <div className="grid grid-cols-[24px_1fr] items-start gap-2 rounded border border-slate-200 px-2 py-1.5">
      <span className={`mt-0.5 inline-flex h-5 w-5 items-center justify-center rounded-full text-xs font-semibold ${ok ? "bg-emerald-100 text-emerald-700" : icon === "✕" ? "bg-red-100 text-red-700" : "bg-slate-100 text-slate-500"}`}>{icon}</span>
      <div>
        <p className="text-xs font-semibold text-slate-900">{label}</p>
        <p className="text-xs text-slate-700">{status}</p>
        <p className="text-[11px] text-slate-500">{note}</p>
      </div>
    </div>
  );
}

function MetaChip({ label, value, tone = "default" }: { label: string; value: string; tone?: "default" | "badge" }) {
  return (
    <span className={`inline-flex items-center gap-1 rounded border px-2 py-1 text-xs ${tone === "badge" ? "border-slate-300 bg-white font-semibold text-slate-800" : "border-slate-200 bg-white text-slate-700"}`}>
      <span className="text-slate-500">{label}:</span>
      <span>{value}</span>
    </span>
  );
}
