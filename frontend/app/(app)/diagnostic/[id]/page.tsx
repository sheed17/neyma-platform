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
import { useAuth } from "@/lib/auth";
import { clientFacingAppError } from "@/lib/present";
import type { DiagnosticResponse, ProspectList, ServiceIntelligence } from "@/lib/types";
import { computeVerdict } from "@/lib/verdict";
import { generateObservationBullets, generateOpportunityFocus, generateWhyNow } from "@/lib/pitch";
import { cleanWebsiteDisplay, isSchemaRelated } from "@/lib/present";
import { getModeledUpsideDisplay } from "@/lib/revenueDisplay";
import Button from "@/app/components/ui/Button";
import { Card } from "@/app/components/ui/Card";
import ListPickerModal from "@/app/components/ListPickerModal";

type SignalTab = "overview" | "competitors" | "siteGaps" | "fullAudit";
type OutreachAction = "Called" | "Left VM" | "Emailed" | "No Answer";
const OUTREACH_ACTIONS: OutreachAction[] = ["Called", "Left VM", "Emailed", "No Answer"];

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

type CaptureVerificationSignalLike = {
  status?: string | null;
  value?: string | null;
  confidence?: string | null;
  observed_pages?: string[] | null;
  evidence?: string[] | null;
};

type CaptureReadinessState = {
  status: string;
  icon: string;
  note: string;
  confidence?: string;
  observedPages?: string[];
  evidence?: string;
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

function titleCase(value: string | null | undefined): string {
  const raw = String(value || "").trim();
  if (!raw) return "";
  return raw.replace(/[_-]+/g, " ").replace(/\b\w/g, (m) => m.toUpperCase());
}

function buildDiagnosticHref(
  diagnosticId: number,
  context: { from: string; scanId: string; listId: string },
  extra?: Record<string, string | number | null | undefined>,
): string {
  const params = new URLSearchParams();
  if (context.from) params.set("from", context.from);
  if (context.scanId) params.set("scanId", context.scanId);
  if (context.listId) params.set("listId", context.listId);
  Object.entries(extra || {}).forEach(([key, value]) => {
    if (value !== null && value !== undefined && String(value) !== "") {
      params.set(key, String(value));
    }
  });
  const query = params.toString();
  return `/diagnostic/${diagnosticId}${query ? `?${query}` : ""}`;
}

function captureMeta(signal: CaptureVerificationSignalLike | null | undefined): {
  confidence?: string;
  observedPages?: string[];
  evidence?: string;
} {
  const confidence = titleCase(signal?.confidence || undefined) || undefined;
  const observedPages = (signal?.observed_pages || []).filter(Boolean).slice(0, 3);
  const evidence = (signal?.evidence || []).find((item) => String(item || "").trim());
  return {
    confidence,
    observedPages: observedPages.length ? observedPages : undefined,
    evidence: evidence ? simplifyCaptureEvidence(String(evidence)) : undefined,
  };
}

function capturePageLabel(page: string): string {
  const raw = String(page || "").trim();
  if (!raw || raw === "/") return "Homepage";
  const normalized = raw.replace(/\/+$/, "").toLowerCase();
  if (!normalized || normalized === "") return "Homepage";
  if (normalized.includes("contact")) return "Contact page";
  if (normalized.includes("appoint") || normalized.includes("schedule") || normalized.includes("book")) return "Appointment page";
  return urlTitle(raw);
}

function simplifyCaptureEvidence(value: string): string {
  const text = String(value || "").trim();
  if (!text) return "";
  const lower = text.toLowerCase();
  if (lower.includes("form html") || lower.includes("form plugin")) return "Submit-capable form found.";
  if (lower.includes("appointment request cta")) return "Appointment request action found.";
  if (lower.includes("strong scheduling cta")) return "Booking or scheduling call-to-action found.";
  if (lower.includes("call-to-schedule")) return "Phone-only scheduling prompt found.";
  if (lower.includes("time-selection")) return "Time-selection controls found.";
  if (lower.includes("flow step markers")) return "Step-by-step appointment flow found.";
  if (lower.includes("booking-oriented link target")) return "Likely booking path found from the homepage.";
  if (lower.includes("known scheduling platform")) return "Scheduling platform detected.";
  if (lower.includes("booking cta observed")) return "Booking action found on a scanned page.";
  return text;
}

function verificationSummaryLabel(usesRenderedHomepage: boolean, followupCount: number): string {
  if (usesRenderedHomepage && followupCount > 0) {
    return `Rendered homepage + ${followupCount} follow-up page${followupCount === 1 ? "" : "s"}`;
  }
  if (usesRenderedHomepage) return "Rendered homepage check";
  if (followupCount > 0) return `Homepage + ${followupCount} follow-up page${followupCount === 1 ? "" : "s"}`;
  return "Homepage check only";
}

function flowOutcomeLabel(label: string): string {
  const normalized = label.toLowerCase();
  if (normalized.includes("detected on scanned pages")) return "Scheduling CTA found";
  if (normalized.includes("online self-scheduling")) return "Online booking verified";
  if (normalized.includes("appointment request")) return "Request flow verified";
  if (normalized.includes("phone-only")) return "Phone-only path verified";
  if (normalized.includes("booking flow not verified")) return "Booking flow not verified";
  if (normalized.includes("verified form detected")) return "Contact form verified";
  if (normalized.includes("verified absent")) return "Contact form not found";
  if (normalized.includes("not verified")) return "Not verified";
  return label;
}

function schedulingCtaState(
  signal: CaptureVerificationSignalLike | null | undefined,
  detected: boolean | null | undefined,
): CaptureReadinessState {
  const meta = captureMeta(signal);
  if (signal?.status === "detected" || detected === true) {
    return {
      status: "Scheduling CTA found",
      icon: "✓",
      note: "Neyma found a booking or scheduling call-to-action on scanned pages.",
      ...meta,
    };
  }
  return {
    status: "Not verified in this scan",
    icon: "—",
    note: "No scheduling call-to-action was verified on the pages checked.",
    ...meta,
  };
}

function bookingFlowState(signal: CaptureVerificationSignalLike | null | undefined): CaptureReadinessState {
  const meta = captureMeta(signal);
  const value = String(signal?.value || "").trim().toLowerCase();
  if (value === "online_self_scheduling") {
    return {
      status: "Online self-scheduling verified",
      icon: "✓",
      note: "Neyma verified a booking path where patients can continue online.",
      ...meta,
    };
  }
  if (value === "appointment_request_form") {
    return {
      status: "Appointment request flow detected",
      icon: "✓",
      note: "Neyma verified an appointment request flow, but not full self-scheduling.",
      ...meta,
    };
  }
  if (value === "call_only") {
    return {
      status: "Phone-only scheduling path verified",
      icon: "✕",
      note: "Neyma verified a path that sends patients to call instead of booking online.",
      ...meta,
    };
  }
  return {
    status: "Booking flow not verified in this scan",
    icon: "—",
    note: "A scheduling path may exist, but Neyma did not verify the full booking flow in pages checked.",
    ...meta,
  };
}

function contactFormState(
  signal: CaptureVerificationSignalLike | null | undefined,
  detected: boolean | null | undefined,
  formStructure: unknown,
): CaptureReadinessState {
  const meta = captureMeta(signal);
  if (signal?.status === "detected" || detected === true) {
    return {
      status: "Verified form detected",
      icon: "✓",
      note: formStructure
        ? `Neyma verified a submit-capable form (${String(formStructure)}).`
        : "Neyma verified a submit-capable form.",
      ...meta,
    };
  }
  if (signal?.status === "not_detected" || detected === false) {
    return {
      status: "Verified absent on scanned pages",
      icon: "✕",
      note: formStructure
        ? `Neyma checked the scanned pages and did not verify a submit-capable form (${String(formStructure)}).`
        : "Neyma checked the scanned pages and did not verify a submit-capable form.",
      ...meta,
    };
  }
  return {
    status: "Not verified in this scan",
    icon: "—",
    note: formStructure
      ? `A contact path may exist, but Neyma did not verify a submit-capable form (${String(formStructure)}).`
      : "A contact path may exist, but Neyma did not verify a submit-capable form in the pages checked.",
    ...meta,
  };
}

function crawlMethodLabel(raw: unknown): string {
  const v = String(raw || "").trim().toLowerCase();
  if (v === "hybrid_playwright_landing_only") return "Enhanced homepage review";
  if (v === "playwright") return "Enhanced page review";
  if (v === "requests") return "Standard page review";
  if (v === "requests_fallback_playwright_unavailable") return "Standard page review";
  return "Page check";
}

function coverageWarningLabel(raw: unknown): string {
  const text = String(raw || "").trim();
  if (!text) return "Some page checks were limited during this review.";
  return text
    .replace(/playwright/gi, "enhanced page checks")
    .replace(/static-only/gi, "standard")
    .replace(/crawling/gi, "checking pages")
    .replace(/crawl/gi, "page check")
    .replace(/rendered page/gi, "enhanced page")
    .replace(/scanned pages/gi, "pages checked");
}

function formatMarketContextValue(label: "density" | "visibility" | "servicePages" | "form" | "website", value: unknown): string {
  if (label === "density") {
    const text = String(value || "—").trim();
    if (!text || text === "—") return "—";
    return text.charAt(0).toUpperCase() + text.slice(1).toLowerCase();
  }
  if (label === "visibility") {
    const text = String(value || "—").trim().toLowerCase();
    if (!text || text === "—") return "—";
    if (text === "low") return "Limited";
    if (text === "medium") return "Moderate";
    if (text === "high") return "Strong";
    return text.charAt(0).toUpperCase() + text.slice(1);
  }
  if (label === "servicePages") {
    const count = Number(value || 0);
    return Number.isFinite(count) ? `${count}` : "—";
  }
  if (label === "form") {
    const text = String(value || "").trim().toLowerCase();
    if (!text || text === "unknown") return "Not confirmed";
    if (text === "single_step") return "Single-step form";
    if (text === "multi_step") return "Multi-step form";
    return text.replace(/[_-]+/g, " ").replace(/\b\w/g, (m) => m.toUpperCase());
  }
  if (label === "website") {
    return value ? "Website available" : "No website";
  }
  return String(value || "—");
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
  const { access } = useAuth();
  const id = Number(params.id);
  const invalidId = !id || Number.isNaN(id);
  const from = String(searchParams.get("from") || "").toLowerCase();
  const scanId = searchParams.get("scanId") || "";
  const listId = searchParams.get("listId") || "";
  const routeContext = { from, scanId, listId };

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
  const canUseWorkspace = access?.can_use.workspace !== false;
  const canSave = access?.can_use.save !== false;
  const canShare = access?.can_use.share !== false;
  const canExport = access?.can_use.export !== false;

  const headerRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (invalidId) return;
    getDiagnostic(id)
      .then(setResult)
      .catch((e) => setError(clientFacingAppError(e instanceof Error ? e.message : "Failed to load brief", "We couldn't load this brief right now. Please try again.")))
      .finally(() => setLoading(false));
  }, [id, invalidId]);

  useEffect(() => {
    if (!canSave) {
      setLists([]);
      return;
    }
    let cancelled = false;
    async function loadLists() {
      const data = await getProspectLists().catch(() => ({ items: [] }));
      if (!cancelled) setLists(data.items);
    }
    void loadLists();
    return () => {
      cancelled = true;
    };
  }, [canSave]);

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

  function handleBack() {
    if (typeof window !== "undefined" && window.history.length > 1) {
      router.back();
      return;
    }
    router.push(navContext.backHref);
  }

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
          router.replace(buildDiagnosticHref(Number(job.diagnostic_id), routeContext, { refresh: Date.now() }));
          setNotice("Results refreshed.");
          setRerunning(false);
          return;
        }
        router.push(buildDiagnosticHref(Number(job.diagnostic_id), routeContext));
      } else {
        setError(clientFacingAppError(job.error || "Re-run failed", "We couldn't refresh this brief right now. Please try again."));
        setRerunning(false);
      }
    } catch (err) {
      setError(clientFacingAppError(err instanceof Error ? err.message : "Re-run failed", "We couldn't refresh this brief right now. Please try again."));
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
      setError(clientFacingAppError(err instanceof Error ? err.message : "Failed to create share link", "We couldn't create a share link right now. Please try again."));
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
      setError(clientFacingAppError(err instanceof Error ? err.message : "Failed to add to list", "We couldn't add this brief to a list right now. Please try again."));
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
      setError(clientFacingAppError(err instanceof Error ? err.message : "Failed to log outreach", "We couldn't save that outreach update right now. Please try again."));
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
  const captureVerification = conv.capture_verification || null;
  const schedulingCta = captureVerification?.scheduling_cta || null;
  const bookingFlow = captureVerification?.booking_flow || null;
  const contactFormVerification = captureVerification?.contact_form || null;
  const followupPagesChecked = Array.isArray(captureVerification?.followup_pages_checked)
    ? captureVerification.followup_pages_checked.filter(Boolean)
    : [];
  const homepagePage = String(captureVerification?.homepage_page || "/");
  const reviewIntel = b?.review_intelligence || result.review_intelligence || {};
  const svc: Partial<ServiceIntelligence> = result.service_intelligence || {};
  const spaV2 = (b?.service_page_analysis?.v2 as Record<string, unknown> | undefined)
    || (svc.service_page_analysis_v2 as Record<string, unknown> | undefined)
    || {};

  const verdict = computeVerdict(result);
  const websiteHref = result.website
    ? (result.website.startsWith("http://") || result.website.startsWith("https://") ? result.website : `https://${result.website}`)
    : null;
  const websiteLabel = cleanWebsiteDisplay(result.website);
  const hasWebsite = Boolean(String(result.website || "").trim());
  const phoneLabel = result.phone ? String(result.phone).trim() : "";
  const phoneHref = phoneLabel ? `tel:${phoneLabel}` : null;
  const paidStatus = paidStatusLabel(String(ds?.google_ads_line || result.paid_status || "Unknown"));
  const reviewCount = firstNumber(mp?.reviews) || firstNumber(result.review_position) || 0;
  const rating = firstNumber(result.review_position);
  const localAvgReviews = firstNumber(mp?.local_avg) || firstNumber((result.evidence || []).find((e) => String(e.label || "").toLowerCase().includes("review"))?.value) || 0;
  const opportunityProfile =
    ed?.opportunity_profile && typeof ed.opportunity_profile === "object"
      ? ed.opportunity_profile
      : null;
  const opportunitySignal = String(opportunityProfile?.label || verdict.label || "Opportunity Signal");
  const modeledUpside = getModeledUpsideDisplay(result);
  const topGap = String(verdict.topGap || csg?.service || sg?.service || "—");
  const preferredFocus = modeledUpside.serviceContext && modeledUpside.serviceContext !== "Primary gap"
    ? modeledUpside.serviceContext
    : "";
  const summaryFocus = generateOpportunityFocus(result, preferredFocus);
  const noWebsiteConfirmed = [
    "No website was detected for this business.",
    (reviewCount > 0 || rating != null || localAvgReviews > 0)
      ? `Listing data shows ${reviewCount || "0"} reviews${rating ? ` at ${rating.toFixed(1)} stars` : ""}${localAvgReviews > 0 ? ` versus a local average of ${localAvgReviews}` : ""}.`
      : "",
    String(mp?.market_density || result.market_density || "").trim() && String(mp?.market_density || result.market_density || "—") !== "—"
      ? `Market density is currently classified as ${String(mp?.market_density || result.market_density || "").toLowerCase()}.`
      : "",
    paidStatus !== "Unknown"
      ? `Paid demand signal: ${paidStatus === "Active" ? "active ads detected from off-site signals." : "no active ads detected."}`
      : "",
    phoneLabel ? "Phone contact is present in listing data." : "",
  ].filter(Boolean);

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

  const observations = generateObservationBullets(result, summaryFocus).slice(0, 3);
  const whyNow = generateWhyNow(result, summaryFocus);
  const schedulingState = schedulingCtaState(schedulingCta, conv.scheduling_cta_detected as boolean | null | undefined);
  const bookingState = bookingFlowState(bookingFlow);
  const contactState = contactFormState(
    contactFormVerification,
    conv.contact_form as boolean | null | undefined,
    convStruct.form_single_or_multi_step,
  );
  const verificationOutcomeSummary = [bookingState.status, contactState.icon === "✓" ? "Contact form verified" : ""]
    .filter(Boolean)
    .join(" • ");
  const verificationSummary = verificationSummaryLabel(usesPlaywrightPath, followupPagesChecked.length);
  const uniqueFollowupPagesChecked = Array.from(new Set(followupPagesChecked.filter(Boolean)));

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
  const geoCoverage = pagesCrawled > 0
    ? `${pct(geoIntentPages, pagesCrawled)}% · ${geoIntentPages}/${pagesCrawled} pages`
    : geoIntentPages > 0
      ? `${geoIntentPages} page${geoIntentPages === 1 ? "" : "s"}`
      : "—";
  const whyNowItems = (observations.length ? observations : [whyNow]).slice(0, 3);
  const heroCompetitors = competitorRows.slice(0, 4);
  const recommendationSummary = hasWebsite
    ? `${result.business_name} is worth pursuing where ${summaryFocus.toLowerCase()} overlaps with visible demand and a fixable capture gap.`
    : `${result.business_name} is worth pursuing where listing demand signals are already present and the competitive story is still favorable.`;
  const recommendationDetail = modeledUpside.mode === "range"
    ? `${modeledUpside.value} in modeled annual upside suggests this brief is strong enough to move into outreach.`
    : `Use the brief to frame the outreach around ${summaryFocus.toLowerCase()} before opening the full audit.`;
  const heroVerificationItems = [
    { label: "Scheduling CTA", status: schedulingState.status, complete: schedulingState.icon === "✓" },
    { label: "Booking flow", status: bookingState.status, complete: bookingState.icon === "✓" },
    { label: "Contact form", status: contactState.status, complete: contactState.icon === "✓" },
    { label: "Phone prominent", status: boolLabel(conv.phone_prominent as boolean | null | undefined), complete: Boolean(conv.phone_prominent) },
  ];

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

  return (
    <div className="mx-auto max-w-[var(--max-content)] px-3 pb-24 sm:px-4 lg:px-6">
      <main className="space-y-5 lg:space-y-6">
        <p className="text-xs font-medium uppercase tracking-[0.08em] text-[var(--text-muted)] sm:text-sm sm:normal-case sm:tracking-normal">
          <Link href={navContext.backHref} className="app-link">{navContext.crumbLabel}</Link>
          <span> → </span>
          <span>{result.business_name}</span>
        </p>

        {(notice || error) && (
          <Card className="rounded-[22px] border border-[var(--border-default)] p-4">
            {notice && (
              <div className="flex flex-wrap items-center gap-2 text-sm text-emerald-700">
                <span>{notice}</span>
                {noticeHref && noticeCta ? <Link href={noticeHref} className="app-link font-medium">{noticeCta}</Link> : null}
              </div>
            )}
            {error && <p className="text-sm text-red-600">{error}</p>}
          </Card>
        )}

        <div ref={headerRef}>
          <Card className="rounded-[28px] border border-[var(--border-default)] bg-[var(--bg-card)] p-5 sm:p-6 lg:p-7">
            <div className="space-y-5 lg:space-y-6">
              <div className="flex flex-wrap items-start justify-between gap-4">
                <div className="min-w-0 flex-1 space-y-4">
                  <button
                    type="button"
                    onClick={handleBack}
                    className="inline-flex items-center gap-2 rounded-full border border-[var(--border-default)] bg-[var(--surface)] px-3 py-1 text-xs font-medium text-[var(--text-secondary)] transition hover:brightness-95"
                  >
                    <span aria-hidden="true">←</span>
                    <span>{navContext.backLabel}</span>
                  </button>

                  <div className="flex flex-wrap items-center gap-2">
                    <HeroPill tone="green">Qualified Lead</HeroPill>
                    <HeroPill tone="neutral">{opportunitySignal}</HeroPill>
                  </div>

                  <div className="space-y-2">
                    <h1 className="page-title text-[clamp(2rem,4vw,2.75rem)]">{result.business_name}</h1>
                    <div className="flex flex-wrap items-center gap-x-2 gap-y-1 text-sm text-[var(--text-secondary)]">
                      <span>{result.city}{result.state ? `, ${result.state}` : ""}</span>
                      {(websiteHref || phoneHref) ? <span>•</span> : null}
                      {websiteHref ? (
                        <a href={websiteHref} target="_blank" rel="noreferrer" className="font-medium text-[var(--text-primary)] hover:underline">
                          {websiteLabel || websiteHref}
                        </a>
                      ) : <span>Website not available</span>}
                      {phoneHref ? (
                        <>
                          <span>•</span>
                          <a href={phoneHref} className="font-medium text-[var(--text-primary)] hover:underline">
                            {phoneLabel}
                          </a>
                        </>
                      ) : null}
                    </div>
                  </div>
                </div>

                <div className="flex w-full flex-col gap-2 sm:w-auto sm:flex-row sm:flex-wrap sm:items-center">
                  {canSave ? (
                    <Button variant="primary" onClick={() => setAddListOpen(true)} className="w-full sm:w-auto">Add to Pipeline</Button>
                  ) : null}
                  {canUseWorkspace ? (
                    <Button onClick={() => setOutreachOpen(true)} className="w-full border-[var(--border-default)] sm:w-auto">Log Outreach</Button>
                  ) : null}
                </div>
              </div>

              {!canUseWorkspace ? (
                <div className="rounded-[20px] border border-[var(--border-default)] bg-[var(--muted)] px-4 py-3 text-sm text-[var(--text-secondary)]">
                  Create a <span className="font-semibold text-[var(--text-primary)]">free account</span> to <span className="font-semibold text-[var(--text-primary)]">save this brief</span>, log outreach, share it, or export the PDF.
                </div>
              ) : null}

              <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
                <HeroSignalCell label="Reviews count" value={reviewCount ? String(reviewCount) : "—"} />
                <HeroSignalCell
                  label="Review delta vs nearest"
                  value={nearestReviews > 0 ? `${reviewDelta >= 0 ? "+" : ""}${reviewDelta} vs nearest` : "—"}
                />
                <HeroSignalCell label="Paid ads" value={paidStatus} />
                <HeroSignalCell label="Market density" value={String(mp?.market_density || result.market_density || "—")} />
                <HeroSignalCell label="Geo intent coverage" value={geoCoverage} />
              </div>

              <div className="flex flex-col gap-3 rounded-[22px] border border-[var(--border-default)] bg-[var(--surface)] px-4 py-4 lg:flex-row lg:items-center lg:justify-between">
                <div className="space-y-1">
                  <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-[var(--text-muted)]">Top Gap</p>
                  <p className="text-base font-medium text-[var(--text-primary)]">{topGap}</p>
                </div>
                <span className="inline-flex items-center rounded-full border border-[var(--border-default)] bg-white px-3 py-1.5 text-xs font-medium text-[var(--text-secondary)]">
                  {summaryFocus}
                </span>
              </div>

              <div className="grid gap-4 lg:grid-cols-2">
                <section className="rounded-[22px] border border-[var(--border-default)] bg-[var(--surface)] p-5">
                  <div className="flex items-center justify-between gap-3">
                    <p className="section-kicker">Why now</p>
                    <span className="text-xs text-[var(--text-secondary)]">{whyNow}</span>
                  </div>
                  <ul className="mt-3 space-y-3">
                    {whyNowItems.map((item) => (
                      <li key={item} className="flex items-start gap-3">
                        <span className={`mt-1.5 h-2.5 w-2.5 rounded-full ${rationaleTone(item) === "urgent" ? "bg-amber-500" : "bg-emerald-500"}`} />
                        <span className="text-sm leading-6 text-[var(--text-primary)]">{item}</span>
                      </li>
                    ))}
                  </ul>
                </section>

                <section className="rounded-[22px] border border-[var(--border-default)] bg-[var(--surface)] p-5">
                  <div className="flex items-center justify-between gap-3">
                    <p className="section-kicker">Competitors nearby</p>
                    <span className="text-xs text-[var(--text-secondary)]">
                      {nearestReviews > 0 ? `${reviewDelta >= 0 ? "+" : ""}${reviewDelta} vs nearest` : "Review context"}
                    </span>
                  </div>

                  <div className="mt-3 space-y-3">
                    {heroCompetitors.map((row, idx) => (
                      <div
                        key={`${row.name}-${idx}`}
                        className="grid grid-cols-[minmax(0,1.4fr)_92px_72px] items-center gap-3 text-sm"
                      >
                        <div className="min-w-0">
                          <p className="truncate font-medium text-[var(--text-primary)]">
                            {row.name}{row.isYou ? " (You)" : ""}
                          </p>
                          <div className="mt-1 h-2.5 rounded-full bg-white">
                            <div
                              className={`h-2.5 rounded-full ${row.isYou ? "bg-[var(--text-primary)]" : "bg-[var(--primary)]/55"}`}
                              style={{ width: `${pct(row.reviews, maxReviews)}%` }}
                            />
                          </div>
                        </div>
                        <p className="text-right font-medium tabular-nums text-[var(--text-primary)]">{row.reviews || "—"}</p>
                        <p className="text-right text-[var(--text-secondary)]">{row.distance}</p>
                      </div>
                    ))}
                  </div>
                </section>
              </div>

              <div className="grid gap-4 lg:grid-cols-2">
                <section className="rounded-[22px] border border-[var(--border-default)] bg-[var(--surface)] p-5">
                  <p className="section-kicker">Capture verification</p>
                  <div className="mt-3 space-y-3">
                    {heroVerificationItems.map((item) => (
                      <VerificationItem key={item.label} label={item.label} status={item.status} complete={item.complete} />
                    ))}
                  </div>
                </section>

                <section className="rounded-[22px] border border-[var(--border-default)] bg-[var(--surface)] p-5">
                  <p className="section-kicker">Recommendation</p>
                  <p className="mt-3 text-sm leading-6 text-[var(--text-primary)]">{recommendationSummary}</p>
                  <p className="mt-2 text-sm leading-6 text-[var(--text-secondary)]">{recommendationDetail}</p>
                  <div className="mt-4 flex flex-wrap gap-2">
                    <Button onClick={() => setOutreachOpen(true)}>Draft outreach</Button>
                    <Button onClick={() => setActiveTab("fullAudit")} className="border-[var(--border-default)]">
                      Full audit
                    </Button>
                  </div>
                </section>
              </div>
            </div>
          </Card>
        </div>

        <Card className="rounded-[28px] border border-[var(--border-default)] p-4 sm:p-5 lg:p-6">
          <div className="mb-4 flex flex-wrap gap-2">
            <TabButton active={activeTab === "overview"} onClick={() => setActiveTab("overview")}>Overview</TabButton>
            <TabButton active={activeTab === "competitors"} onClick={() => setActiveTab("competitors")}>Competitors</TabButton>
            <TabButton active={activeTab === "siteGaps"} onClick={() => setActiveTab("siteGaps")}>Site Gaps</TabButton>
            <TabButton active={activeTab === "fullAudit"} onClick={() => setActiveTab("fullAudit")}>Full Audit</TabButton>
          </div>
          {activeCtaRow && (
            <div className="mb-4 flex flex-wrap items-center gap-2 rounded-[22px] border border-[var(--border-default)] bg-[var(--surface)] px-4 py-3">
              <span className="text-xs font-semibold uppercase tracking-[0.16em] text-[var(--text-muted)]">Active filter</span>
              <span className="inline-flex items-center gap-2 rounded-full border border-[var(--border-default)] bg-white px-3 py-1.5 text-xs font-medium text-[var(--text-primary)]">
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
            <div className="space-y-4 text-sm">
              <Card className="rounded-[22px] border border-[var(--border-default)] p-5">
                <p className="section-kicker">Overview</p>
                <p className="mt-2 text-sm text-[var(--text-secondary)]">
                  Core lead context now lives above the fold. Use the tabs to go deeper into nearby competitors, verified site gaps, and the full audit trail.
                </p>
                {modeledUpside.mode === "range" ? (
                  <div className="mt-4 rounded-[18px] border border-[var(--border-default)] bg-[var(--surface)] px-4 py-4 text-xs text-[var(--text-secondary)]">
                    <p><span className="text-[var(--text-muted)]">Annual upside:</span> <span className="font-semibold text-[var(--text-primary)]">{modeledUpside.value}</span></p>
                    <p className="mt-1">{modeledUpside.context}</p>
                  </div>
                ) : null}
                {!hasWebsite ? (
                  <div className="mt-4 rounded-[18px] border border-[var(--border-default)] bg-[var(--surface)] px-4 py-4">
                    <h3 className="text-sm font-semibold text-[var(--text-primary)]">What Neyma can confirm without a website</h3>
                    <ul className="mt-2 list-disc space-y-1 pl-5 text-[var(--text-secondary)]">
                      {noWebsiteConfirmed.map((item, i) => <li key={i}>{item}</li>)}
                    </ul>
                  </div>
                ) : null}
              </Card>
            </div>
          )}

          {activeTab === "competitors" && (
            <div className="space-y-4 text-sm">
              <div className="grid gap-3 md:grid-cols-3">
                <MetricCard label="Market Density" value={String(mp?.market_density || result.market_density || "—")} />
                <MetricCard label="Review Delta vs Nearest" value={nearestReviews > 0 ? `${reviewDelta >= 0 ? "+" : ""}${reviewDelta}` : "—"} />
                <MetricCard label="Local Avg Reviews" value={localAvgReviews ? String(localAvgReviews) : "—"} />
              </div>

              <div className="overflow-x-auto rounded-[22px] border border-[var(--border-default)]">
                <table className="min-w-full text-left text-xs sm:text-sm">
                  <thead className="bg-[var(--surface)] text-[var(--text-secondary)]">
                    <tr>
                      <th className="px-3 py-2 font-medium">Practice</th>
                      <th className="px-3 py-2 font-medium">Reviews</th>
                      <th className="px-3 py-2 font-medium">Distance</th>
                      <th className="px-3 py-2 font-medium">Notes</th>
                    </tr>
                  </thead>
                  <tbody>
                    {competitorRows.map((row, idx) => (
                      <tr key={`${row.name}-${idx}`} className={`border-t border-[var(--border-default)] ${row.isYou ? "bg-[var(--surface)]" : "bg-white"}`}>
                        <td className="px-3 py-2 font-medium text-[var(--text-primary)]">{row.name}{row.isYou ? " (You)" : ""}</td>
                        <td className="px-3 py-2">
                          <div className="flex items-center gap-2">
                            <span className="w-10 text-right tabular-nums">{row.reviews || "—"}</span>
                            <div className="h-2 w-28 rounded bg-slate-100">
                              <div className={`h-2 rounded ${row.isYou ? "bg-[var(--text-primary)]" : "bg-[var(--primary)]/45"}`} style={{ width: `${pct(row.reviews, maxReviews)}%` }} />
                            </div>
                          </div>
                        </td>
                        <td className="px-3 py-2">{row.distance}</td>
                        <td className="px-3 py-2 text-[var(--text-secondary)]">{row.note}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <div className="rounded-[18px] border border-[var(--border-default)] bg-[var(--surface)] p-4 text-[var(--text-secondary)]">
                <p className="text-xs font-semibold uppercase tracking-wide text-[var(--text-muted)]">Key Insight</p>
                <p className="mt-1">{verdict.reasons[0] || "Competitive pressure appears moderate; prioritize converting existing demand."}</p>
              </div>
            </div>
          )}

          {activeTab === "siteGaps" && (
            <div className="space-y-4 text-sm">
              <div className="grid gap-3 md:grid-cols-4">
                <MetricCard label="Pages Checked" value={String(pagesCrawled || "—")} />
                <MetricCard label="Geo Intent Pages" value={String(geoIntentPages || 0)} />
                <MetricCard label="Service Pages" value={String(servicePages || 0)} tone={servicePages < 3 ? "danger" : "default"} />
                <MetricCard label="CTA Elements" value={String(ctaCount || 0)} />
              </div>

              <Card className="rounded-[22px] border border-[var(--border-default)] p-4">
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <h3 className="text-sm font-semibold text-[var(--text-primary)]">Capture Verification</h3>
                    <p className="mt-1 text-[11px] text-[var(--text-muted)]">
                      Neyma shows only what it verified in the pages checked and the first booking or contact step it could confirm.
                    </p>
                  </div>
                  {followupPagesChecked.length ? (
                    <span className="rounded-full border border-[var(--border-default)] bg-[var(--surface)] px-2 py-1 text-[10px] font-medium uppercase tracking-wide text-[var(--text-secondary)]">
                      {followupPagesChecked.length} follow-up page{followupPagesChecked.length === 1 ? "" : "s"} checked
                    </span>
                  ) : null}
                </div>
                <div className="mt-3 space-y-2">
                  <CaptureReadinessRow
                    label="Scheduling CTA"
                    {...schedulingState}
                  />
                  <CaptureReadinessRow
                    label="Booking Flow"
                    {...bookingState}
                  />
                  <CaptureReadinessRow
                    label="Contact Form"
                    {...contactState}
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
                    label="Enhanced Page Review"
                    status={usesPlaywrightPath ? "Used in this scan" : "Not used in this scan"}
                    icon={usesPlaywrightPath ? "✓" : "✕"}
                    note={usesPlaywrightPath
                      ? "Neyma used enhanced page verification where needed during this review."
                      : "This review relied on standard page verification only."}
                  />
                </div>
                <div className="mt-3 rounded-[18px] border border-[var(--border-default)] bg-[var(--surface)] px-4 py-4">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Verification path</p>
                      <span className="rounded-full border border-[var(--border-default)] bg-white px-2 py-1 text-[10px] font-medium text-[var(--text-secondary)]">
                        {verificationSummary}
                      </span>
                  </div>
                  <div className="mt-3 flex flex-wrap items-center gap-2">
                    <CapturePagePill page={homepagePage} tone="primary" />
                    {uniqueFollowupPagesChecked.length ? (
                      <>
                        <span className="text-slate-400">→</span>
                        {uniqueFollowupPagesChecked.slice(0, 3).map((page) => <CapturePagePill key={page} page={page} />)}
                      </>
                    ) : null}
                    {verificationOutcomeSummary ? (
                      <>
                        <span className="text-slate-400">→</span>
                        {[schedulingState.status, bookingState.status, contactState.status]
                          .filter((value) => value && value !== "Not verified in this scan")
                          .slice(0, 3)
                          .map((value) => (
                            <span key={value} className="rounded-full border border-[var(--border-default)] bg-white px-2.5 py-1 text-[11px] font-medium text-[var(--text-secondary)]">
                              {flowOutcomeLabel(value)}
                            </span>
                          ))}
                      </>
                    ) : null}
                  </div>
                </div>
              </Card>

              {!usesPlaywrightPath ? (
                <div className="rounded-[18px] border border-amber-200 bg-amber-50 p-4 text-amber-900">
                  <p className="text-xs font-semibold uppercase tracking-wide">Coverage Note</p>
                  <p className="mt-1 text-sm">{coverageWarningLabel(crawlWarning)}</p>
                </div>
              ) : null}
            </div>
          )}

          {activeTab === "fullAudit" && (
            <div className="space-y-4 text-sm">
              <div className="rounded-[22px] border border-[var(--border-default)] bg-[var(--surface)] p-4">
                <div className="flex flex-wrap items-center gap-3">
                  <MetaChip label="Pages" value={String(pagesCrawled || 0)} />
                  <MetaChip label="JS Detected" value={jsDetected ? "Yes" : "No"} tone="badge" />
                  <MetaChip label="Method" value={crawlMethodLabel(svc.crawl_method)} />
                </div>
                {!usesPlaywrightPath ? (
                  <p className="mt-2 rounded border border-amber-200 bg-amber-50 px-2 py-1 text-xs text-amber-900">
                    Verification note: {coverageWarningLabel(crawlWarning)}
                  </p>
                ) : null}
              </div>

              <section className="grid gap-3 lg:grid-cols-2">
                <Card className="rounded-[22px] border border-[var(--border-default)] p-4">
                  <h3 className="text-sm font-semibold text-[var(--text-primary)]">Reviews & Reputation</h3>
                  <ul className="mt-2 space-y-1 text-[var(--text-secondary)]">
                    <li>Your reviews vs avg: <strong>{reviewCount || "—"} vs {localAvgReviews || "—"}</strong></li>
                    <li>Last review date: <strong>{(ds as Record<string, unknown>).last_review_days_ago != null ? `${String((ds as Record<string, unknown>).last_review_days_ago)} days ago` : "—"}</strong></li>
                    <li>Competitors sampled: <strong>{String((result.evidence || []).find((e) => String(e.label || "").toLowerCase().includes("reviews vs market")) ? competitorRows.length - 1 : competitorRows.length - 1)}</strong></li>
                  </ul>
                  <div className="mt-3 rounded-[18px] border border-[var(--border-default)] bg-[var(--surface)] p-3">
                    <p className="text-[10px] font-semibold uppercase tracking-wide text-[var(--text-muted)]">Review Summary</p>
                    <p className="mt-1 text-xs text-[var(--text-secondary)]">
                      {String((reviewIntel as Record<string, unknown>).summary || "No review-theme summary available for this lead yet.")}
                    </p>
                  </div>
                </Card>

                <Card className="rounded-[22px] border border-[var(--border-default)] p-4">
                  <h3 className="text-sm font-semibold text-[var(--text-primary)]">Market Context</h3>
                  <ul className="mt-2 space-y-1 text-[var(--text-secondary)]">
                    <li>Market density: <strong>{formatMarketContextValue("density", mp?.market_density || result.market_density || "—")}</strong></li>
                    <li>Review position: <strong>{formatMarketContextValue("visibility", svc.visibility_gap || svc.high_value_service_leverage || "—")}</strong></li>
                    <li>Service coverage: <strong>{formatMarketContextValue("servicePages", servicePages)} page{servicePages === 1 ? "" : "s"} found</strong></li>
                    <li>Contact path: <strong>{formatMarketContextValue("form", convStruct.form_single_or_multi_step || "unknown")}</strong></li>
                    <li>Website: <strong>{formatMarketContextValue("website", result.website)}</strong></li>
                  </ul>
                </Card>
              </section>

              <section className="space-y-2">
                <h3 className="text-sm font-semibold text-[var(--text-primary)]">Geo Intent Coverage</h3>
                <div className="rounded-[22px] border border-[var(--border-default)] p-4">
                  <div className="mb-2 flex items-center justify-between text-xs text-[var(--text-secondary)]">
                    <span>{geoIntentPages} of 30 pages</span>
                    <span>{pct(geoIntentPages, 30)}%</span>
                  </div>
                  <div className="h-2 rounded bg-[var(--surface)]">
                    <div className="h-2 rounded bg-[var(--text-primary)]" style={{ width: `${pct(geoIntentPages, 30)}%` }} />
                  </div>
                </div>

                <Card className="rounded-[22px] border border-[var(--border-default)] p-4">
                  <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-[var(--text-muted)]">Detected</p>
                  <div className="overflow-x-auto">
                    <table className="min-w-full text-left text-xs sm:text-sm">
                      <thead className="bg-[var(--surface)] text-[var(--text-secondary)]">
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
                          <tr key={`${row.url}-${idx}`} className={`border-t border-[var(--border-default)] ${highlighted ? "bg-amber-50/70" : ""}`}>
                            <td className="px-2 py-1.5 text-[var(--text-primary)]">{row.title}</td>
                            <td className={`px-2 py-1.5 font-mono text-[11px] ${highlighted ? "text-amber-900" : "text-[var(--text-secondary)]"}`}>{row.url}</td>
                            <td className="px-2 py-1.5">
                              <div className="flex flex-wrap gap-1">
                                {row.signals.includes("city") ? <span className="rounded bg-zinc-200 px-1.5 py-0.5 text-[10px] text-zinc-800">📍 city</span> : null}
                                {row.signals.includes("near-me") ? <span className="rounded bg-emerald-100 px-1.5 py-0.5 text-[10px] text-emerald-700">🔍 near-me</span> : null}
                                {row.signals.includes("schema") ? <span className="rounded bg-violet-100 px-1.5 py-0.5 text-[10px] text-violet-700">🏷 schema</span> : null}
                                {row.signals.includes("meta") ? <span className="rounded bg-amber-100 px-1.5 py-0.5 text-[10px] text-amber-800">📝 meta</span> : null}
                                {!row.signals.length ? <span className="text-[var(--text-muted)]">—</span> : null}
                              </div>
                            </td>
                            <td className="px-2 py-1.5">{row.hasCTA ? "Yes" : "No"}</td>
                          </tr>
                        );}) : (
                          <tr><td className="px-2 py-2 text-[var(--text-muted)]" colSpan={4}>No geo-intent pages captured.</td></tr>
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
                  <Card className="rounded-[22px] border border-[var(--border-default)] p-4">
                    <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-[var(--text-muted)]">Service Signal Verification</p>
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
                              {row.aiVerdict ? <span className="rounded bg-zinc-200 px-1.5 py-0.5 text-[10px] font-semibold text-zinc-800">AI: {row.aiVerdict}</span> : null}
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
                <h3 className="text-sm font-semibold text-[var(--text-primary)]">CTA Elements</h3>
                <Card className="rounded-[22px] border border-[var(--border-default)] p-4">
                  <p className="text-xs text-[var(--text-muted)]">Total CTA elements detected</p>
                  <p className="text-2xl font-semibold text-[var(--text-primary)]">{ctaCount}</p>
                  <p className="mt-1 text-xs text-[var(--text-muted)]">Of these, {clickableCtaCount} are clickable links/buttons.</p>
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
                          <span className={`rounded px-1.5 py-0.5 text-[10px] font-semibold ${row.type === "Book" ? "bg-zinc-200 text-zinc-800" : row.type === "Schedule" ? "bg-emerald-100 text-emerald-700" : row.type === "Contact" ? "bg-amber-100 text-amber-800" : "bg-slate-200 text-slate-700"}`}>{row.type}</span>
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
                <h3 className="mb-2 text-sm font-semibold text-[var(--text-primary)]">Risk Flags</h3>
                <div className="rounded-[22px] border border-amber-200 bg-amber-50 p-4">
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

        <Card className="rounded-[22px] border border-[var(--border-default)] p-4 text-xs text-[var(--text-secondary)]">
          <div className="flex flex-wrap gap-2">
            <Link href={navContext.backHref} className="app-link">{navContext.backLabel}</Link>
            {canShare ? <Button onClick={handleShare} disabled={sharing}>{sharing ? "Sharing..." : "Share"}</Button> : null}
            {canExport ? (
              <a
                href={getDiagnosticBriefPdfUrl(id)}
                target="_blank"
                rel="noreferrer"
                className="inline-flex h-9 items-center rounded-[var(--radius-md)] border border-[var(--border-default)] bg-[var(--bg-card)] px-3 text-sm font-medium text-[var(--text-secondary)] hover:bg-slate-50"
              >
                Download PDF
              </a>
            ) : null}
            <Button onClick={() => handleRerun(false)} disabled={rerunning}>{rerunning ? "Refreshing..." : "Refresh brief"}</Button>
            {canUseWorkspace ? (
              <Button onClick={handleDelete} disabled={deleting} className="border-rose-200 text-rose-600 hover:bg-rose-50">Delete</Button>
            ) : null}
          </div>
          {shareUrl ? <p className="mt-2 break-all">Share URL: {shareUrl}</p> : null}
        </Card>
      </main>

      {showMobileFooter && (
        <div className="fixed inset-x-0 bottom-0 z-50 border-t border-[var(--border-default)] bg-[var(--bg-card)]/95 p-3 backdrop-blur lg:hidden">
          <div className="mx-auto flex max-w-6xl items-center justify-between gap-2">
            <div className="min-w-0">
              <p className="truncate text-xs font-semibold">{result.business_name}</p>
              <p className="truncate text-[11px] text-[var(--text-muted)]">{opportunitySignal || "Opportunity signal: —"}</p>
            </div>
            <div className="flex gap-2">
              {canSave ? <Button onClick={() => setAddListOpen(true)}>Add to Pipeline</Button> : null}
              {canUseWorkspace ? <Button onClick={() => setOutreachOpen(true)} className="border-[var(--border-default)]">Log Outreach</Button> : null}
            </div>
          </div>
        </div>
      )}

      {addListOpen && canSave && (
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

      {outreachOpen && canUseWorkspace && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
          <div className="w-full max-w-md rounded-[22px] border border-[var(--border-default)] bg-[var(--bg-card)] p-4 sm:p-5">
            <h3 className="text-sm font-semibold text-[var(--text-primary)]">Log Outreach</h3>
            <p className="mt-1 text-xs text-[var(--text-muted)]">Record the latest rep action and any quick notes.</p>

            <div className="mt-4 space-y-3">
              <div className="rounded-[18px] border border-[var(--border-default)] bg-[var(--surface)] p-3">
                <label className="mb-1.5 block text-[11px] font-medium uppercase tracking-[0.08em] text-[var(--text-muted)]">
                  Outcome
                </label>
                <div className="grid grid-cols-2 gap-2">
                  {OUTREACH_ACTIONS.map((option) => {
                    const active = outreachAction === option;
                    return (
                      <button
                        key={option}
                        type="button"
                        onClick={() => setOutreachAction(option)}
                        className={`rounded-[12px] border px-3 py-2 text-left text-sm transition ${
                          active
                            ? "border-[var(--primary)] bg-white font-medium text-[var(--text-primary)]"
                            : "border-[var(--border-default)] bg-white text-[var(--text-secondary)] hover:border-[var(--ring)]"
                        }`}
                      >
                        {option}
                      </button>
                    );
                  })}
                </div>
              </div>

              <div className="rounded-[18px] border border-[var(--border-default)] bg-[var(--surface)] p-3">
                <label className="mb-1.5 block text-[11px] font-medium uppercase tracking-[0.08em] text-[var(--text-muted)]">
                  Notes
                </label>
                <textarea
                  className="h-24 w-full rounded-[12px] border border-[var(--border-default)] bg-white px-3 py-2 text-sm text-[var(--text-primary)] outline-none transition placeholder:text-[var(--text-muted)] focus:border-[var(--ring)]"
                  value={outreachNote}
                  onChange={(e) => setOutreachNote(e.target.value)}
                  placeholder="Optional notes"
                />
              </div>
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
                  <span className={`rounded px-2 py-1 text-xs font-semibold ${activeCtaRow.type === "Book" ? "bg-zinc-200 text-zinc-800" : activeCtaRow.type === "Schedule" ? "bg-emerald-100 text-emerald-700" : activeCtaRow.type === "Contact" ? "bg-amber-100 text-amber-800" : "bg-slate-200 text-slate-700"}`}>
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

function HeroPill({ children, tone }: { children: React.ReactNode; tone: "green" | "neutral" }) {
  return (
    <span
      className={`inline-flex items-center rounded-full border px-2.5 py-1 text-xs font-semibold ${
        tone === "green"
          ? "border-emerald-200 bg-emerald-50 text-emerald-700"
          : "border-[var(--border-default)] bg-[var(--surface)] text-[var(--text-primary)]"
      }`}
    >
      {children}
    </span>
  );
}

function HeroSignalCell({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-[14px] bg-[var(--surface)] px-4 py-3">
      <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-[var(--text-secondary)]">{label}</p>
      <p className="mt-1 text-sm font-medium text-[var(--text-primary)]">{value || "—"}</p>
    </div>
  );
}

function rationaleTone(text: string): "urgent" | "advantage" {
  const lower = text.toLowerCase();
  if (
    lower.includes("active") ||
    lower.includes("dense") ||
    lower.includes("gap") ||
    lower.includes("missing") ||
    lower.includes("weak") ||
    lower.includes("limited") ||
    lower.includes("thin")
  ) {
    return "urgent";
  }
  return "advantage";
}

function VerificationItem({ label, status, complete }: { label: string; status: string; complete: boolean }) {
  return (
    <div className="flex items-start gap-3 rounded-[14px] bg-white px-3 py-3">
      <span
        className={`mt-0.5 inline-flex h-5 w-5 items-center justify-center rounded-full text-xs font-semibold ${
          complete ? "bg-emerald-100 text-emerald-700" : "bg-slate-100 text-slate-500"
        }`}
      >
        {complete ? "✓" : "—"}
      </span>
      <div className="min-w-0">
        <p className="text-sm font-medium text-[var(--text-primary)]">{label}</p>
        <p className="mt-1 text-xs leading-5 text-[var(--text-secondary)]">{status}</p>
      </div>
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

function captureTone(icon: string): { dot: string; panel: string; badge: string } {
  const ok = icon === "✓";
  if (ok) {
    return {
      dot: "bg-emerald-100 text-emerald-700",
      panel: "border-emerald-200 bg-emerald-50/40",
      badge: "bg-emerald-100 text-emerald-700",
    };
  }
  if (icon === "✕") {
    return {
      dot: "bg-red-100 text-red-700",
      panel: "border-red-200 bg-red-50/40",
      badge: "bg-red-100 text-red-700",
    };
  }
  return {
    dot: "bg-slate-100 text-slate-500",
    panel: "border-slate-200 bg-white",
    badge: "bg-slate-100 text-slate-700",
  };
}

function CapturePagePill({ page, tone = "default" }: { page: string; tone?: "default" | "primary" }) {
  const label = capturePageLabel(page);
  return (
    <span className={`rounded-full border px-2.5 py-1 font-mono text-[11px] ${tone === "primary" ? "border-[#4f79c7]/25 bg-[#4f79c7]/8 text-[#22406e]" : "border-slate-200 bg-white text-slate-700"}`}>
      {label}
    </span>
  );
}

function CaptureReadinessRow({
  label,
  status,
  icon,
  note,
  confidence,
  observedPages,
  evidence,
}: {
  label: string;
  status: string;
  icon: string;
  note: string;
  confidence?: string;
  observedPages?: string[];
  evidence?: string;
}) {
  const tone = captureTone(icon);
  const uniqueObservedPages = Array.from(new Set((observedPages || []).filter(Boolean)));
  return (
    <div className={`grid grid-cols-[24px_1fr] items-start gap-2 rounded border px-2 py-2 ${tone.panel}`}>
      <span className={`mt-0.5 inline-flex h-5 w-5 items-center justify-center rounded-full text-xs font-semibold ${tone.dot}`}>{icon}</span>
      <div className="min-w-0">
        <div className="flex flex-wrap items-center gap-2">
          <p className="text-xs font-semibold text-slate-900">{label}</p>
          <p className="text-xs text-slate-700">{status}</p>
          {confidence ? (
            <span className={`rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide ${tone.badge}`}>
              {confidence} confidence
            </span>
          ) : null}
        </div>
        <p className="mt-1 text-[11px] text-slate-500">{note}</p>
        {(uniqueObservedPages.length || evidence) ? (
          <div className="mt-2 flex flex-wrap gap-1.5">
            {uniqueObservedPages.map((page) => <CapturePagePill key={page} page={page} />)}
            {evidence ? (
              <span className="rounded-full border border-slate-200 bg-white px-2 py-1 text-[11px] text-slate-600">
                {evidence}
              </span>
            ) : null}
          </div>
        ) : null}
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
