"use client";

import { useEffect, useState } from "react";
import { useParams, useRouter, useSearchParams } from "next/navigation";
import Link from "next/link";
import {
  addProspectsToList,
  createProspectList,
  deleteDiagnostic,
  getDiagnostic,
  createDiagnosticShareLink,
  getDiagnosticBriefPdfUrl,
  getProspectLists,
  pollUntilDone,
  submitDiagnostic,
} from "@/lib/api";
import type { DiagnosticResponse, ProspectList } from "@/lib/types";
import Button from "@/app/components/ui/Button";
import { Card } from "@/app/components/ui/Card";
import ListPickerModal from "@/app/components/ListPickerModal";

function oppProfileText(op: unknown): string {
  if (!op) return "";
  if (typeof op === "object" && op !== null && "label" in op) {
    const o = op as { label?: string; why?: string };
    return o.why ? `${o.label} (${o.why})` : o.label ?? "";
  }
  return String(op);
}

function leverageDriversText(op: unknown): string {
  if (!op || typeof op !== "object") return "";
  const drivers = (op as {
    leverage_drivers?: {
      missing_high_value_pages?: boolean;
      market_density_high?: boolean;
      structured_trust_weak?: boolean;
      paid_active?: boolean;
      review_deficit?: boolean;
    };
  }).leverage_drivers;
  if (!drivers) return "";
  return [
    `missing high-value pages: ${drivers.missing_high_value_pages ? "yes" : "no"}`,
    `high-density market: ${drivers.market_density_high ? "yes" : "no"}`,
    `paid ads active: ${drivers.paid_active ? "yes" : "no"}`,
    `review deficit: ${drivers.review_deficit ? "yes" : "no"}`,
    `structured trust weak: ${drivers.structured_trust_weak ? "yes" : "no"}`,
  ].join(", ");
}

function firstNumber(value: unknown): number | null {
  if (value == null) return null;
  const match = String(value).match(/-?\d+(\.\d+)?/);
  if (!match) return null;
  const num = Number(match[0]);
  return Number.isFinite(num) ? num : null;
}

function boolValueLabel(value: unknown): string {
  if (value == null) return "—";
  return value ? "Yes" : "No";
}

function triStateLabel(value: unknown): string {
  if (value === "detected") return "Detected";
  if (value === "not_detected") return "Not detected";
  return "Unknown";
}

function wiringBadge(count: unknown): string {
  const n = Number(count || 0);
  if (!Number.isFinite(n) || n <= 0) return "❌ Orphan";
  if (n <= 2) return "⚠ Weak";
  if (n <= 7) return "✓ Linked";
  return "⭐ Core";
}

function parseCompetitorLine(value: string): { name: string; reviews: string; distance: string; notes: string } {
  const parts = value
    .split(/[—-]/g)
    .map((p) => p.trim())
    .filter(Boolean);
  const name = parts[0] || value.trim();
  const reviewsMatch = value.match(/(\d+(?:\.\d+)?)\s*reviews?/i);
  const ratingMatch = value.match(/\((\d\.\d)\)/);
  const distanceMatch = value.match(/(\d+(?:\.\d+)?)\s*mi\b/i);
  const reviews = reviewsMatch
    ? `${reviewsMatch[1]}${ratingMatch ? ` (${ratingMatch[1]})` : ""}`
    : "—";
  const distance = distanceMatch ? `${distanceMatch[1]} mi` : "—";
  const notes = parts.slice(1).join(" — ") || "Competitive context";
  return { name, reviews, distance, notes };
}

export default function DiagnosticDetailPage() {
  const params = useParams();
  const router = useRouter();
  const searchParams = useSearchParams();
  const id = Number(params.id);
  const invalidId = !id || isNaN(id);
  const from = String(searchParams.get("from") || "").toLowerCase();
  const scanId = searchParams.get("scanId") || "";

  const [result, setResult] = useState<DiagnosticResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [rerunning, setRerunning] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [sharing, setSharing] = useState(false);
  const [shareUrl, setShareUrl] = useState<string | null>(null);
  const [lists, setLists] = useState<ProspectList[]>([]);
  const [addListOpen, setAddListOpen] = useState(false);
  const [addingToList, setAddingToList] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);

  useEffect(() => {
    if (invalidId) {
      return;
    }
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

  const navContext = (() => {
    if (from === "ask") {
      return {
        crumbLabel: "Ask Neyma",
        backLabel: "Back to results",
        backHref: "/ask",
      };
    }
    if (from === "territory" && scanId) {
      return {
        crumbLabel: "Territory",
        backLabel: "Back to territory",
        backHref: `/territory/${encodeURIComponent(scanId)}`,
      };
    }
    return {
      crumbLabel: "Dashboard",
      backLabel: "Back to Dashboard",
      backHref: "/dashboard",
    };
  })();

  async function handleRerun() {
    if (!result) return;
    setRerunning(true);
    try {
      const { job_id } = await submitDiagnostic({
        business_name: result.business_name,
        city: result.city,
        state: result.state || "",
      });
      const job = await pollUntilDone(job_id);
      if (job.status === "completed" && job.diagnostic_id) {
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
    if (!confirm("Delete this diagnostic?")) return;
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
      alert("Share link copied to clipboard");
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
      if (!listId) {
        if (!payload.newListName) throw new Error("List name is required");
        const created = await createProspectList(payload.newListName);
        listId = created.id;
      }
      await addProspectsToList(listId, [id]);
      const updated = await getProspectLists().catch(() => ({ items: [] }));
      setLists(updated.items);
      setNotice(`Added ${result?.business_name || "lead"} to list.`);
      setAddListOpen(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to add to list");
    } finally {
      setAddingToList(false);
    }
  }

  if (invalidId) {
    return (
      <div className="mx-auto max-w-5xl px-2 py-10">
        <main className="text-center">
          <p className="text-red-600">Invalid diagnostic ID</p>
          <Link href="/dashboard" className="mt-4 inline-block text-sm text-zinc-600 underline">Back to dashboard</Link>
        </main>
      </div>
    );
  }

  if (loading) {
    return (
      <div className="mx-auto max-w-5xl px-2 py-10">
        <main className="text-center">
          <div className="inline-block h-6 w-6 animate-spin rounded-full border-2 border-zinc-300 border-t-zinc-600" />
          <p className="mt-3 text-sm text-zinc-400">Loading diagnostic…</p>
        </main>
      </div>
    );
  }

  if (!result) {
    return (
      <div className="mx-auto max-w-5xl px-2 py-10">
        <main className="text-center">
          <p className="text-red-600">{error || "Diagnostic not found"}</p>
          <Link href="/dashboard" className="mt-4 inline-block text-sm text-zinc-600 underline">Back to dashboard</Link>
        </main>
      </div>
    );
  }

  const b = result.brief;
  const ed = b?.executive_diagnosis;
  const mp = b?.market_position;
  const cc = b?.competitive_context;
  const ds = b?.demand_signals;
  const csg = b?.competitive_service_gap;
  const sg = b?.strategic_gap;
  const ht = b?.high_ticket_gaps;
  const rucg = b?.revenue_upside_capture_gap;
  const ci = b?.conversion_infrastructure;
  const cd = b?.competitive_delta as Record<string, unknown> | undefined;
  const reviewIntel = b?.review_intelligence as Record<string, unknown> | undefined;
  const convStruct = b?.conversion_structure as Record<string, unknown> | undefined;
  const marketSat = b?.market_saturation as Record<string, unknown> | undefined;
  const geo = b?.geo_coverage as Record<string, unknown> | undefined;
  const reviewComplaintThemes = (reviewIntel?.complaint_themes as Record<string, unknown>) || {};
  const ccLine3Items = Array.isArray((cc as Record<string, unknown> | undefined)?.line3_items)
    ? ((cc as Record<string, unknown>).line3_items as string[])
    : [];
  const parsedCompetitorRows = ccLine3Items.map(parseCompetitorLine);
  const formatMiles = (value: unknown): string => {
    if (value == null) return "—";
    const num = Number(value);
    if (Number.isFinite(num)) {
      return Number.isInteger(num) ? `${num} mi` : `${num.toFixed(1)} mi`;
    }
    return `${String(value)} mi`;
  };
  const conversionFormValue = (value: unknown): string => {
    if (value === "multi_step") return "Multi-step (more than one step to submit)";
    if (value === "single_step") return "Single-step";
    return String(value);
  };
  const websiteHref = result.website
    ? (result.website.startsWith("http://") || result.website.startsWith("https://") ? result.website : `https://${result.website}`)
    : null;
  const reviewsNum = firstNumber(mp?.reviews);
  const localAvgNum = firstNumber(mp?.local_avg);
  const belowLocalAverage = reviewsNum != null && localAvgNum != null && reviewsNum < (localAvgNum * 0.8);
  const adStatus = ds?.google_ads_line || result.paid_status || "—";
  const missingLandingPages = ht?.missing_landing_pages ?? result.service_intelligence?.missing_services ?? [];
  const detectedServices = ht?.high_ticket_services_detected ?? result.service_intelligence?.detected_services ?? [];
  const serviceUpsideMap = new Map(
    (ht?.service_level_upside ?? [])
      .filter((x) => x?.service)
      .map((x) => [String(x.service).toLowerCase(), String(x.upside || "—")]),
  );
  const servicePageAnalysis = (b?.service_page_analysis as Record<string, unknown> | undefined) ?? {};
  const servicePageAnalysisV2 = (
    (servicePageAnalysis.v2 as Record<string, unknown> | undefined)
    ?? (result.service_intelligence?.service_page_analysis_v2 as Record<string, unknown> | undefined)
    ?? {}
  );
  const spaCoverage = (servicePageAnalysisV2.service_coverage as Record<string, unknown> | undefined) ?? {};
  const spaDepth = (servicePageAnalysisV2.content_depth as Record<string, unknown> | undefined) ?? {};
  const spaConversion = (servicePageAnalysisV2.conversion_readiness as Record<string, unknown> | undefined) ?? {};
  const spaLocal = (servicePageAnalysisV2.local_intent_coverage as Record<string, unknown> | undefined) ?? {};
  const spaTrust = (servicePageAnalysisV2.structured_trust_signals as Record<string, unknown> | undefined) ?? {};
  const spaCrawl = (servicePageAnalysisV2.crawl_coverage as Record<string, unknown> | undefined) ?? {};
  const crawlConfidence = String(spaCrawl.confidence || "unknown");
  const coverageStatus = triStateLabel(spaCoverage.status);
  const depthStatus = triStateLabel(spaDepth.status);
  const conversionStatus = triStateLabel(spaConversion.status);
  const localStatus = triStateLabel(spaLocal.status);
  const wiringPagesWithSupport = Number(spaTrust.pages_with_wiring_support ?? 0);
  const wiringAvgBodyUnique = Number(spaTrust.avg_inbound_unique_pages_body_only ?? 0);
  const highValueServices = (servicePageAnalysis.services as Array<Record<string, unknown>> | undefined)
    ?? (result.service_intelligence?.high_value_services as Array<Record<string, unknown>> | undefined)
    ?? [];
  const highValueSummary = (servicePageAnalysis.summary as Record<string, unknown> | undefined)
    ?? (result.service_intelligence?.high_value_summary as Record<string, unknown> | undefined)
    ?? {};
  const highValueLeverage = String(
    (servicePageAnalysis.leverage as string | undefined)
    ?? (result.service_intelligence?.high_value_service_leverage as string | undefined)
    ?? "—",
  );
  const allServices = Array.from(new Set([...detectedServices, ...missingLandingPages]));
  const hasWebsite = Boolean(result.website);
  const targetServicePageCount = Number(cd?.target_service_page_count ?? 0);
  const hasServiceCoverageData = highValueServices.length > 0 || allServices.length > 0 || targetServicePageCount > 0;
  const landingPagesValue = !hasWebsite
    ? "Not available"
    : (missingLandingPages.length ? `${missingLandingPages.length} missing` : (hasServiceCoverageData ? "All key pages present" : "Insufficient crawl data"));
  const landingPagesTone: "good" | "warn" | "neutral" = (!hasWebsite || !hasServiceCoverageData || missingLandingPages.length > 0) ? "warn" : "good";
  const landingPagesStatus = !hasWebsite
    ? "No website detected"
    : (!hasServiceCoverageData ? "Coverage could not be validated from crawl data" : undefined);
  const leadDensity = mp?.market_density ?? result.market_density ?? "—";
  const oppText = oppProfileText(ed?.opportunity_profile || result.opportunity_profile);
  const demandContextLine =
    ds?.google_ads_line
    || ds?.organic_visibility_tier
    || (ds?.review_velocity_30d != null ? `Review velocity is ${ds.review_velocity_30d} in the last 30 days.` : "")
    || "";
  const problemHeadline = `${ed?.constraint || result.constraint || "Constraint unclear"}, ${ed?.primary_leverage || result.primary_leverage || "leverage unclear"}.`;
  const problemContext = [
    oppText ? `Opportunity profile: ${oppText}.` : "",
    `Market density is ${leadDensity}.`,
    demandContextLine ? `${demandContextLine}` : "",
  ].filter(Boolean).join(" ");
  const strategicReviewComparison = (() => {
    const competitorReviews = firstNumber(sg?.competitor_reviews);
    const leadReviews = firstNumber(mp?.reviews);
    if (competitorReviews == null || leadReviews == null) return "relative to that competitor";
    if (leadReviews < competitorReviews) return "below that competitor";
    if (leadReviews > competitorReviews) return "above that competitor";
    return "in line with that competitor";
  })();
  const structuredPlan = Array.isArray((b as Record<string, unknown> | undefined)?.intervention_plan_structured)
    ? ((b as Record<string, unknown>).intervention_plan_structured as Array<Record<string, unknown>>)
    : [];
  const planItems = structuredPlan.length > 0
    ? structuredPlan.map((item, index) => ({
      step: Number(item.step) || index + 1,
      category: String(item.category || "Operational"),
      action: String(item.action || ""),
    }))
    : (b?.intervention_plan?.length
      ? b.intervention_plan.map((action, index) => ({
        step: index + 1,
        category: "Operational",
        action,
      }))
      : result.intervention_plan.map((item) => ({
        step: item.step,
        category: item.category,
        action: item.action,
      })));
  const suggestionHrefFromCategory = (category: string): string => {
    const cat = category.toLowerCase();
    if (cat.includes("demand")) return "#demand-signals-full";
    if (cat.includes("capture")) return "#service-page-analysis";
    if (cat.includes("conversion")) return "#conversion-infrastructure";
    if (cat.includes("trust")) return "#review-authority";
    if (cat.includes("reputation")) return "#review-authority";
    if (cat.includes("seo")) return "#service-page-analysis";
    if (cat.includes("operational")) return "#conversion-infrastructure";
    if (cat.includes("strategic")) return "#competitive-context-full";
    return "#market-position-full";
  };
  const ctaCount = Number(convStruct?.cta_count ?? 0);
  const ctaLabels = Array.isArray(convStruct?.cta_labels)
    ? (convStruct.cta_labels as string[])
    : (ctaCount > 0 ? ["Book", "Schedule", "Contact", "Call"] : []);

  return (
    <div className="mx-auto max-w-6xl text-[var(--text-primary)]">
      <main>
        {/* Top bar */}
        <div className="mb-4 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <p className="text-sm text-[var(--text-muted)]">
            <Link href={navContext.backHref} className="app-link">{navContext.crumbLabel}</Link> {"→"} <span className="text-[var(--text-secondary)]">{result.business_name}</span>
          </p>
          <div className="flex items-center gap-2">
            <Link href={navContext.backHref} className="app-link text-sm">{navContext.backLabel}</Link>
            <Button
              onClick={() => setAddListOpen(true)}
            >
              Add to list
            </Button>
            <Button
              onClick={handleShare}
              disabled={sharing}
            >
              {sharing ? "Sharing…" : "Share"}
            </Button>
            <a
              href={getDiagnosticBriefPdfUrl(id)}
              target="_blank"
              rel="noreferrer"
              className="inline-flex h-9 items-center rounded-[var(--radius-md)] border border-[var(--border-default)] bg-[var(--bg-card)] px-3 text-sm font-medium text-[var(--text-secondary)] hover:bg-slate-50"
            >
              Download PDF
            </a>
            <Button
              onClick={handleRerun}
              disabled={rerunning}
            >
              {rerunning ? "Re-running…" : "Re-run diagnostic"}
            </Button>
            <Button
              onClick={handleDelete}
              disabled={deleting}
              className="border-rose-200 text-rose-600 hover:bg-rose-50"
            >
              Delete
            </Button>
          </div>
        </div>

        {(notice || error) && (
          <div className="mb-4">
            {notice && <p className="text-sm text-emerald-700">{notice}</p>}
            {error && <p className="text-sm text-red-600">{error}</p>}
          </div>
        )}

        <Card className="mb-4 p-4">
          <h2 className="mb-2 text-sm font-semibold text-[var(--text-primary)]">Contact</h2>
          <div className="grid gap-2 text-sm sm:grid-cols-2">
            <p>
              <strong>Phone:</strong>{" "}
              {result.phone ? <a href={`tel:${result.phone}`} className="app-link">{result.phone}</a> : "—"}
            </p>
            <p className="break-all">
              <strong>Website:</strong>{" "}
              {websiteHref ? <a href={websiteHref} target="_blank" rel="noreferrer" className="app-link">{result.website}</a> : "—"}
            </p>
          </div>
        </Card>

        <Card className="p-6">
          <h2 className="mb-1 text-sm font-medium uppercase tracking-wider text-zinc-500">Revenue Intelligence Brief</h2>
          <p className="mb-5 text-sm text-zinc-700">Lead #{result.lead_id} · {result.business_name} · {result.city}{result.state ? `, ${result.state}` : ""}</p>

          <div className="space-y-6 text-sm">
            {shareUrl && (
              <BriefSection title="Share Link">
                <p className="break-all text-zinc-700">{shareUrl}</p>
              </BriefSection>
            )}
            <BriefSection title="Problem">
              <p className="text-base font-semibold text-[var(--text-primary)]">{problemHeadline}</p>
              <p>{problemContext}</p>
              {leverageDriversText(ed?.opportunity_profile) && (
                <p className="text-xs text-[var(--text-muted)]">Leverage drivers: {leverageDriversText(ed?.opportunity_profile)}.</p>
              )}
              {b?.executive_footnote && <p className="text-xs text-[var(--text-muted)]">{b.executive_footnote}</p>}
            </BriefSection>

            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-6">
              <MetricCard
                label="Reviews"
                value={mp?.reviews || "—"}
                status={belowLocalAverage ? "Need more vs local avg" : (localAvgNum != null ? "In line with local avg" : undefined)}
                tone={belowLocalAverage ? "warn" : "good"}
              />
              <MetricCard label="Conversion Rate" value="—" status="No GA4 data" />
              <MetricCard label="Ad Status" value={adStatus} tone={/active|running/i.test(adStatus) ? "good" : "warn"} />
              <MetricCard label="Market Density" value={leadDensity} />
              <MetricCard
                label="Last Review"
                value={ds?.last_review_days_ago != null ? `${ds.last_review_days_ago} days ago` : "—"}
                tone={ds?.last_review_days_ago != null && ds.last_review_days_ago > 45 ? "warn" : "good"}
              />
              <MetricCard
                label="Review Velocity"
                value={ds?.review_velocity_30d != null ? `${ds.review_velocity_30d} in 30d` : "—"}
                tone={ds?.review_velocity_30d != null && ds.review_velocity_30d < 2 ? "warn" : "good"}
              />
            </div>

            <div className="grid gap-3 md:grid-cols-2">
              <MetricCard
                label="PPC Status"
                value={ds?.google_ads_line || "—"}
                status={ds?.paid_channels_detected?.length ? `Channels: ${ds.paid_channels_detected.join(", ")}` : undefined}
              />
              <MetricCard
                label="Landing Pages"
                value={landingPagesValue}
                status={landingPagesStatus}
                tone={landingPagesTone}
              />
            </div>

            <BriefSection title="Paid & Demand">
              <div className="grid gap-3 md:grid-cols-2">
                <MetricCard label="Paid Status" value={ds?.google_ads_line || "—"} status={ds?.paid_channels_detected?.length ? ds.paid_channels_detected.join(", ") : undefined} />
                <MetricCard label="Opportunity Value" value={ed?.modeled_revenue_upside || (rucg?.annual_low != null ? `$${rucg.annual_low.toLocaleString()}-$${(rucg.annual_high ?? 0).toLocaleString()} annually` : "—")} tone="good" />
              </div>
              {(ds?.paid_channels_detected?.length || ds?.organic_visibility_tier) && (
                <div className="rounded-[var(--radius-md)] border border-[var(--border-default)] bg-[var(--bg-card)] p-3">
                  {ds?.paid_channels_detected?.length ? <p><strong>Paid channels:</strong> {ds.paid_channels_detected.join(", ")}</p> : null}
                  {ds?.organic_visibility_tier ? <p><strong>Organic visibility:</strong> {ds.organic_visibility_tier}{ds.organic_visibility_reason ? ` — ${ds.organic_visibility_reason}` : ""}</p> : null}
                </div>
              )}
            </BriefSection>

            {(highValueServices.length > 0 || allServices.length > 0 || cd || ht?.schema || ci?.page_load_ms != null) ? (
              <BriefSection title="Service / Page Analysis" sectionId="service-page-analysis">
                <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                  <MetricCard
                    label="Service Coverage"
                    value={spaCoverage.status === "unknown"
                      ? "Unknown"
                      : (spaCoverage.total != null
                      ? `${spaCoverage.present ?? 0} of ${spaCoverage.total} pages`
                      : (cd?.target_service_page_count != null ? `${cd.target_service_page_count} pages` : "—"))}
                    status={`${coverageStatus}${spaCoverage.ratio != null ? ` · ${Math.round(Number(spaCoverage.ratio) * 100)}% coverage` : ""}`}
                  />
                  <MetricCard
                    label="Content Depth"
                    value={spaDepth.status === "unknown"
                      ? "Unknown"
                      : (spaDepth.average_words != null
                      ? `${Math.round(Number(spaDepth.average_words))} words avg`
                      : (cd?.target_avg_word_count_service_pages != null ? `${Math.round(Number(cd.target_avg_word_count_service_pages))} words` : "—"))}
                    status={`${depthStatus}${spaDepth.min_words != null && spaDepth.max_words != null ? ` · Min ${spaDepth.min_words} · Max ${spaDepth.max_words}` : ""}`}
                  />
                  <MetricCard
                    label="Conversion Readiness"
                    value={spaConversion.status === "unknown"
                      ? "Unknown"
                      : (spaConversion.pages_with_cta != null
                      ? `${spaConversion.pages_with_cta} pages with CTA`
                      : "—")}
                    status={`${conversionStatus}${spaConversion.pages_with_booking != null || spaConversion.pages_with_financing != null ? ` · Booking ${spaConversion.pages_with_booking ?? 0} · Financing ${spaConversion.pages_with_financing ?? 0}` : ""}`}
                  />
                  <MetricCard
                    label="Local Pages"
                    value={spaLocal.status === "unknown"
                      ? "Unknown"
                      : (spaLocal.city_or_near_me_pages != null
                      ? String(spaLocal.city_or_near_me_pages)
                      : String(geo?.city_or_near_me_page_count ?? "—"))}
                    status={`${localStatus}${spaLocal.has_multi_location_page != null ? ` · Multi-location: ${spaLocal.has_multi_location_page ? "Yes" : "No"}` : ""}`}
                  />
                </div>
                {(spaDepth.thin_pages != null || ci?.page_load_ms != null || spaCrawl.pages_checked != null || spaTrust.pages_with_wiring_support != null) && (
                  <p className="text-xs text-[var(--text-muted)]">
                    {spaDepth.thin_pages != null ? `Thin pages (<${spaDepth.thin_page_threshold_words ?? 300} words): ${spaDepth.thin_pages}. ` : ""}
                    {spaTrust.pages_with_wiring_support != null ? `Wiring support: ${wiringPagesWithSupport} pages linked in-body (avg ${wiringAvgBodyUnique.toFixed(2)}). ` : ""}
                    {spaCrawl.pages_checked != null ? `Based on ${spaCrawl.pages_checked} pages checked (${crawlConfidence} confidence). ` : ""}
                    {ci?.page_load_ms != null ? `Page load ${ci.page_load_ms} ms. ` : ""}
                  </p>
                )}
                {highValueServices.length > 0 ? (
                  <div className="mt-3 overflow-x-auto rounded-[var(--radius-md)] border border-[var(--border-default)]">
                    <table className="min-w-full text-left text-xs sm:text-sm">
                      <thead className="bg-slate-50 text-[var(--text-muted)]">
                        <tr>
                          <th className="px-3 py-2 font-medium">Service</th>
                          <th className="px-3 py-2 font-medium">Page</th>
                          <th className="px-3 py-2 font-medium">Depth</th>
                          <th className="px-3 py-2 font-medium text-[var(--text-muted)]">Wiring (bonus)</th>
                          <th className="px-3 py-2 font-medium">Tier</th>
                          <th className="px-3 py-2 font-medium">Validation</th>
                          <th className="px-3 py-2 font-medium">CTA / Conv</th>
                        </tr>
                      </thead>
                      <tbody>
                        {highValueServices.map((svc, idx) => {
                          const name = String(svc.display_name || svc.service || `Service ${idx + 1}`);
                          const pageExists = Boolean(svc.page_exists);
                          const wordCount = Number(svc.word_count || 0);
                          const depth = String(svc.depth_score || "—");
                          const conv = (svc.conversion as Record<string, unknown>) || {};
                          const inboundBodyOnly = Number(svc.inbound_unique_pages_body_only ?? 0);
                          const wiring = wiringBadge(inboundBodyOnly);
                          const tier = String(svc.optimization_tier || "—");
                          const predictionStatus = String(svc.prediction_status || svc.qualification_status || svc.optimization_tier || "—");
                          const coreRules = (svc.core_rules_passed as Record<string, unknown>) || {};
                          return (
                            <tr key={`${name}-${idx}`} className="border-t border-[var(--border-default)]">
                              <td className="px-3 py-2">{name}</td>
                              <td className="px-3 py-2">
                                <span className="inline-flex items-center gap-2">
                                  {pageExists ? (
                                    <span className="inline-flex h-5 w-5 items-center justify-center rounded-full bg-emerald-100 text-emerald-600 text-xs font-bold">✓</span>
                                  ) : (
                                    <span className="inline-flex h-5 w-5 items-center justify-center rounded-full bg-red-100 text-red-600 text-xs font-bold">✗</span>
                                  )}
                                  {pageExists ? String(svc.url || "Page exists") : "Missing page"}
                                </span>
                              </td>
                              <td className="px-3 py-2">{pageExists ? `${depth} (${wordCount} words)` : "—"}</td>
                              <td className="px-3 py-2">{`${inboundBodyOnly} · ${wiring}`}</td>
                              <td className="px-3 py-2">{tier}</td>
                              <td className="px-3 py-2">
                                <p>
                                  <span className={`inline-flex items-center rounded-full border px-2 py-0.5 text-xs font-semibold ${statusBadgeClass(predictionStatus)}`}>
                                    {predictionStatus}
                                  </span>
                                </p>
                                {Object.keys(coreRules).length > 0 && (
                                  <details className="mt-1 text-xs text-[var(--text-secondary)]">
                                    <summary className="cursor-pointer select-none">Validation details</summary>
                                    <div className="mt-1 space-y-1">
                                      {Object.entries(coreRules).map(([k, v]) => (
                                        <p key={k}>
                                          {k}: {v ? "pass" : "fail"}
                                        </p>
                                      ))}
                                    </div>
                                  </details>
                                )}
                              </td>
                              <td className="px-3 py-2">
                                {`CTA ${Number(conv.cta_count || 0)} · Booking ${Boolean(conv.booking_link) ? "Yes" : "No"} · Links ${Number(conv.internal_links || 0)} · Financing ${Boolean(conv.financing_mentioned) ? "Yes" : "No"}`}
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                ) : allServices.length > 0 && (
                  <div className="overflow-x-auto rounded-[var(--radius-md)] border border-[var(--border-default)]">
                    <table className="min-w-full text-left text-xs sm:text-sm">
                      <thead className="bg-slate-50 text-[var(--text-muted)]">
                        <tr>
                          <th className="px-3 py-2 font-medium">Service</th>
                          <th className="px-3 py-2 font-medium">Landing Page</th>
                          <th className="px-3 py-2 font-medium text-[var(--text-muted)]">Wiring (bonus)</th>
                          <th className="px-3 py-2 font-medium">Revenue Impact</th>
                        </tr>
                      </thead>
                      <tbody>
                        {allServices.map((service) => {
                          const hasPage = !missingLandingPages.some((s) => s.toLowerCase() === service.toLowerCase());
                          return (
                            <tr key={service} className="border-t border-[var(--border-default)]">
                              <td className="px-3 py-2">{service}</td>
                              <td className="px-3 py-2">
                                <span className="inline-flex items-center gap-2">
                                  {hasPage ? (
                                    <span className="inline-flex h-5 w-5 items-center justify-center rounded-full bg-emerald-100 text-emerald-600 text-xs font-bold">✓</span>
                                  ) : (
                                    <span className="inline-flex h-5 w-5 items-center justify-center rounded-full bg-red-100 text-red-600 text-xs font-bold">✗</span>
                                  )}
                                  {hasPage ? "Page exists" : "Missing page"}
                                </span>
                              </td>
                              <td className="px-3 py-2">—</td>
                              <td className="px-3 py-2">{serviceUpsideMap.get(service.toLowerCase()) || "—"}</td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                )}
                {highValueServices.length > 0 && (
                  <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
                    <MetricCard label="Total high-value services" value={String(highValueSummary.total_high_value_services ?? "—")} />
                    <MetricCard label="Coverage ratio" value={highValueSummary.service_coverage_ratio != null ? String(highValueSummary.service_coverage_ratio) : "—"} />
                    <MetricCard label="Optimized ratio" value={highValueSummary.optimized_ratio != null ? String(highValueSummary.optimized_ratio) : "—"} />
                    <MetricCard label="Leverage" value={highValueLeverage} tone={highValueLeverage === "high" ? "warn" : "good"} />
                  </div>
                )}
              </BriefSection>
            ) : null}

            <BriefSection title="Share of Voice">
              <p className="text-[var(--text-secondary)]">Local competitive position snapshot.</p>
              <div className="overflow-x-auto rounded-[var(--radius-md)] border border-[var(--border-default)]">
                <table className="min-w-full text-left text-xs sm:text-sm">
                  <thead className="bg-slate-50 text-[var(--text-muted)]">
                    <tr>
                      <th className="px-3 py-2 font-medium">Name</th>
                      <th className="px-3 py-2 font-medium">Reviews</th>
                      <th className="px-3 py-2 font-medium">Distance</th>
                      <th className="px-3 py-2 font-medium">Notes</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr className="border-t border-[var(--border-default)] bg-emerald-50/40">
                      <td className="px-3 py-2 font-semibold">{result.business_name} (You)</td>
                      <td className="px-3 py-2">{mp?.reviews || "—"}</td>
                      <td className="px-3 py-2">—</td>
                      <td className="px-3 py-2">{result.review_position || "Lead baseline"}</td>
                    </tr>
                    {sg?.competitor_name && (
                      <tr className="border-t border-[var(--border-default)]">
                        <td className="px-3 py-2">{sg.competitor_name}</td>
                        <td className="px-3 py-2">{sg.competitor_reviews ?? "—"}</td>
                        <td className="px-3 py-2">{formatMiles(sg.distance_miles)}</td>
                        <td className="px-3 py-2">Nearest competitor</td>
                      </tr>
                    )}
                    {csg?.competitor_name && csg.competitor_name !== sg?.competitor_name && (
                      <tr className="border-t border-[var(--border-default)]">
                        <td className="px-3 py-2">{csg.competitor_name}</td>
                        <td className="px-3 py-2">{csg.competitor_reviews ?? "—"}</td>
                        <td className="px-3 py-2">{formatMiles(csg.distance_miles)}</td>
                        <td className="px-3 py-2">{csg.service ? `${csg.service} gap` : "Service gap competitor"}</td>
                      </tr>
                    )}
                    {parsedCompetitorRows.map((item, i) => (
                      <tr key={`cc-${i}`} className="border-t border-[var(--border-default)]">
                        <td className="px-3 py-2">{item.name}</td>
                        <td className="px-3 py-2">{item.reviews}</td>
                        <td className="px-3 py-2">{item.distance}</td>
                        <td className="px-3 py-2">{item.notes}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </BriefSection>

            <BriefSection title="Review Authority & Velocity" sectionId="review-authority">
              <div className="grid gap-3 md:grid-cols-3">
                <MetricCard label="Reviews" value={mp?.reviews || "—"} status={belowLocalAverage ? "Need more vs local avg" : (mp?.local_avg ? "In line with local avg" : undefined)} tone={belowLocalAverage ? "warn" : "good"} />
                <MetricCard label="Last Review" value={ds?.last_review_days_ago != null ? `${ds.last_review_days_ago} days ago` : "—"} />
                <MetricCard label="Velocity" value={ds?.review_velocity_30d != null ? `${ds.review_velocity_30d} in 30d` : "—"} />
              </div>
              {reviewIntel?.summary && <p>{String(reviewIntel.summary)}</p>}
              {Object.keys(reviewComplaintThemes).length > 0 && (
                <p>
                  <strong>Complaint themes:</strong>{" "}
                  {Object.entries(reviewComplaintThemes).map(([k, v]) => `${k}: ${v}`).join(", ")}
                </p>
              )}
            </BriefSection>

            <BriefSection title="Validated Revenue Uplift">
              <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                {rucg?.primary_service && (
                  <UpliftCard
                    title={`Optimized ${rucg.primary_service} page`}
                    amount={rucg.annual_low != null ? `$${rucg.annual_low.toLocaleString()}-$${(rucg.annual_high ?? 0).toLocaleString()}` : "—"}
                    subtitle={`${rucg.consult_low ?? "—"}-${rucg.consult_high ?? "—"} consults/mo`}
                  />
                )}
                {(ht?.service_level_upside ?? []).map((svc, idx) => (
                  <UpliftCard key={idx} title={svc.service || "Service lever"} amount={svc.upside || "—"} />
                ))}
                <UpliftCard
                  title="Total Projected Uplift"
                  amount={ed?.modeled_revenue_upside || "—"}
                  subtitle="Modeled annual upside"
                  tone="total"
                />
              </div>
              {rucg?.gap_service && <p><strong>Competitive gap service:</strong> {rucg.gap_service}</p>}
              {rucg?.method_note && <p className="text-xs text-[var(--text-muted)]">{rucg.method_note}</p>}
            </BriefSection>

            {planItems.length > 0 && (
              <BriefSection title={`Intervention Plan (${planItems.length} steps)`}>
                <div className="space-y-2">
                  {planItems.map((item) => (
                    <InterventionStepCard key={item.step} step={item.step} category={item.category} action={item.action} href={suggestionHrefFromCategory(item.category)} />
                  ))}
                </div>
              </BriefSection>
            )}

            {((ci && (ci.online_booking != null || ci.contact_form != null || ci.phone_prominent != null || ci.mobile_optimized != null || ci.page_load_ms != null)) || convStruct) && (
              <BriefSection title="Conversion Infrastructure" sectionId="conversion-infrastructure">
                <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-6">
                  <MetricCard label="Contact Form" value={boolValueLabel(ci?.contact_form)} tone={ci?.contact_form ? "good" : "warn"} />
                  <MetricCard label="Calendar / Booking" value={boolValueLabel(ci?.online_booking)} tone={ci?.online_booking ? "good" : "warn"} />
                  <MetricCard label="Phone Clickable" value={boolValueLabel(convStruct?.phone_clickable ?? ci?.phone_prominent)} tone={(convStruct?.phone_clickable ?? ci?.phone_prominent) ? "good" : "warn"} />
                  <MetricCard label="Page Load" value={ci?.page_load_ms != null ? `${ci.page_load_ms} ms` : "—"} />
                  <MetricCard label="Form Structure" value={convStruct?.form_single_or_multi_step != null ? conversionFormValue(convStruct.form_single_or_multi_step) : "—"} />
                  <MetricCard label="CTA Count" value={convStruct?.cta_count != null ? String(convStruct.cta_count) : "—"} />
                </div>
                <p className="text-xs text-[var(--text-muted)]">
                  <strong>CTA labels detected:</strong> {ctaLabels.length ? ctaLabels.join(", ") : "None detected"}
                </p>
              </BriefSection>
            )}

            {(mp?.revenue_band || mp?.reviews || mp?.local_avg || mp?.market_density || result.review_position || result.market_density) ? (
              <BriefSection title="Market Position (Full Detail)" sectionId="market-position-full">
                <KV label="Revenue Band" value={mp?.revenue_band} />
                {mp?.revenue_band_method && <p className="text-xs text-zinc-500">{mp.revenue_band_method}</p>}
                <KV label="Reviews" value={mp?.reviews} />
                <KV label="Local Avg" value={mp?.local_avg} />
                <KV label="Market Density" value={mp?.market_density ?? result.market_density} />
                {!mp && result.review_position && <KV label="Review Position" value={result.review_position} />}
              </BriefSection>
            ) : null}

            {(cc?.line1 || cc?.line2 || cc?.line3) ? (
              <BriefSection title="Competitive Context (Full Detail)" sectionId="competitive-context-full">
                {cc?.line1 && <p>{cc.line1}</p>}
                {cc?.line2 && <p>{cc.line2}</p>}
                {cc?.line3 && <p>{cc.line3}</p>}
                {ccLine3Items.length > 0 && (
                  <ul className="list-inside list-disc space-y-1">
                    {ccLine3Items.map((item, i) => <li key={i}>{item}</li>)}
                  </ul>
                )}
              </BriefSection>
            ) : null}

            {csg && (csg.service || csg.competitor_name) ? (
              <BriefSection title="Competitive Service Gap (Full Detail)">
                <KV label="Type" value={csg.type ?? "High-Margin Capture Gap"} />
                <KV label="Service" value={csg.service} />
                <KV label="Nearest competitor" value={csg.competitor_name} />
                {csg.competitor_reviews != null && <KV label="Competitor reviews" value={String(csg.competitor_reviews)} />}
                {csg.lead_reviews != null && <KV label="Lead reviews" value={String(csg.lead_reviews)} />}
                {csg.distance_miles != null && <KV label="Distance" value={`${csg.distance_miles} mi`} />}
                {csg.schema_missing && <KV label="Schema" value="Missing" />}
              </BriefSection>
            ) : null}

            {cd ? (
              <BriefSection title="Competitive Delta (Full Detail)" sectionId="competitive-delta-full">
                <KV
                  label="Service pages"
                  value={
                    cd.competitor_avg_service_pages != null
                      ? `${cd.target_service_page_count ?? 0} pages with service-like paths (for example, /implants, /cosmetic) vs competitor avg ${Number(cd.competitor_avg_service_pages).toFixed(1)}`
                      : `Target ${cd.target_service_page_count ?? 0} pages with service-like paths (e.g. /implants, /cosmetic).`
                  }
                />
                {cd.target_pages_with_faq_schema != null && (
                  <KV
                    label="FAQ/structured trust coverage"
                    value={
                      cd.competitor_avg_pages_with_schema != null
                        ? `${cd.target_pages_with_faq_schema} of ${cd.target_service_page_count ?? 0} service pages vs competitor avg ${Number(cd.competitor_avg_pages_with_schema).toFixed(1)} pages`
                        : `${cd.target_pages_with_faq_schema} of ${cd.target_service_page_count ?? 0} service pages`
                    }
                  />
                )}
                <KV
                  label="Service page depth"
                  value={
                    cd.competitor_avg_word_count != null
                      ? `Average word count across ${cd.target_service_page_count ?? 0} service pages: ~${Math.round(Number(cd.target_avg_word_count_service_pages ?? 0))} (min ${cd.target_min_word_count_service_pages ?? "N/A"}, max ${cd.target_max_word_count_service_pages ?? "N/A"}) vs competitor avg ~${Math.round(Number(cd.competitor_avg_word_count))}`
                      : cd.target_avg_word_count_service_pages != null
                        ? `Average word count across ${cd.target_service_page_count ?? 0} service pages: ~${Math.round(Number(cd.target_avg_word_count_service_pages))} (min ${cd.target_min_word_count_service_pages ?? "N/A"}, max ${cd.target_max_word_count_service_pages ?? "N/A"})`
                        : undefined
                  }
                />
                <p className="text-xs text-zinc-500">
                  {cd.competitors_sampled ? `Based on ${cd.competitors_sampled} nearby competitors.` : "Target-only snapshot for this run."}
                </p>
                {cd.competitor_site_metrics_count != null && Number(cd.competitor_site_metrics_count) > 0 && (
                  <p className="text-xs text-zinc-500">
                    Competitor averages from {String(cd.competitor_site_metrics_count)} competitor sites crawled.
                  </p>
                )}
                {cd.competitor_avg_service_pages == null && (
                  <p className="text-xs text-zinc-500">
                    {String(cd.competitor_crawl_note || "Competitor site metrics were not run for this brief; only target metrics are shown.")}
                  </p>
                )}
              </BriefSection>
            ) : null}

            {(ds?.google_ads_line != null || ds?.meta_ads_line != null || ds?.organic_visibility_tier || ds?.last_review_days_ago != null || ds?.review_velocity_30d != null) || (!ds && result.paid_status) ? (
              <BriefSection title="Demand Signals (Full Detail)" sectionId="demand-signals-full">
                <KV label="Google Ads" value={ds?.google_ads_line} />
                {ds?.google_ads_source && <p className="text-xs text-zinc-500">Source: {ds.google_ads_source}</p>}
                <KV label="Meta Ads" value={ds?.meta_ads_line} />
                {ds?.meta_ads_source && <p className="text-xs text-zinc-500">Source: {ds.meta_ads_source}</p>}
                {ds?.paid_channels_detected?.length ? <KV label="Paid channels detected" value={ds.paid_channels_detected.join(", ")} /> : null}
                {ds?.organic_visibility_tier && (
                  <KV label="Organic Visibility" value={`${ds.organic_visibility_tier}${ds.organic_visibility_reason ? ` — ${ds.organic_visibility_reason}` : ""}`} />
                )}
                {ds?.last_review_days_ago != null && <KV label="Last Review" value={`~${ds.last_review_days_ago} days ago`} />}
                {ds?.review_velocity_30d != null && <KV label="Review Velocity" value={`~${ds.review_velocity_30d} in last 30 days`} />}
                {!ds && result.paid_status && <KV label="Paid Status" value={result.paid_status} />}
              </BriefSection>
            ) : null}

            {sg && sg.competitor_name ? (
              <BriefSection title="Strategic Gap (Full Detail)">
                <p>Nearest competitor {sg.competitor_name} is {formatMiles(sg.distance_miles)} away and has {sg.competitor_reviews ?? "—"} reviews.</p>
                <p>Market density: {sg.market_density ?? "High"}. This practice&apos;s review position is {strategicReviewComparison}.</p>
              </BriefSection>
            ) : null}

            {marketSat ? (
              <BriefSection title="Market Saturation">
                {marketSat.top_5_avg_reviews != null && <KV label="Top 5 avg reviews" value={String(marketSat.top_5_avg_reviews)} />}
                {marketSat.competitor_median_reviews != null && marketSat.target_gap_from_median != null && (
                  <KV label="Median comparison" value={`Median ${marketSat.competitor_median_reviews}; target is ${marketSat.target_gap_from_median}`} />
                )}
              </BriefSection>
            ) : null}

            {geo ? (
              <BriefSection title="Geographic Coverage">
                {geo.city_or_near_me_page_count != null && <KV label="City/near-me pages" value={`${geo.city_or_near_me_page_count} detected`} />}
                {geo.city_or_near_me_page_count != null && (
                  <p className="text-xs text-zinc-500">(URLs with city name or &apos;near me&apos; in path/title).</p>
                )}
                {geo.has_multi_location_page != null && <KV label="Multi-location page" value={geo.has_multi_location_page ? "Detected" : "Not detected"} />}
              </BriefSection>
            ) : null}

            {(b?.risk_flags?.length || result.risk_flags?.length) ? (
              <BriefSection title="Risk Flags">
                <ul className="list-inside list-disc space-y-1">
                  {(b?.risk_flags ?? result.risk_flags ?? []).map((f, i) => <li key={i}>{f}</li>)}
                </ul>
              </BriefSection>
            ) : null}

            {(b?.evidence_bullets?.length || result.evidence?.length) ? (
              <BriefSection title="Evidence">
                <ul className="list-inside list-disc space-y-1 text-zinc-700">
                  {b?.evidence_bullets?.length
                    ? b.evidence_bullets.map((e, i) => <li key={i}>{e}</li>)
                    : (result.evidence ?? []).map((e, i) => <li key={i}><strong>{e.label}:</strong> {e.value}</li>)}
                </ul>
              </BriefSection>
            ) : null}
          </div>
        </Card>
      </main>

      {addListOpen && (
        <ListPickerModal
          open={addListOpen}
          title="Add this lead to list"
          lists={lists}
          busy={addingToList}
          onClose={() => {
            if (addingToList) return;
            setAddListOpen(false);
          }}
          onConfirm={handleAddToList}
        />
      )}
    </div>
  );
}

function BriefSection({ title, children, sectionId }: { title: string; children: React.ReactNode; sectionId?: string }) {
  return (
    <div id={sectionId} className="rounded-[var(--radius-md)] border border-[var(--border-default)] bg-[var(--bg-card)] px-4 py-3 scroll-mt-24">
      <h3 className="mb-2 text-sm font-semibold text-[var(--text-primary)]">{title}</h3>
      <div className="space-y-1.5 text-[var(--text-secondary)]">{children}</div>
    </div>
  );
}

function MetricCard({
  label,
  value,
  status,
  tone = "neutral",
}: {
  label: string;
  value: string;
  status?: string;
  tone?: "good" | "warn" | "neutral";
}) {
  const toneClass = tone === "good"
    ? "border-emerald-200 bg-emerald-50/40"
    : tone === "warn"
      ? "border-amber-200 bg-amber-50/40"
      : "border-[var(--border-default)] bg-[var(--bg-card)]";
  return (
    <div className={`rounded-[var(--radius-md)] border p-3 ${toneClass}`}>
      <p className="text-xs uppercase tracking-wide text-[var(--text-muted)]">{label}</p>
      <p className="mt-1 text-base font-semibold text-[var(--text-primary)]">{value || "—"}</p>
      {status && <p className="mt-1 text-xs text-[var(--text-secondary)]">{status}</p>}
    </div>
  );
}

function UpliftCard({
  title,
  amount,
  subtitle,
  tone = "normal",
}: {
  title: string;
  amount: string;
  subtitle?: string;
  tone?: "normal" | "total";
}) {
  const classes = tone === "total"
    ? "border-emerald-300 bg-emerald-50"
    : "border-[var(--border-default)] bg-[var(--bg-card)]";
  return (
    <div className={`rounded-[var(--radius-md)] border p-3 ${classes}`}>
      <p className="text-xs uppercase tracking-wide text-[var(--text-muted)]">{title}</p>
      <p className="mt-1 text-base font-semibold text-[var(--text-primary)]">{amount}</p>
      {subtitle && <p className="text-xs text-[var(--text-secondary)]">{subtitle}</p>}
    </div>
  );
}

function InterventionStepCard({
  step,
  category,
  action,
  href,
}: {
  step: number;
  category: string;
  action: string;
  href: string;
}) {
  const cat = category.toLowerCase();
  const tone = cat.includes("critical")
    ? "border-red-200 bg-red-50/40"
    : cat.includes("important") || cat.includes("conversion")
      ? "border-amber-200 bg-amber-50/40"
      : cat.includes("strategic") || cat.includes("demand") || cat.includes("capture") || cat.includes("trust")
        ? "border-blue-200 bg-blue-50/40"
        : "border-yellow-200 bg-yellow-50/40";
  return (
    <div className={`rounded-[var(--radius-md)] border px-3 py-2 ${tone}`}>
      <div className="flex flex-wrap items-center justify-between gap-2">
        <p className="font-semibold text-[var(--text-primary)]">Step {step}: {category}</p>
        <a href={href} className="text-xs font-medium text-[var(--text-secondary)] underline underline-offset-2">View suggestions</a>
      </div>
      <p className="mt-1 text-[var(--text-secondary)]">{action}</p>
    </div>
  );
}

function KV({ label, value }: { label: string; value?: string | null }) {
  if (value == null || value === "") return null;
  return <p><strong>{label}:</strong> {value}</p>;
}

function statusBadgeClass(status: string): string {
  const s = (status || "").toLowerCase();
  if (s === "strong") return "border-emerald-200 bg-emerald-50 text-emerald-700";
  if (s === "moderate") return "border-blue-200 bg-blue-50 text-blue-700";
  if (s === "weak" || s === "weak_stub_page") return "border-amber-200 bg-amber-50 text-amber-700";
  if (s === "umbrella_only" || s === "rejected_non_service" || s === "missing") return "border-rose-200 bg-rose-50 text-rose-700";
  return "border-[var(--border-default)] bg-[var(--bg-card)] text-[var(--text-secondary)]";
}
