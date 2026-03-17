"use client";

import { type ReactNode, useEffect, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import {
  addProspectsToList,
  createProspectList,
  ensureAskProspectBrief,
  getAskResults,
  getJobStatus,
  getProspectLists,
  runAskQuery,
} from "@/lib/api";
import { useAuth } from "@/lib/auth";
import { clientFacingAppError, clientFacingBriefError } from "@/lib/present";
import type { ProspectList } from "@/lib/types";
import Button from "@/app/components/ui/Button";
import Badge from "@/app/components/ui/Badge";
import { Card, CardBody } from "@/app/components/ui/Card";
import Textarea from "@/app/components/ui/Textarea";
import EmptyState from "@/app/components/ui/EmptyState";
import ListPickerModal from "@/app/components/ListPickerModal";
import BriefBuildProgress, { type BriefBuildProgressState } from "@/app/components/BriefBuildProgress";
import { BorderTrail } from "@/components/ui/border-trail";

type AskProspect = {
  diagnostic_id?: number | null;
  place_id?: string | null;
  business_name?: string;
  city?: string;
  state?: string;
  website?: string | null;
  rating?: number;
  user_ratings_total?: number;
  opportunity_profile?: string;
  primary_leverage?: string;
  ai_explanation?: string;
  match_evidence_level?: "deep_verified" | "lightweight_verified" | "deterministic" | "inferred" | null;
  match_evidence?: Array<{
    criterion_key?: string;
    criterion_type?: string;
    service?: string | null;
    source?: string;
    matched?: boolean;
    details?: Record<string, unknown> | null;
  }> | null;
};

const ASK_RESULTS_STORAGE_KEY = "ask_results";

type AskResultsCache = {
  query: string;
  headline: string | null;
  results: AskProspect[];
  applied_criteria?: string[];
  unsupported_parts?: string[];
  unsupported_message?: string | null;
  no_results_summary?: {
    total_scanned?: number;
    filtered_out_by_criterion?: Record<string, number>;
    criterion_that_eliminated_most?: string | null;
    no_results_suggestion?: string | null;
  } | null;
  agentic_iterations?: Array<Record<string, unknown>>;
  job_id?: string | null;
  saved_at: string;
};

type AskProgressState = {
  phase: string;
  candidates: number;
  scored: number;
  listed: number;
  iteration: number;
  maxIterations: number;
};

type AskProgressEvent = {
  id: string;
  label: string;
  detail: string;
};

function askBriefReady(row: AskProspect): boolean {
  return Boolean(row.diagnostic_id);
}

const promptIdeas = [
  "Find 10 dentists in San Jose, CA with missing implants page",
  "Find dentists in Austin, TX with strong demand but weak service depth",
  "Find practices in Miami, FL below local review average with weak conversion paths",
  "Find high-opportunity dentists in Phoenix, AZ with shallow Invisalign coverage",
];

function normalizeAskError(error: string | null) {
  const raw = String(error || "").trim();
  const lower = raw.toLowerCase();

  if (lower.includes("we can't process this query") || lower.includes("please use a location")) {
    return {
      title: "Tighten the request",
      description: "Ask Neyma needs a city and state, plus one supported filter, before the AI ranking flow can build the shortlist.",
      hints: [
        "Use City, ST: San Jose, CA",
        "Use supported filters like review gap, website quality, or missing service page",
        "Example: Find dentists in Phoenix, AZ with shallow Invisalign coverage",
      ],
    };
  }

  if (!raw) {
    return {
      title: "Request could not run",
      description: "Try again with a more specific query.",
      hints: [],
    };
  }

  return {
    title: "Request could not run",
    description: raw,
    hints: [],
  };
}

function formatPhaseLabel(phase: string) {
  const normalized = phase.replaceAll("_", " ").trim();
  if (!normalized) return "Preparing request";

  const lower = normalized.toLowerCase();
  if (lower.includes("candidate fetch")) return "Discovering candidates";
  if (lower.includes("score")) return "Scoring opportunity signals";
  if (lower.includes("list")) return "Building shortlist";
  if (lower.includes("iter")) return "Refining the next pass";
  if (lower.includes("complete")) return "Finalizing results";

  return normalized.charAt(0).toUpperCase() + normalized.slice(1);
}

function formatEvidenceLevel(level: AskProspect["match_evidence_level"]) {
  switch (level) {
    case "deep_verified":
      return "Deep verified";
    case "lightweight_verified":
      return "Fast verified";
    case "deterministic":
      return "Signal matched";
    default:
      return "Inferred";
  }
}

function evidenceLevelClassName(level: AskProspect["match_evidence_level"]) {
  switch (level) {
    case "deep_verified":
      return "border-emerald-200 bg-emerald-50 text-emerald-700";
    case "lightweight_verified":
      return "border-sky-200 bg-sky-50 text-sky-700";
    case "deterministic":
      return "border-amber-200 bg-amber-50 text-amber-700";
    default:
      return "border-slate-200 bg-slate-100 text-slate-700";
  }
}

function humanizeCriterionLabel(item: NonNullable<AskProspect["match_evidence"]>[number]) {
  const type = String(item.criterion_type || "").trim();
  const service = String(item.service || "").trim();

  if (type === "missing_service_page") {
    return service ? `Missing ${service.replaceAll("_", " ")} page` : "Missing service page";
  }

  return (type || "matched signal").replaceAll("_", " ");
}

function formatEvidenceSource(source: string | undefined) {
  switch (source) {
    case "deep_verified_diagnostic":
      return "deep check";
    case "lightweight_check":
      return "fast check";
    case "deterministic_signal":
      return "signal";
    default:
      return "inference";
  }
}

function askUsageMessage(access: ReturnType<typeof useAuth>["access"]): ReactNode {
  if (!access) return null;
  if (access.viewer.is_guest) {
    return "Ask Neyma is available after signup so the shortlist can be saved and reopened inside the workspace.";
  }
  if (String(access.plan_tier) === "free") {
    return `${access.remaining.ask ?? 0} of ${access.limits.ask ?? 0} Ask Neyma requests left this month on the free plan.`;
  }
  return (
    <>
      <span className="font-semibold text-[var(--text-primary)]">{String(access.plan_tier).toUpperCase()} Plan:</span>{" "}
      <span className="font-semibold text-[var(--text-primary)]">Ask Neyma</span> is available without a usage cap.
    </>
  );
}

export default function AskPage() {
  const router = useRouter();
  const { access, accessLoading } = useAuth();
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [messageHref, setMessageHref] = useState<string | null>(null);
  const [messageCta, setMessageCta] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [progressState, setProgressState] = useState<AskProgressState | null>(null);
  const [progressEvents, setProgressEvents] = useState<AskProgressEvent[]>([]);
  const [results, setResults] = useState<AskProspect[]>([]);
  const [headline, setHeadline] = useState<string | null>(null);
  const [appliedCriteria, setAppliedCriteria] = useState<string[]>([]);
  const [unsupportedParts, setUnsupportedParts] = useState<string[]>([]);
  const [unsupportedMessage, setUnsupportedMessage] = useState<string | null>(null);
  const [noResultsSummary, setNoResultsSummary] = useState<AskResultsCache["no_results_summary"]>(null);
  const [pendingIntent, setPendingIntent] = useState<Record<string, unknown> | null>(null);
  const [requiresConfirmation, setRequiresConfirmation] = useState(false);
  const [agenticIterations, setAgenticIterations] = useState<Array<Record<string, unknown>>>([]);
  const [ensuringKey, setEnsuringKey] = useState<string | null>(null);
  const [lists, setLists] = useState<ProspectList[]>([]);
  const [listModalOpen, setListModalOpen] = useState(false);
  const [listBusy, setListBusy] = useState(false);
  const [listTargetDiagnosticId, setListTargetDiagnosticId] = useState<number | null>(null);
  const [listTargetBusinessName, setListTargetBusinessName] = useState<string>("");
  const [briefProgress, setBriefProgress] = useState<BriefBuildProgressState | null>(null);
  const readyBriefCount = results.filter((row) => askBriefReady(row)).length;
  const pendingBriefCount = Math.max(0, results.length - readyBriefCount);
  const askLimitReached = !accessLoading && access?.can_use.ask === false;
  const usageMessage = askUsageMessage(access);

  function applyPrompt(next: string) {
    setQuery(next);
    setError(null);
  }

  function updateProgress(next: AskProgressState) {
    setProgressState(next);
    setProgressEvents((prev) => {
      const last = prev[0];
      const nextLabel = formatPhaseLabel(next.phase);
      const nextDetail = buildProgressEventDetail(next);
      if (last && last.label === nextLabel && last.detail === nextDetail) {
        return prev;
      }

      return [
        {
          id: `${Date.now()}-${next.phase}-${next.iteration}-${next.candidates}-${next.scored}-${next.listed}`,
          label: nextLabel,
          detail: nextDetail,
        },
        ...prev,
      ].slice(0, 5);
    });
    setMessage(
      `${next.phase.replaceAll("_", " ")} · Found ${next.candidates}, scored ${next.scored}, listed ${next.listed}${next.iteration ? ` · Iteration ${next.iteration}${next.maxIterations ? `/${next.maxIterations}` : ""}` : ""}`,
    );
  }

  useEffect(() => {
    setQuery("");
  }, []);

  useEffect(() => {
    try {
      const raw = sessionStorage.getItem(ASK_RESULTS_STORAGE_KEY);
      if (!raw) return;
      const parsed = JSON.parse(raw) as AskResultsCache;
      const savedAtMs = Date.parse(parsed.saved_at || "");
      if (Number.isNaN(savedAtMs) || Date.now() - savedAtMs > 24 * 60 * 60 * 1000) {
        sessionStorage.removeItem(ASK_RESULTS_STORAGE_KEY);
        return;
      }
      if (Array.isArray(parsed.results) && parsed.results.length > 0) {
        setResults(parsed.results);
        setHeadline(parsed.headline || null);
        setAppliedCriteria(Array.isArray(parsed.applied_criteria) ? parsed.applied_criteria : []);
        setUnsupportedParts(Array.isArray(parsed.unsupported_parts) ? parsed.unsupported_parts : []);
        setUnsupportedMessage(parsed.unsupported_message || null);
        setNoResultsSummary(parsed.no_results_summary || null);
        setAgenticIterations(Array.isArray(parsed.agentic_iterations) ? parsed.agentic_iterations : []);
      }
    } catch {
      // ignore malformed cache
    }
  }, []);

  useEffect(() => {
    if (results.length === 0) return;
    const payload: AskResultsCache = {
      query,
      headline,
      results,
      applied_criteria: appliedCriteria,
      unsupported_parts: unsupportedParts,
      unsupported_message: unsupportedMessage,
      no_results_summary: noResultsSummary,
      agentic_iterations: agenticIterations,
      saved_at: new Date().toISOString(),
    };
    sessionStorage.setItem(ASK_RESULTS_STORAGE_KEY, JSON.stringify(payload));
  }, [agenticIterations, appliedCriteria, headline, noResultsSummary, query, results, unsupportedMessage, unsupportedParts]);

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

  function clearResults() {
    setResults([]);
    setHeadline(null);
    setAppliedCriteria([]);
    setUnsupportedParts([]);
    setUnsupportedMessage(null);
    setNoResultsSummary(null);
    setPendingIntent(null);
    setRequiresConfirmation(false);
    setAgenticIterations([]);
    setMessage(null);
    setMessageHref(null);
    setMessageCta(null);
    setError(null);
    setProgressState(null);
    setProgressEvents([]);
    setBriefProgress(null);
    sessionStorage.removeItem(ASK_RESULTS_STORAGE_KEY);
  }

  function markAskBriefReady(row: AskProspect, diagnosticId: number) {
    setResults((prev) => prev.map((candidate) => {
      const samePlace = row.place_id && candidate.place_id && row.place_id === candidate.place_id;
      const sameIdentity = (candidate.business_name || "") === (row.business_name || "")
        && (candidate.city || "") === (row.city || "")
        && (candidate.state || "") === (row.state || "");
      return samePlace || sameIdentity
        ? { ...candidate, diagnostic_id: diagnosticId }
        : candidate;
    }));
  }

  async function executeRun(confirmedLowConfidence = false) {
    if (askLimitReached) {
      setError(access?.viewer.is_guest ? "Ask Neyma requires a free account." : "Free plan Ask Neyma limit reached for this month.");
      return;
    }
    setError(null);
    setResults([]);
    setHeadline(null);
    setAppliedCriteria([]);
    setUnsupportedParts([]);
    setUnsupportedMessage(null);
    setNoResultsSummary(null);
    setPendingIntent(null);
    setRequiresConfirmation(false);
    setAgenticIterations([]);
    setProgressState(null);
    setProgressEvents([]);
    setBriefProgress(null);
    sessionStorage.removeItem(ASK_RESULTS_STORAGE_KEY);
    setLoading(true);
    setMessageHref(null);
    setMessageCta(null);
    try {
      const start = await runAskQuery(query, confirmedLowConfidence);
      if (start.requires_confirmation || start.status === "requires_confirmation") {
        setPendingIntent((start.normalized_intent || start.intent || {}) as Record<string, unknown>);
        setRequiresConfirmation(true);
        setMessage(start.question || start.message || "Please confirm intent before running.");
        if (Array.isArray(start.unsupported_parts)) {
          setUnsupportedParts(start.unsupported_parts);
        }
        setLoading(false);
        return;
      }
      if (!start.job_id) {
        throw new Error("Ask job did not start");
      }
      setMessage(start.message || null);

      let completed = false;
      const maxLoops = 360;
      const sleepMs = 1500;
      for (let i = 0; i < maxLoops; i++) {
        const st = await getJobStatus(start.job_id);
        const progress = (st.progress || {}) as Record<string, unknown>;
        const phase = String(progress.phase || "running");
        const p = (progress.progress || {}) as Record<string, unknown>;
        const candidates = Number(p.candidates_found || 0);
        const scored = Number(p.scored || 0);
        const listed = Number(p.list_count || 0);
        const iteration = Number(p.iteration || 0);
        const maxIterations = Number(p.max_iterations || 0);
        updateProgress({
          phase,
          candidates,
          scored,
          listed,
          iteration,
          maxIterations,
        });

        const partial = Array.isArray(progress.partial_results) ? (progress.partial_results as AskProspect[]) : [];
        if (partial.length > 0) {
          setResults(partial);
        }

        if (st.status === "completed") {
          completed = true;
          break;
        }
        if (st.status === "failed") throw new Error(st.error || "Request failed");
        await new Promise((r) => setTimeout(r, sleepMs));
      }

      if (!completed) throw new Error("Ask query timed out");

      const out = await getAskResults(start.job_id);
      const result = out.result || {};
      const prospects = Array.isArray((result as Record<string, unknown>).prospects)
        ? ((result as Record<string, unknown>).prospects as AskProspect[])
        : [];
      const applied = Array.isArray((result as Record<string, unknown>).applied_criteria)
        ? ((result as Record<string, unknown>).applied_criteria as string[])
        : [];
      const unsupported = (result as Record<string, unknown>).unsupported_message;
      const unsupportedPartsRaw = Array.isArray((result as Record<string, unknown>).unsupported_parts)
        ? ((result as Record<string, unknown>).unsupported_parts as string[])
        : [];
      const noResults = {
        total_scanned: Number((result as Record<string, unknown>).total_scanned || 0),
        filtered_out_by_criterion: ((result as Record<string, unknown>).filtered_out_by_criterion || {}) as Record<string, number>,
        criterion_that_eliminated_most: ((result as Record<string, unknown>).criterion_that_eliminated_most || null) as string | null,
        no_results_suggestion: ((result as Record<string, unknown>).no_results_suggestion || null) as string | null,
      };
      const iterations = Array.isArray((result as Record<string, unknown>).iterations)
        ? ((result as Record<string, unknown>).iterations as Array<Record<string, unknown>>)
        : [];
      setAgenticIterations(iterations);
      if (iterations.length > 0) {
        const last = iterations[iterations.length - 1] || {};
        setMessage(`Iteration ${iterations.length}: ${Number(last.prefilter_count || 0)} scanned -> ${Number(last.postfilter_count || 0)} matched`);
      }
      setResults(prospects);
      setAppliedCriteria(applied);
      setUnsupportedParts(unsupportedPartsRaw);
      setUnsupportedMessage(typeof unsupported === "string" && unsupported.trim() ? unsupported : null);
      setNoResultsSummary(noResults);
      const total = Number((result as Record<string, unknown>).total_matches || prospects.length);
      const intent = (result as Record<string, unknown>).intent as Record<string, unknown> | undefined;
      setHeadline(
        `Found ${total} matches${intent?.city ? ` in ${String(intent.city)}${intent?.state ? `, ${String(intent.state)}` : ""}` : ""}.`,
      );
      setProgressState(null);
      setProgressEvents([]);
      setMessage("Done.");
      sessionStorage.setItem(ASK_RESULTS_STORAGE_KEY, JSON.stringify({
        query,
        headline: `Found ${total} matches${intent?.city ? ` in ${String(intent.city)}${intent?.state ? `, ${String(intent.state)}` : ""}` : ""}.`,
        results: prospects,
        applied_criteria: applied,
        unsupported_parts: unsupportedPartsRaw,
        unsupported_message: typeof unsupported === "string" && unsupported.trim() ? unsupported : null,
        no_results_summary: noResults,
        agentic_iterations: iterations,
        job_id: start.job_id,
        saved_at: new Date().toISOString(),
      } satisfies AskResultsCache));
    } catch (err) {
      setProgressState(null);
      setProgressEvents([]);
      setError(clientFacingAppError(err instanceof Error ? err.message : "Failed to run request", "We couldn't run that Ask Neyma request right now. Please try again."));
    } finally {
      setLoading(false);
    }
  }

  async function handleRun(e: React.FormEvent) {
    e.preventDefault();
    await executeRun(false);
  }

  async function ensureDiagnosticId(row: AskProspect): Promise<number> {
    if (row.diagnostic_id) {
      return row.diagnostic_id;
    }

    const key = `${row.place_id || ""}-${row.business_name || ""}`;
    setEnsuringKey(key);
    setError(null);
    setMessageHref(null);
    setMessageCta(null);
    setBriefProgress(null);
    try {
      const ensure = await ensureAskProspectBrief({
        place_id: row.place_id,
        business_name: row.business_name || "",
        city: row.city || "",
        state: row.state || "",
        website: row.website,
      });
      if (ensure.status === "ready" && ensure.diagnostic_id) {
        markAskBriefReady(row, ensure.diagnostic_id);
        return ensure.diagnostic_id;
      }
      if (!ensure.job_id) throw new Error("Failed to start brief build");

      setMessage("Building full brief...");
      setBriefProgress({
        phase: "preparing_brief",
        businessName: row.business_name || "",
        city: row.city || "",
        state: row.state || "",
        polls: 0,
      });
      for (let i = 0; i < 240; i++) {
        const st = await getJobStatus(ensure.job_id);
        if (st.status === "completed" && st.diagnostic_id) {
          setBriefProgress(null);
          markAskBriefReady(row, st.diagnostic_id);
          return st.diagnostic_id;
        }
        if (st.status === "failed") throw new Error(clientFacingBriefError(st.error));
        setBriefProgress(buildBriefProgressState(st.progress, i + 1, row));
        await new Promise((r) => setTimeout(r, 2000));
      }
      throw new Error(clientFacingBriefError("Brief build timed out"));
    } catch (err) {
      const e = err instanceof Error ? err : new Error("Failed to build brief");
      setError(clientFacingBriefError(e.message));
      throw e;
    } finally {
      setBriefProgress(null);
      setEnsuringKey(null);
    }
  }

  async function onViewBrief(row: AskProspect) {
    try {
      const id = await ensureDiagnosticId(row);
      router.push(`/diagnostic/${id}?from=ask`);
    } catch (err) {
      setError(clientFacingBriefError(err instanceof Error ? err.message : "Failed to open brief"));
    }
  }

  async function onAddToList(row: AskProspect) {
    try {
      const diagId = await ensureDiagnosticId(row);
      setListTargetDiagnosticId(diagId);
      setListTargetBusinessName(row.business_name || "");
      setListModalOpen(true);
    } catch (err) {
      setError(clientFacingBriefError(err instanceof Error ? err.message : "Failed to prepare add to list"));
    }
  }

  async function onConfirmAddToList(payload: { listId?: number; newListName?: string }) {
    if (!listTargetDiagnosticId) throw new Error("No diagnostic selected");
    setListBusy(true);
    try {
      let listId = payload.listId;
      let createdNew = false;
      if (!listId) {
        if (!payload.newListName) throw new Error("List name is required");
        const created = await createProspectList(payload.newListName);
        listId = created.id;
        createdNew = true;
      }
      await addProspectsToList(listId, [listTargetDiagnosticId]);
      const updated = await getProspectLists().catch(() => ({ items: [] }));
      setLists(updated.items);
      setMessage(`Added ${listTargetBusinessName || "prospect"} to ${createdNew ? "new list" : "list"}.`);
      setMessageHref(`/lists/${listId}`);
      setMessageCta("Open list");
      setListModalOpen(false);
      setListTargetDiagnosticId(null);
      setListTargetBusinessName("");
    } finally {
      setListBusy(false);
    }
  }

  return (
    <div className="mx-auto max-w-6xl">
      <div className="mx-auto max-w-4xl text-center">
        <p className="section-kicker">Ask Neyma</p>
        <h1 className="display-title mt-3 text-4xl sm:text-6xl">
          Ask for the exact prospect you want.
        </h1>
        <p className="mx-auto mt-4 max-w-2xl text-sm text-[var(--text-secondary)] sm:text-base">
          Describe the target in plain English. Neyma uses AI reasoning and ML ranking to turn that request into a shortlist with reasons, not just names.
        </p>
        {usageMessage ? (
          <p className="mx-auto mt-3 max-w-2xl text-xs leading-6 text-[var(--text-secondary)]">{usageMessage}</p>
        ) : null}
      </div>

      <Card className="relative mx-auto mt-6 max-w-4xl overflow-hidden border border-[var(--border-default)] bg-[var(--bg-card)] shadow-[0_20px_50px_rgba(10,10,10,0.04)]">
        <BorderTrail
          className="bg-[#a1a1a1] opacity-60"
          size={88}
          style={{
            boxShadow:
              "0 0 18px 8px rgb(161 161 161 / 16%), 0 0 32px 14px rgb(161 161 161 / 10%)",
          }}
        />
        <CardBody className="p-4 sm:p-5">
          <div className="mb-4 flex flex-wrap gap-2">
            {promptIdeas.map((idea) => (
              <button
                key={idea}
                type="button"
                onClick={() => applyPrompt(idea)}
                className="rounded-full border border-[var(--border-default)] bg-[var(--bg-card)] px-3 py-1.5 text-xs text-[var(--text-secondary)] transition hover:bg-[var(--muted)] hover:text-[var(--text-primary)]"
              >
                {idea}
              </button>
            ))}
          </div>

          <form onSubmit={handleRun} className="space-y-3">
            <div className="rounded-[20px] border border-[var(--border-default)] bg-[var(--bg-card)] p-3 shadow-[0_10px_30px_rgba(10,10,10,0.04)]">
              <Textarea
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                rows={4}
                placeholder=""
                className="resize-none border-0 bg-transparent px-1 py-1 text-base leading-relaxed focus:border-transparent"
              />
              <div className="mt-3 flex flex-col gap-3 border-t border-[var(--border-default)] pt-3 sm:flex-row sm:items-center sm:justify-between">
                <div className="flex flex-wrap items-center gap-3 text-xs text-[var(--text-muted)]">
                  <span className="rounded-full bg-[var(--secondary)] px-2.5 py-1 text-[var(--secondary-foreground)]">Plain-English query</span>
                  <span className="rounded-full bg-[var(--muted)] px-2.5 py-1 text-[var(--text-secondary)]">Shortlist with reasons</span>
                  <Link href="/territory/new" className="app-link font-medium">
                    Start from territory scan
                  </Link>
                </div>
                <Button
                  type="submit"
                  disabled={loading || askLimitReached}
                  variant="primary"
                  className="h-11 w-full rounded-full px-5 sm:w-auto"
                >
                  {loading ? (
                    <span className="inline-flex items-center gap-2">
                      <span className="inline-flex h-4 w-4 animate-spin rounded-full border-2 border-white/35 border-t-white" />
                      Thinking
                    </span>
                  ) : askLimitReached ? "Monthly limit reached" : "Ask Neyma"}
                </Button>
              </div>
            </div>

            {message && (
              loading && progressState ? (
                <AgentProgressPanel progress={progressState} events={progressEvents} />
              ) : (
                <div className="rounded-2xl border border-[var(--border-default)] bg-[var(--muted)] px-4 py-3 text-left text-sm text-[var(--text-secondary)]">
                  <div className="flex flex-wrap items-center gap-2">
                    <span>{message}</span>
                    {messageHref && messageCta ? <Link href={messageHref} className="app-link font-medium">{messageCta}</Link> : null}
                  </div>
                </div>
              )
            )}
            {error && (
              <div className="rounded-[20px] border border-[var(--border-default)] bg-[var(--muted)] p-4 text-left">
                <div className="rounded-[16px] border border-[var(--border-default)] bg-[var(--bg-card)] px-4 py-3">
                  <p className="text-sm font-semibold text-[var(--text-primary)]">{normalizeAskError(error).title}</p>
                  <p className="mt-1 text-sm text-[var(--text-secondary)]">{normalizeAskError(error).description}</p>
                  {normalizeAskError(error).hints.length > 0 && (
                    <div className="mt-3 flex flex-wrap gap-2">
                      {normalizeAskError(error).hints.map((hint) => (
                        <span key={hint} className="rounded-full border border-[var(--border-default)] bg-[var(--secondary)] px-3 py-1.5 text-xs text-[var(--text-secondary)]">
                          {hint}
                        </span>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            )}

            {askLimitReached ? (
              <div className="rounded-2xl border border-[var(--border-default)] bg-white px-4 py-3 text-left text-sm text-[var(--text-secondary)]">
                <div className="flex flex-wrap items-center gap-2">
                  <span>Ask Neyma usage is paused until the next reset or a plan upgrade.</span>
                  <Link href="/settings" className="app-link font-medium">View plan and usage</Link>
                </div>
              </div>
            ) : null}
          </form>
        </CardBody>
      </Card>

      {briefProgress && (
        <BriefBuildProgress progress={briefProgress} className="mx-auto mt-4 max-w-4xl" />
      )}

      {requiresConfirmation && pendingIntent && (
        <Card className="mx-auto mt-4 max-w-4xl border-amber-200 bg-amber-50/50">
          <CardBody>
            <p className="text-sm font-semibold text-[var(--text-primary)]">Low-confidence intent parse</p>
            <p className="mt-1 text-sm text-[var(--text-secondary)]">
              We interpreted this as {String(pendingIntent.vertical || "general_local_business")} in {String(pendingIntent.city || "—")}, {String(pendingIntent.state || "—")}.
            </p>
            <p className="mt-1 text-xs text-[var(--text-muted)]">
              Criteria: {Array.isArray(pendingIntent.criteria) && pendingIntent.criteria.length > 0
                ? (pendingIntent.criteria as Array<Record<string, unknown>>).map((c) => `${String(c.type)}${c.service ? `:${String(c.service)}` : ""}`).join(", ")
                : "none"}.
            </p>
            <div className="mt-3 flex items-center gap-2">
              <Button onClick={() => { setRequiresConfirmation(false); setPendingIntent(null); }} className="border-[var(--border-default)] bg-[var(--bg-card)] text-[var(--text-secondary)]">Cancel</Button>
              <Button onClick={() => void executeRun(true)} variant="primary">Run with this intent</Button>
            </div>
          </CardBody>
        </Card>
      )}

      {headline && (
        <div className="mx-auto mt-6 flex max-w-5xl flex-col items-start gap-3 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <p className="text-sm font-medium text-[var(--text-secondary)]">{headline}</p>
            {results.length > 0 && (
              <p className="mt-1 text-xs text-[var(--text-muted)]">
                <span className="font-semibold text-[var(--text-secondary)]">{readyBriefCount} briefs ready</span> ·{" "}
                <span className="font-semibold text-[var(--text-secondary)]">{pendingBriefCount} not built yet</span>
              </p>
            )}
          </div>
          <button onClick={clearResults} className="text-sm text-[var(--text-secondary)] hover:underline">Clear results</button>
        </div>
      )}
      {(appliedCriteria.length > 0 || unsupportedMessage || unsupportedParts.length > 0) && (
        <div className="mx-auto mt-2 max-w-5xl space-y-2">
          {appliedCriteria.length > 0 && (
            <div className="flex flex-wrap gap-2">
              {appliedCriteria.map((item) => (
                <span key={item} className="rounded-full border border-[var(--border-default)] bg-[var(--bg-card)] px-3 py-1 text-xs text-[var(--text-secondary)]">
                  {item}
                </span>
              ))}
            </div>
          )}
          {unsupportedMessage && (
            <p className="text-xs text-[var(--text-muted)]">{unsupportedMessage}</p>
          )}
          {unsupportedParts.length > 0 && (
            <p className="text-xs text-[var(--text-muted)]">Unsupported parts: {unsupportedParts.join(", ")}.</p>
          )}
        </div>
      )}
      {!loading && results.length === 0 && noResultsSummary?.total_scanned && noResultsSummary.total_scanned > 0 && (
        <Card className="mx-auto mt-4 max-w-5xl border-[var(--border-default)]">
          <CardBody>
            <p className="text-sm text-[var(--text-secondary)]">
              <span className="font-semibold text-[var(--text-primary)]">No matches.</span> We scanned{" "}
              <span className="font-semibold text-[var(--text-primary)]">{noResultsSummary.total_scanned} prospects</span>; most exclusions came from{" "}
              <span className="font-semibold text-[var(--text-primary)]">{noResultsSummary.criterion_that_eliminated_most || "filters"}</span>.
            </p>
            {noResultsSummary.filtered_out_by_criterion && Object.keys(noResultsSummary.filtered_out_by_criterion).length > 0 && (
              <p className="mt-1 text-xs text-[var(--text-muted)]">
                Filter breakdown: {Object.entries(noResultsSummary.filtered_out_by_criterion).map(([k, v]) => `${k}: ${v}`).join(", ")}.
              </p>
            )}
            {noResultsSummary.no_results_suggestion && (
              <p className="mt-1 text-xs text-[var(--text-muted)]">{noResultsSummary.no_results_suggestion}</p>
            )}
          </CardBody>
        </Card>
      )}
      {agenticIterations.length > 0 && (
        <details className="mx-auto mt-4 max-w-5xl rounded-[20px] border border-[var(--border-default)] bg-[var(--bg-card)] px-4 py-3">
          <summary className="cursor-pointer list-none">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <p className="text-sm font-medium text-[var(--text-primary)]">How this result was built</p>
                <p className="mt-1 text-xs text-[var(--text-muted)]">Territory scanned, criteria applied, shortlist returned.</p>
              </div>
              <span className="text-xs font-medium text-[var(--text-secondary)]">View details</span>
            </div>
          </summary>
          <div className="mt-3 space-y-2 rounded-[16px] border border-[var(--border-default)] bg-[var(--muted)] px-3 py-3 text-xs text-[var(--text-muted)]">
            {agenticIterations.map((it, idx) => {
              return (
                <div key={idx} className="rounded-2xl border border-[var(--border-default)] bg-[var(--bg-card)] px-3 py-2.5">
                  <p className="font-medium text-[var(--text-primary)]">Pass {Number(it.iter || idx + 1)}</p>
                  <p className="mt-1">
                    {Number(it.postfilter_count || 0)} matches returned after scanning the local market.
                  </p>
                  <p className="mt-1">
                    Search radius: {Number(it.radius || 0)} miles.
                  </p>
                </div>
              );
            })}
          </div>
        </details>
      )}

      {results.length > 0 ? (
        <div className="mx-auto mt-4 max-w-5xl space-y-3">
          {results.map((r, i) => {
            const key = `${r.place_id || ""}-${r.business_name || ""}`;
            const busy = ensuringKey === key;
            const briefReady = askBriefReady(r);
            const evidence = Array.isArray(r.match_evidence)
              ? r.match_evidence.filter((item) => item && item.matched !== false).slice(0, 4)
              : [];
            return (
              <Card key={`${r.diagnostic_id || i}-${r.business_name || ""}`} className={`overflow-hidden bg-[var(--bg-card)] shadow-[0_12px_30px_rgba(10,10,10,0.04)] ${briefReady ? "border border-emerald-200" : "border border-[var(--border-default)]"}`}>
                <CardBody className="p-4 sm:p-5">
                  <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
                    <div className="min-w-0 flex-1">
                      <div className="flex flex-wrap items-center gap-2">
                        <h3 className="text-lg font-semibold text-[var(--text-primary)]">{r.business_name || "—"}</h3>
                        <Badge tone={busy ? "default" : briefReady ? "success" : "muted"}>
                          {busy ? "Building brief" : briefReady ? "Brief ready" : "Brief not built"}
                        </Badge>
                        <span
                          className={`inline-flex rounded-full border px-2 py-0.5 text-xs font-medium ${evidenceLevelClassName(r.match_evidence_level)}`}
                        >
                          {formatEvidenceLevel(r.match_evidence_level)}
                        </span>
                        {(r.rating != null || r.user_ratings_total != null) && (
                          <span className="rounded-full border border-[var(--border-default)] bg-[var(--secondary)] px-2.5 py-1 text-xs text-[var(--text-secondary)]">
                            {r.rating ?? "—"} stars · {r.user_ratings_total ?? "—"} reviews
                          </span>
                        )}
                      </div>
                      <p className="mt-1 text-sm text-[var(--text-muted)]">
                        {r.city || "—"}{r.state ? `, ${r.state}` : ""}
                      </p>
                      <div className="mt-3 rounded-2xl border border-[var(--border-default)] bg-[var(--muted)] px-4 py-3 text-sm leading-relaxed text-[var(--text-secondary)]">
                        {r.ai_explanation || r.primary_leverage || r.opportunity_profile || "No explanation returned."}
                      </div>
                      {evidence.length > 0 && (
                        <div className="mt-3 flex flex-wrap gap-2">
                          {evidence.map((item, idx) => (
                            <span
                              key={`${r.place_id || r.business_name || i}-evidence-${item.criterion_key || idx}`}
                              className="inline-flex items-center gap-1 rounded-full border border-[var(--border-default)] bg-[var(--bg-card)] px-3 py-1 text-xs text-[var(--text-secondary)]"
                            >
                              <span className="font-medium capitalize text-[var(--text-primary)]">
                                {humanizeCriterionLabel(item)}
                              </span>
                              <span className="text-[var(--text-muted)]">· {formatEvidenceSource(item.source)}</span>
                            </span>
                          ))}
                        </div>
                      )}
                      {r.match_evidence_level === "inferred" && (
                        <p className="mt-2 text-xs text-[var(--text-muted)]">
                          This match is still inference-heavy. Open the brief for stronger verification.
                        </p>
                      )}
                    </div>
                    <div className="flex w-full shrink-0 flex-col gap-2 sm:w-auto sm:flex-col sm:items-end">
                      <Button
                        onClick={() => void onViewBrief(r)}
                        variant="primary"
                        disabled={busy}
                        className="h-10 w-full rounded-full px-4 sm:w-auto"
                      >
                        {busy ? "Building..." : briefReady ? "Open brief" : "Build brief"}
                      </Button>
                      <Button
                        onClick={() => void onAddToList(r)}
                        disabled={busy}
                        className="h-10 w-full rounded-full px-4 sm:w-auto"
                      >
                        Add to list
                      </Button>
                    </div>
                  </div>
                </CardBody>
              </Card>
            );
          })}
        </div>
      ) : (
        !loading && !error && <div className="mx-auto mt-6 max-w-4xl"><EmptyState title="No results yet" description="Run an Ask query above to find prospects that match your criteria." /></div>
      )}

      {listModalOpen && (
        <ListPickerModal
          open={listModalOpen}
          title="Add prospect to list"
          lists={lists}
          busy={listBusy}
          onClose={() => {
            if (listBusy) return;
            setListModalOpen(false);
          }}
          onConfirm={onConfirmAddToList}
        />
      )}
    </div>
  );
}

function buildBriefProgressState(
  progress: Record<string, unknown> | null | undefined,
  polls: number,
  row: Pick<AskProspect, "business_name" | "city" | "state">,
): BriefBuildProgressState {
  const payload = (progress || {}) as Record<string, unknown>;
  const inner = (payload.progress || {}) as Record<string, unknown>;

  return {
    phase: String(payload.phase || "building_brief"),
    businessName: row.business_name || "",
    city: row.city || "",
    state: row.state || "",
    polls,
    pagesChecked: Number(inner.pages_crawled || inner.pages_checked || 0) || undefined,
    signalsFound: Number(inner.signals_found || inner.signals || inner.findings || 0) || undefined,
  };
}

function AgentProgressPanel({
  progress,
  events,
}: {
  progress: AskProgressState;
  events: AskProgressEvent[];
}) {
  const phaseLabel = formatPhaseLabel(progress.phase);
  const phaseKey = progress.phase.toLowerCase();
  const activePhaseEvents = getPhaseSubevents(progress.phase);
  const steps = [
    {
      label: "Discovering candidates",
      description: progress.candidates > 0 ? "Pulling the first matching practices into the working set." : "Searching the territory for candidate practices.",
      done: !phaseKey.includes("candidate_fetch") && progress.candidates > 0,
      active: phaseKey.includes("candidate_fetch") || progress.candidates === 0,
      meta: `${progress.candidates} found`,
    },
    {
      label: "Scoring opportunity signals",
      description: progress.scored > 0 ? "Checking service coverage, market position, and conversion weakness." : "Comparing each candidate against the request.",
      done: progress.scored > 0 && !phaseKey.includes("candidate_fetch"),
      active: phaseKey.includes("scor") || (progress.candidates > 0 && progress.scored === 0),
      meta: `${progress.scored} scored`,
    },
    {
      label: "Building shortlist",
      description: progress.listed > 0 ? "Keeping the strongest matches and dropping weak fits." : "Assembling the final ranked shortlist.",
      done: progress.listed > 0 && !phaseKey.includes("candidate_fetch") && !phaseKey.includes("scor"),
      active: phaseKey.includes("list") || phaseKey.includes("iter") || (progress.scored > 0 && progress.listed === 0),
      meta: `${progress.listed} listed`,
    },
  ];
  const totalSignals = progress.candidates + progress.scored + progress.listed;
  const [subeventIndex, setSubeventIndex] = useState(0);
  const [dotCount, setDotCount] = useState(1);

  useEffect(() => {
    const subeventTimer = window.setInterval(() => {
      setSubeventIndex((current) => (current + 1) % activePhaseEvents.length);
    }, 1600);
    const dotTimer = window.setInterval(() => {
      setDotCount((current) => (current % 3) + 1);
    }, 420);

    return () => {
      window.clearInterval(subeventTimer);
      window.clearInterval(dotTimer);
    };
  }, [activePhaseEvents.length]);

  return (
    <div className="rounded-[24px] border border-[var(--border-default)] bg-[var(--bg-card)] p-4 text-left shadow-[0_24px_60px_rgba(10,10,10,0.04)] sm:p-5">
      <div className="rounded-[20px] border border-[var(--border-default)] bg-[var(--bg-card)] p-4 sm:p-5">
        <div className="flex flex-col gap-5 sm:flex-row sm:items-start">
          <div className="flex items-center gap-4 sm:w-[240px] sm:flex-col sm:items-start">
            <div className="relative flex h-18 w-18 items-center justify-center rounded-full border border-[var(--border-default)] bg-[var(--muted)] shadow-[inset_0_1px_0_rgba(255,255,255,0.8),0_16px_32px_rgba(10,10,10,0.04)]">
              <span className="absolute inline-flex h-14 w-14 animate-ping rounded-full border border-[var(--border-default)]" />
              <span className="absolute inline-flex h-10 w-10 animate-spin rounded-full border-2 border-[var(--border-default)] border-t-[var(--primary)]" />
              <span className="relative inline-flex h-3.5 w-3.5 rounded-full bg-[var(--primary)] shadow-[0_0_0_6px_rgba(10,10,10,0.04)]" />
            </div>
            <div className="min-w-0">
              <div className="flex flex-wrap items-center gap-2">
                <p className="text-sm font-semibold text-[var(--text-primary)]">Neyma is working</p>
                {progress.iteration ? (
                  <span className="rounded-full border border-[var(--border-default)] bg-[var(--secondary)] px-2 py-1 text-[11px] font-medium text-[var(--text-secondary)]">
                    Iteration {progress.iteration}{progress.maxIterations ? `/${progress.maxIterations}` : ""}
                  </span>
                ) : null}
              </div>
              <p className="mt-1 text-sm text-[var(--text-secondary)]">{phaseLabel}</p>
              <p className="mt-2 text-xs leading-5 text-[var(--text-muted)]">
                {activePhaseEvents[subeventIndex]}
                <span className="inline-block w-4 text-left text-[var(--text-secondary)]">{'.'.repeat(dotCount)}</span>
              </p>
            </div>
          </div>

          <div className="min-w-0 flex-1">
            <div className="grid gap-2 sm:grid-cols-3">
              <ProgressMetric label="Found" value={progress.candidates} tint="green" />
              <ProgressMetric label="Scored" value={progress.scored} tint="blue" />
              <ProgressMetric label="Listed" value={progress.listed} tint="gold" />
            </div>

            <div className="mt-3 rounded-[20px] border border-[var(--border-default)] bg-[var(--muted)] p-3.5">
              <div className="flex items-center justify-between gap-3">
                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-[var(--text-muted)]">Action trace</p>
                <p className="text-xs text-[var(--text-muted)]">{totalSignals} workflow updates</p>
              </div>
              {events.length > 0 && (
                <div className="mt-3 space-y-2 rounded-[16px] border border-[var(--border-default)] bg-[var(--bg-card)] px-3 py-3">
                  {events.map((event, index) => (
                    <div key={event.id} className="flex items-start gap-3">
                      <span className={`mt-0.5 inline-flex h-2.5 w-2.5 shrink-0 rounded-full ${index === 0 ? "animate-pulse bg-[var(--primary)]" : "bg-black/15"}`} />
                      <div className="min-w-0">
                        <p className="text-xs font-medium text-[var(--text-primary)]">{event.label}</p>
                        <p className="text-xs leading-5 text-[var(--text-muted)]">{event.detail}</p>
                      </div>
                    </div>
                  ))}
                </div>
              )}
              <div className="mt-3 space-y-2">
                {steps.map((step) => (
                  <div key={step.label} className="flex items-start gap-3 rounded-2xl border border-[var(--border-default)] bg-[var(--bg-card)] px-3 py-3">
                    <span className="mt-0.5 inline-flex h-5 w-5 shrink-0 items-center justify-center rounded-full border border-[var(--border-default)] bg-[var(--muted)]">
                      {step.done ? (
                        <span className="inline-flex h-2.5 w-2.5 rounded-full bg-[var(--primary)]" />
                      ) : step.active ? (
                        <span className="inline-flex h-2.5 w-2.5 animate-pulse rounded-full bg-[var(--primary)]" />
                      ) : (
                        <span className="inline-flex h-2.5 w-2.5 rounded-full bg-black/15" />
                      )}
                    </span>
                    <div className="min-w-0 flex-1">
                      <div className="flex flex-wrap items-center justify-between gap-2">
                        <p className="text-sm font-medium text-[var(--text-primary)]">{step.label}</p>
                        <span className="text-xs text-[var(--text-muted)]">{step.meta}</span>
                      </div>
                      <p className="mt-1 text-xs leading-5 text-[var(--text-muted)]">{step.description}</p>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function buildProgressEventDetail(progress: AskProgressState) {
  const label = formatPhaseLabel(progress.phase);
  if (label === "Discovering candidates") {
    return progress.candidates > 0
      ? `${progress.candidates} practices pulled into the working set.`
      : "Starting a fresh territory pass.";
  }
  if (label === "Scoring opportunity signals") {
    return progress.scored > 0
      ? `${progress.scored} candidates checked against service depth and market weakness.`
      : "Comparing candidates against the prompt.";
  }
  if (label === "Building shortlist") {
    return progress.listed > 0
      ? `${progress.listed} strongest matches kept for the shortlist.`
      : "Sorting the best matches into rank order.";
  }
  if (label === "Refining the next pass") {
    return `Iteration ${progress.iteration || 1}${progress.maxIterations ? ` of ${progress.maxIterations}` : ""} is adjusting the search window.`;
  }

  return "Advancing the request through the workflow.";
}

function getPhaseSubevents(phase: string) {
  const label = formatPhaseLabel(phase);
  if (label === "Discovering candidates") {
    return [
      "Checking the local market for matching practices",
      "Resolving businesses into the candidate set",
      "Expanding the first pass across nearby listings",
    ];
  }
  if (label === "Scoring opportunity signals") {
    return [
      "Reviewing service coverage and site depth",
      "Comparing market position against nearby competitors",
      "Checking for conversion weakness and leverage",
    ];
  }
  if (label === "Building shortlist") {
    return [
      "Dropping weak fits from the ranked set",
      "Keeping the highest-leverage prospects",
      "Preparing the final shortlist for review",
    ];
  }
  if (label === "Refining the next pass") {
    return [
      "Adjusting the search window for a better pass",
      "Rebalancing the candidate pool",
      "Running another shortlist refinement",
    ];
  }

  return [
    "Preparing the next step",
    "Moving the request through the workflow",
    "Keeping the run active while results settle",
  ];
}

function ProgressMetric({
  label,
  value,
  tint,
}: {
  label: string;
  value: number;
  tint: "green" | "blue" | "gold";
}) {
  const palette = {
    green: "bg-[var(--secondary)] text-[var(--secondary-foreground)]",
    blue: "bg-[var(--secondary)] text-[var(--secondary-foreground)]",
    gold: "bg-[var(--secondary)] text-[var(--secondary-foreground)]",
  }[tint];

  return (
    <div className="rounded-[18px] border border-[var(--border-default)] bg-[var(--bg-card)] px-3 py-3">
      <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-[var(--text-muted)]">{label}</p>
      <div className="mt-2 flex items-center justify-between gap-3">
        <p className="text-2xl font-semibold text-[var(--text-primary)]">{value}</p>
        <span className={`inline-flex h-8 min-w-8 items-center justify-center rounded-full px-2 text-xs font-medium ${palette}`}>
          {label}
        </span>
      </div>
    </div>
  );
}
