"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import {
  addProspectsToList,
  createProspectList,
  ensureAskProspectBrief,
  getAskResults,
  getJobStatus,
  getProspectLists,
  runAskAgenticQuery,
  runAskQuery,
} from "@/lib/api";
import type { ProspectList } from "@/lib/types";
import Button from "@/app/components/ui/Button";
import { Card, CardBody } from "@/app/components/ui/Card";
import Textarea from "@/app/components/ui/Textarea";
import { Table, THead, TH, TR, TD } from "@/app/components/ui/Table";
import EmptyState from "@/app/components/ui/EmptyState";
import ListPickerModal from "@/app/components/ListPickerModal";

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

export default function AskPage() {
  const router = useRouter();
  const [query, setQuery] = useState("Find 10 dentists in San Jose that have missing implants page");
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
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
  const [verifiedMode, setVerifiedMode] = useState(true);
  const [agenticMode, setAgenticMode] = useState(false);
  const [lists, setLists] = useState<ProspectList[]>([]);
  const [listModalOpen, setListModalOpen] = useState(false);
  const [listBusy, setListBusy] = useState(false);
  const [listTargetDiagnosticId, setListTargetDiagnosticId] = useState<number | null>(null);
  const [listTargetBusinessName, setListTargetBusinessName] = useState<string>("");

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
      if (parsed.query) setQuery(parsed.query);
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
    setError(null);
    sessionStorage.removeItem(ASK_RESULTS_STORAGE_KEY);
  }

  async function executeRun(confirmedLowConfidence = false) {
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
    sessionStorage.removeItem(ASK_RESULTS_STORAGE_KEY);
    setLoading(true);
    try {
      const start = agenticMode
        ? await runAskAgenticQuery(query, verifiedMode ? "verified" : "fast", confirmedLowConfidence)
        : await runAskQuery(query, verifiedMode ? "verified" : "fast", confirmedLowConfidence);
      if (start.requires_confirmation || start.status === "requires_confirmation") {
        setPendingIntent((start.intent || {}) as Record<string, unknown>);
        setRequiresConfirmation(true);
        setMessage(start.message || "Please confirm intent before running.");
        setLoading(false);
        return;
      }
      if (!start.job_id) {
        throw new Error("Ask job did not start");
      }
      setMessage(start.message);

      let completed = false;
      const maxLoops = verifiedMode ? 720 : 240;
      const sleepMs = verifiedMode ? 2500 : 1500;
      for (let i = 0; i < maxLoops; i++) {
        const st = await getJobStatus(start.job_id);
        const progress = (st.progress || {}) as Record<string, unknown>;
        const phase = String(progress.phase || "running");
        const p = (progress.progress || {}) as Record<string, unknown>;
        const candidates = Number(p.candidates_found || 0);
        const scored = Number(p.scored || 0);
        const listed = Number(p.list_count || 0);
        const verifying = Number(p.verifying || 0);
        const verifiedProcessed = Number(p.processed || p.verified_processed || 0);
        if (phase === "agentic_iteration") {
          const iteration = Number(p.iteration || 0);
          const matched = Number(p.matched_count || listed || 0);
          const maxIterations = Number(p.max_iterations || 0);
          const base = `Iteration ${iteration}: ${candidates} scanned -> ${matched} matched${maxIterations ? ` (max ${maxIterations})` : ""}`;
          setMessage(st.status === "completed" ? base : `${base}. Adjusting plan for next iteration...`);
        } else if (verifiedMode && (phase === "verified_diagnostic" || verifying > 0)) {
          setMessage(`Verifying accuracy · ${verifiedProcessed}/${verifying || "?"} checked, ${listed} confirmed matches`);
        } else {
          setMessage(`${phase.replaceAll("_", " ")} · Found ${candidates}, scored ${scored}, listed ${listed}`);
        }

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
      if (agenticMode && iterations.length > 0) {
        const last = iterations[iterations.length - 1] || {};
        const telemetry = (last.telemetry || {}) as Record<string, unknown>;
        setMessage(`Iteration ${iterations.length}: ${Number(telemetry.candidate_count || 0)} scanned -> ${Number(telemetry.matched_count || 0)} matched`);
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
      setMessage(verifiedMode ? "Done. Verified matches only." : "Done.");
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
      setError(err instanceof Error ? err.message : "Failed to run request");
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
    try {
      const ensure = await ensureAskProspectBrief({
        place_id: row.place_id,
        business_name: row.business_name || "",
        city: row.city || "",
        state: row.state || "",
        website: row.website,
      });
      if (ensure.status === "ready" && ensure.diagnostic_id) {
        return ensure.diagnostic_id;
      }
      if (!ensure.job_id) throw new Error("Failed to start brief build");

      setMessage("Building your brief...");
      for (let i = 0; i < 240; i++) {
        const st = await getJobStatus(ensure.job_id);
        if (st.status === "completed" && st.diagnostic_id) {
          return st.diagnostic_id;
        }
        if (st.status === "failed") throw new Error(st.error || "Brief build failed");
        await new Promise((r) => setTimeout(r, 2000));
      }
      throw new Error("Brief build timed out");
    } catch (err) {
      const e = err instanceof Error ? err : new Error("Failed to build brief");
      setError(e.message);
      throw e;
    } finally {
      setEnsuringKey(null);
    }
  }

  async function onViewBrief(row: AskProspect) {
    try {
      const id = await ensureDiagnosticId(row);
      router.push(`/diagnostic/${id}?from=ask`);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to open brief");
    }
  }

  async function onAddToList(row: AskProspect) {
    try {
      const diagId = await ensureDiagnosticId(row);
      setListTargetDiagnosticId(diagId);
      setListTargetBusinessName(row.business_name || "");
      setListModalOpen(true);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to prepare add to list");
    }
  }

  async function onConfirmAddToList(payload: { listId?: number; newListName?: string }) {
    if (!listTargetDiagnosticId) throw new Error("No diagnostic selected");
    setListBusy(true);
    try {
      let listId = payload.listId;
      if (!listId) {
        if (!payload.newListName) throw new Error("List name is required");
        const created = await createProspectList(payload.newListName);
        listId = created.id;
      }
      await addProspectsToList(listId, [listTargetDiagnosticId]);
      const updated = await getProspectLists().catch(() => ({ items: [] }));
      setLists(updated.items);
      setMessage(`Added ${listTargetBusinessName || "prospect"} to list.`);
      setListModalOpen(false);
      setListTargetDiagnosticId(null);
      setListTargetBusinessName("");
    } finally {
      setListBusy(false);
    }
  }

  return (
    <div className="mx-auto max-w-6xl">
      <h1 className="display-title text-3xl font-black tracking-tight">Ask Neyma</h1>
      <p className="mt-1 text-sm text-[var(--text-muted)]">Describe exactly what you want and Neyma will return a shortlist. Use verified mode for service-page accuracy.</p>

      <Card className="mt-5">
        <CardBody>
          <form onSubmit={handleRun} className="space-y-3">
            <Textarea
              label="Request"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              rows={3}
              placeholder="Find up to 20 dentists in San Jose, CA with missing implants page"
            />
            <div className="flex items-center gap-3">
              <Button
                type="submit"
                disabled={loading}
                variant="primary"
              >
                {loading ? "Finding prospects..." : "Run query"}
              </Button>
              <Link href="/territory/new" className="text-sm app-link">
                Or run a structured territory scan
              </Link>
            </div>
            <label className="flex items-center gap-2 text-sm text-[var(--text-secondary)]">
              <input
                type="checkbox"
                checked={verifiedMode}
                onChange={(e) => setVerifiedMode(e.target.checked)}
                className="h-4 w-4 rounded border-[var(--border-default)]"
              />
              Extreme accuracy (verified mode, slower)
            </label>
            <label className="flex items-center gap-2 text-sm text-[var(--text-secondary)]">
              <input
                type="checkbox"
                checked={agenticMode}
                onChange={(e) => setAgenticMode(e.target.checked)}
                className="h-4 w-4 rounded border-[var(--border-default)]"
              />
              Agentic planner loop (iterative scan refinement)
            </label>
            {verifiedMode && (
              <p className="text-xs text-[var(--text-muted)]">
                For queries like “no implants page,” Neyma validates with deep diagnostics and may take 10-25 minutes.
              </p>
            )}
            {agenticMode && (
              <p className="text-xs text-[var(--text-muted)]">
                Agentic mode runs multiple deterministic scan iterations and adjusts radius/candidate cap/filter strategy between rounds.
              </p>
            )}
            {message && <p className="text-sm text-[var(--text-secondary)]">{message}</p>}
            {error && <p className="text-sm text-red-600">{error}</p>}
          </form>
        </CardBody>
      </Card>

      {requiresConfirmation && pendingIntent && (
        <Card className="mt-4 border-amber-200 bg-amber-50/50">
          <CardBody>
            <p className="text-sm font-semibold text-[var(--text-primary)]">Low-confidence interpretation</p>
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
              <Button onClick={() => void executeRun(true)} variant="primary">Run anyway</Button>
            </div>
          </CardBody>
        </Card>
      )}

      {headline && (
        <div className="mt-5 flex items-center justify-between gap-2">
          <p className="text-sm text-[var(--text-secondary)]">{headline}</p>
          <button onClick={clearResults} className="text-sm text-[var(--text-secondary)] hover:underline">Clear results</button>
        </div>
      )}
      {appliedCriteria.length > 0 && (
        <p className="mt-1 text-xs text-[var(--text-muted)]">Results filtered by: {appliedCriteria.join(", ")}.</p>
      )}
      {unsupportedMessage && (
        <p className="mt-1 text-xs text-[var(--text-muted)]">{unsupportedMessage}</p>
      )}
      {unsupportedParts.length > 0 && (
        <p className="mt-1 text-xs text-[var(--text-muted)]">Unsupported parts: {unsupportedParts.join(", ")}.</p>
      )}
      {!loading && results.length === 0 && noResultsSummary?.total_scanned && noResultsSummary.total_scanned > 0 && (
        <Card className="mt-3 border-[var(--border-default)]">
          <CardBody>
            <p className="text-sm text-[var(--text-secondary)]">
              No matches. We scanned {noResultsSummary.total_scanned} prospects; most exclusions from {noResultsSummary.criterion_that_eliminated_most || "filters"}.
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
        <details className="mt-3 rounded-[var(--radius-md)] border border-[var(--border-default)] bg-[var(--bg-card)] px-3 py-2">
          <summary className="cursor-pointer text-sm font-medium text-[var(--text-secondary)]">Search summary</summary>
          <div className="mt-2 space-y-1 text-xs text-[var(--text-muted)]">
            {agenticIterations.map((it, idx) => {
              const t = (it.telemetry || {}) as Record<string, unknown>;
              return (
                <p key={idx}>
                  Iteration {Number(it.iteration || idx + 1)}: {Number(t.candidate_count || 0)} scanned {"->"} {Number(t.matched_count || 0)} matched; radius {Number(it.radius_miles || 0)} mi; cap {Number(it.candidate_cap || 0)}.
                </p>
              );
            })}
          </div>
        </details>
      )}

      {results.length > 0 ? (
        <Card className="mt-3">
          <Table>
            <THead>
              <tr>
                <TH>Business</TH>
                <TH>City</TH>
                <TH>Rating</TH>
                <TH>Reviews</TH>
                <TH>Signal</TH>
                <TH className="text-right">Action</TH>
              </tr>
            </THead>
            <tbody>
              {results.map((r, i) => {
                const key = `${r.place_id || ""}-${r.business_name || ""}`;
                return (
                  <TR key={`${r.diagnostic_id || i}-${r.business_name || ""}`}>
                    <TD className="font-medium text-[var(--text-primary)]">{r.business_name || "—"}</TD>
                    <TD>{r.city || "—"}{r.state ? `, ${r.state}` : ""}</TD>
                    <TD>{r.rating ?? "—"}</TD>
                    <TD>{r.user_ratings_total ?? "—"}</TD>
                    <TD>{r.primary_leverage || r.opportunity_profile || "—"}</TD>
                    <TD className="text-right">
                      <button
                        onClick={() => void onViewBrief(r)}
                        className="app-link mr-3 font-medium disabled:opacity-60"
                        disabled={ensuringKey === key}
                      >
                        {ensuringKey === key ? "Building brief..." : "View brief"}
                      </button>
                      <button
                        onClick={() => void onAddToList(r)}
                        className="text-[var(--text-secondary)] hover:underline disabled:opacity-60"
                        disabled={ensuringKey === key}
                      >
                        Add to list
                      </button>
                    </TD>
                  </TR>
                );
              })}
            </tbody>
          </Table>
        </Card>
      ) : (
        !loading && !error && <div className="mt-4"><EmptyState title="No results yet" description="Run a query above to find prospects matching your criteria." /></div>
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
