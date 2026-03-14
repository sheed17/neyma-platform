"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import {
  addProspectsToList,
  createProspectList,
  ensureTerritoryProspectBrief,
  getJobStatus,
  getProspectLists,
  getTerritoryScanResults,
  markProspectOutcome,
} from "@/lib/api";
import type { ProspectList, ProspectRow, TerritoryScanResultsResponse } from "@/lib/types";
import Button from "@/app/components/ui/Button";
import { Card, CardBody, CardHeader } from "@/app/components/ui/Card";
import Badge from "@/app/components/ui/Badge";
import { Table, THead, TH, TR, TD } from "@/app/components/ui/Table";
import ListPickerModal from "@/app/components/ListPickerModal";
import BriefBuildProgress, { type BriefBuildProgressState } from "@/app/components/BriefBuildProgress";

function territoryBriefReady(row: Pick<ProspectRow, "diagnostic_id" | "full_brief_ready">): boolean {
  return Boolean(row.diagnostic_id && row.full_brief_ready);
}

export default function TerritoryResultsPage() {
  const params = useParams();
  const router = useRouter();
  const scanId = String(params.scanId || "");
  const [data, setData] = useState<TerritoryScanResultsResponse | null>(null);
  const [lists, setLists] = useState<ProspectList[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [sortBy, setSortBy] = useState<"rank" | "name" | "reviews">("rank");
  const [actionProspectId, setActionProspectId] = useState<number | null>(null);
  const [actionLabel, setActionLabel] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);
  const [successHref, setSuccessHref] = useState<string | null>(null);
  const [successCta, setSuccessCta] = useState<string | null>(null);
  const [listModalOpen, setListModalOpen] = useState(false);
  const [listBusy, setListBusy] = useState(false);
  const [listTargetDiagnosticId, setListTargetDiagnosticId] = useState<number | null>(null);
  const [listTargetBusinessName, setListTargetBusinessName] = useState("");
  const [briefProgress, setBriefProgress] = useState<BriefBuildProgressState | null>(null);
  const cacheKey = `territory_scan_${scanId}`;

  useEffect(() => {
    if (!scanId) return;
    try {
      const cached = sessionStorage.getItem(cacheKey);
      if (!cached) return;
      const parsed = JSON.parse(cached) as TerritoryScanResultsResponse;
      if (parsed?.scan_id === scanId) {
        setData(parsed);
        setLoading(false);
      }
    } catch {
      // ignore
    }
  }, [cacheKey, scanId]);

  useEffect(() => {
    let cancelled = false;
    async function poll() {
      let attempts = 0;
      while (!cancelled && attempts < 600) {
        try {
          const results = await getTerritoryScanResults(scanId);
          if (cancelled) return;
          setData(results);
          setLoading(false);
          setError(null);
          if (results.status === "completed" && Array.isArray(results.prospects)) {
            sessionStorage.setItem(cacheKey, JSON.stringify(results));
          }
          if (results.status === "completed" || results.status === "failed") return;
        } catch (err) {
          if (!cancelled) setError(err instanceof Error ? err.message : "Failed to load scan");
        }
        attempts += 1;
        await new Promise((r) => setTimeout(r, 2500));
      }
    }
    if (scanId) void poll();
    return () => {
      cancelled = true;
    };
  }, [cacheKey, scanId]);

  useEffect(() => {
    let cancelled = false;
    async function loadLists() {
      const ls = await getProspectLists().catch(() => ({ items: [] }));
      if (!cancelled) setLists(ls.items);
    }
    void loadLists();
    return () => {
      cancelled = true;
    };
  }, []);

  const progress = useMemo(() => {
    const summary = data?.summary || {};
    const phase = String(summary.phase || "");
    const queryDone = Number(summary.candidate_queries_done || 0);
    const queryTotal = Number(summary.candidate_queries_total || 0);
    const processed = Number(summary.processed || 0);
    const total = Number(summary.scored_candidates || summary.total_candidates || 0);
    if (phase === "candidate_fetch" && queryTotal > 0) return { label: `Finding candidates... ${queryDone}/${queryTotal} areas`, percent: Math.round((queryDone / queryTotal) * 100) };
    if (total > 0) return { label: `Scanning... ${processed}/${total}`, percent: Math.round((processed / total) * 100) };
    return { label: "Finding candidates...", percent: 0 };
  }, [data?.summary]);
  const prospects = useMemo(() => {
    const rows = [...(data?.prospects || [])];
    if (sortBy === "name") rows.sort((a, b) => a.business_name.localeCompare(b.business_name));
    else if (sortBy === "reviews") rows.sort((a, b) => Number(a.user_ratings_total || 0) - Number(b.user_ratings_total || 0));
    else rows.sort((a, b) => (a.rank || 9999) - (b.rank || 9999));
    return rows;
  }, [data?.prospects, sortBy]);

  const territoryProgress = useMemo(
    () =>
      buildTerritoryProgressState({
        city: data?.city || "",
        state: data?.state || "",
        status: data?.status || (loading ? "running" : "idle"),
        summary: (data?.summary || {}) as Record<string, unknown>,
        currentCount: prospects.length,
      }),
    [data?.city, data?.state, data?.status, data?.summary, loading, prospects.length],
  );

  const scanTimestamp = data?.completed_at || data?.created_at || null;
  const scanAgeDays = useMemo(() => {
    if (!scanTimestamp) return null;
    return Math.max(0, Math.floor((Date.now() - new Date(scanTimestamp).getTime()) / 86400000));
  }, [scanTimestamp]);
  const readyBriefCount = useMemo(
    () => prospects.filter((row) => territoryBriefReady(row)).length,
    [prospects],
  );
  const pendingBriefCount = Math.max(0, prospects.length - readyBriefCount);

  const outcomeLabel = (status?: string) => {
    if (!status) return "Not contacted";
    if (status === "contacted") return "Contacted";
    if (status === "closed_won") return "Won";
    if (status === "closed_lost") return "Lost";
    return "Not contacted";
  };

  function markProspectBriefReady(prospectId: number | null | undefined, diagnosticId: number) {
    if (!prospectId) return;
    setData((prev) => {
      if (!prev) return prev;
      const next = {
        ...prev,
        prospects: (prev.prospects || []).map((row) => (
          row.prospect_id === prospectId
            ? { ...row, diagnostic_id: diagnosticId, full_brief_ready: true }
            : row
        )),
      };
      sessionStorage.setItem(cacheKey, JSON.stringify(next));
      return next;
    });
  }

  async function ensureBrief(row: ProspectRow): Promise<number> {
    if (!row.prospect_id) throw new Error("Prospect id missing");
    const ensured = await ensureTerritoryProspectBrief(row.prospect_id);
    if (ensured.status === "ready" && ensured.diagnostic_id) {
      markProspectBriefReady(row.prospect_id, ensured.diagnostic_id);
      return ensured.diagnostic_id;
    }
    if (!ensured.job_id) throw new Error("No job id returned");
    setBriefProgress({
      phase: "preparing_brief",
      businessName: row.business_name || "",
      city: row.city || "",
      state: row.state || "",
      polls: 0,
    });
    for (let i = 0; i < 240; i++) {
      const st = await getJobStatus(ensured.job_id);
      if (st.status === "completed" && st.diagnostic_id) {
        setBriefProgress(null);
        markProspectBriefReady(row.prospect_id, st.diagnostic_id);
        return st.diagnostic_id;
      }
      if (st.status === "failed") throw new Error(st.error || "Failed to build brief");
      setBriefProgress(buildBriefProgressState(st.progress, i + 1, row));
      await new Promise((r) => setTimeout(r, 2000));
    }
    throw new Error("Brief build timed out");
  }

  async function handleViewBrief(row: ProspectRow) {
    try {
      setSuccessMessage(null);
      setSuccessHref(null);
      setSuccessCta(null);
      setActionProspectId(row.prospect_id || -1);
      setActionLabel("Building your brief...");
      const id = row.diagnostic_id && row.full_brief_ready ? row.diagnostic_id : await ensureBrief(row);
      router.push(`/diagnostic/${id}?from=territory&scanId=${encodeURIComponent(scanId)}`);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to open brief");
    } finally {
      setActionProspectId(null);
      setActionLabel(null);
      setBriefProgress(null);
    }
  }

  async function handleAddToList(row: ProspectRow) {
    try {
      setSuccessMessage(null);
      setSuccessHref(null);
      setSuccessCta(null);
      setActionProspectId(row.prospect_id || -1);
      setActionLabel("Preparing full brief...");
      const diagId = row.diagnostic_id && row.full_brief_ready ? row.diagnostic_id : await ensureBrief(row);
      setListTargetDiagnosticId(diagId);
      setListTargetBusinessName(row.business_name || "");
      setListModalOpen(true);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to add to list");
    } finally {
      setActionProspectId(null);
      setActionLabel(null);
      setBriefProgress(null);
    }
  }

  async function handleOutcome(row: ProspectRow, status: "contacted" | "closed_won" | "closed_lost") {
    if (!row.diagnostic_id) return;
    await markProspectOutcome({ diagnostic_id: row.diagnostic_id, status });
    setData(await getTerritoryScanResults(scanId));
  }

  async function handleConfirmAddToList(payload: { listId?: number; newListName?: string }) {
    if (!listTargetDiagnosticId) throw new Error("No prospect selected");
    setListBusy(true);
    try {
      let targetListId = payload.listId;
      let createdNew = false;
      if (!targetListId) {
        if (!payload.newListName) throw new Error("List name is required");
        const created = await createProspectList(payload.newListName);
        targetListId = created.id;
        createdNew = true;
      }
      await addProspectsToList(targetListId, [listTargetDiagnosticId]);
      const updated = await getProspectLists().catch(() => ({ items: [] }));
      setLists(updated.items);
      setSuccessMessage(`Added ${listTargetBusinessName || "prospect"} to ${createdNew ? "new list" : "list"}.`);
      setSuccessHref(`/lists/${targetListId}`);
      setSuccessCta("Open list");
      setListModalOpen(false);
      setListTargetDiagnosticId(null);
      setListTargetBusinessName("");
    } finally {
      setListBusy(false);
    }
  }

  function exportCsv() {
    const headers = ["Rank", "Business", "City", "State", "Rating", "Reviews", "Review Summary"];
    const lines = prospects.map((p) => [String(p.rank || ""), p.business_name, p.city || "", p.state || "", String(p.rating ?? ""), String(p.user_ratings_total ?? ""), p.review_position_summary || ""]);
    const csv = [headers, ...lines].map((r) => r.map((v) => `"${String(v).replace(/"/g, '""')}"`).join(",")).join("\n");
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `territory_${scanId}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }

  if (loading) {
    return (
      <div className="mx-auto max-w-7xl py-6">
        <TerritoryScanProgressPanel progress={territoryProgress} />
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-7xl">
      <div className="mb-4 flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="text-xs text-[var(--text-muted)]">Territory</p>
          <h1 className="text-2xl font-semibold tracking-tight">Market Scan: {data?.city || "Market"}{data?.state ? `, ${data.state}` : ""}</h1>
          <p className="mt-1 text-sm text-[var(--text-muted)]">{prospects.length} prospects · {data?.status === "running" ? progress.label : "Scan complete"}</p>
          {prospects.length > 0 && (
            <p className="mt-1 text-xs text-[var(--text-muted)]">{readyBriefCount} briefs ready · {pendingBriefCount} not built yet</p>
          )}
          {scanTimestamp && (
            <p className="mt-1 text-xs text-[var(--text-muted)]">
              {scanAgeDays && scanAgeDays > 0 ? `Last updated ${scanAgeDays} days ago` : "Just updated"} · {new Date(scanTimestamp).toLocaleDateString("en-US")}
            </p>
          )}
        </div>
        <div className="flex flex-wrap gap-2">
          <Link href="/territory/new"><Button>New scan</Button></Link>
          <Link href="/ask"><Button>Ask Neyma</Button></Link>
          <Link href="/lists"><Button>Lists</Button></Link>
          <Button variant="primary" onClick={exportCsv}>Export CSV</Button>
        </div>
      </div>

      {scanAgeDays != null && scanAgeDays > 30 && (
        <Card className="mb-4 border border-[var(--border-default)] bg-[var(--muted)]">
          <CardBody className="text-sm text-[var(--text-secondary)]">
            This list is {scanAgeDays} days old. Refresh to see latest review counts and competitors.{" "}
            <Link href={`/territory/new?city=${encodeURIComponent(data?.city || "")}&state=${encodeURIComponent(data?.state || "")}&vertical=${encodeURIComponent(data?.vertical || "")}`} className="app-link font-medium">
              Run new scan
            </Link>
          </CardBody>
        </Card>
      )}

      {error && <Card className="mb-4 border border-[var(--border-default)] bg-[var(--muted)]"><CardBody className="text-sm text-[var(--text-secondary)]">{error}</CardBody></Card>}
      {actionLabel && <Card className="mb-4 border border-[var(--border-default)] bg-[var(--muted)]"><CardBody className="text-sm text-[var(--text-secondary)]">{actionLabel}</CardBody></Card>}
      {data?.status === "running" && <TerritoryScanProgressPanel progress={territoryProgress} className="mb-4" />}
      {briefProgress && <BriefBuildProgress progress={briefProgress} className="mb-4" />}
      {successMessage && (
        <Card className="mb-4 border border-[var(--border-default)] bg-[var(--muted)]">
          <CardBody className="flex flex-wrap items-center gap-2 text-sm text-[var(--text-secondary)]">
            <span>{successMessage}</span>
            {successHref && successCta ? <Link href={successHref} className="app-link font-medium">{successCta}</Link> : null}
          </CardBody>
        </Card>
      )}

      <Card>
        <CardHeader
          title="Ranked Market Prospects"
          subtitle={`Showing top ${Number(data?.summary?.accepted || prospects.length)} of ${Number(data?.summary?.scored_candidates || 0)} scored prospects. ${readyBriefCount} ready, ${pendingBriefCount} not built yet.`}
          action={
            <select value={sortBy} onChange={(e) => setSortBy(e.target.value as "rank" | "name" | "reviews")} className="h-8 rounded-[var(--radius-sm)] border border-[var(--border-default)] px-2 text-xs">
              <option value="rank">Sort: Rank</option>
              <option value="name">Sort: Name</option>
              <option value="reviews">Sort: Reviews</option>
            </select>
          }
        />
        <Table>
          <THead>
            <tr>
              <TH>Rank</TH><TH>Business</TH><TH>City</TH><TH>Rating</TH><TH>Reviews</TH><TH>Contact</TH><TH>Website</TH><TH>Key Signal</TH><TH>Status</TH><TH className="text-right">Actions</TH>
            </tr>
          </THead>
          <tbody>
            {prospects.map((row) => {
              const busy = actionProspectId != null && actionProspectId === row.prospect_id;
              const briefReady = territoryBriefReady(row);
              return (
                <TR key={`${row.prospect_id || row.place_id}`}>
                  <TD className="font-medium text-[var(--text-primary)]">#{row.rank || "-"}</TD>
                  <TD className="font-medium text-[var(--text-primary)]">
                    <div className="flex flex-col gap-1">
                      <span>{row.business_name}</span>
                      <span>
                        <Badge tone={busy ? "default" : briefReady ? "success" : "muted"}>
                          {busy ? "Building brief" : briefReady ? "Brief ready" : "Brief not built"}
                        </Badge>
                      </span>
                    </div>
                  </TD>
                  <TD>{row.city}{row.state ? `, ${row.state}` : ""}</TD>
                  <TD>{row.rating ?? "-"}</TD>
                  <TD>{row.user_ratings_total ?? "-"}</TD>
                  <TD>{row.phone ? <a href={`tel:${row.phone}`} className="app-link">{row.phone}</a> : "—"}</TD>
                  <TD>{row.website ? <a href={row.website} target="_blank" rel="noreferrer" className="app-link break-all">{row.website}</a> : "—"}</TD>
                  <TD>{row.key_signal ? <Badge>{row.key_signal}</Badge> : "—"}</TD>
                  <TD><Badge tone={row.outcome_status?.status === "closed_won" ? "success" : row.outcome_status?.status === "closed_lost" ? "danger" : "muted"}>{outcomeLabel(row.outcome_status?.status)}</Badge></TD>
                  <TD className="text-right">
                    <button
                      onClick={() => void handleViewBrief(row)}
                      disabled={busy}
                      className="mr-2 inline-flex h-9 items-center justify-center rounded-full bg-[var(--primary)] px-4 text-sm font-medium text-[var(--primary-foreground)] transition hover:opacity-95 disabled:opacity-60"
                    >
                      {busy ? "Building..." : briefReady ? "Open brief" : "Build brief"}
                    </button>
                    <button onClick={() => void handleAddToList(row)} disabled={busy} className="text-[var(--text-secondary)] hover:underline disabled:opacity-60">Add to list</button>
                    <button onClick={() => void handleOutcome(row, "contacted")} disabled={!row.diagnostic_id} className="ml-2 text-[var(--text-secondary)] hover:underline disabled:opacity-50">Contacted</button>
                  </TD>
                </TR>
              );
            })}
          </tbody>
        </Table>
      </Card>

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
          onConfirm={handleConfirmAddToList}
        />
      )}
    </div>
  );
}

function buildBriefProgressState(
  progress: Record<string, unknown> | null | undefined,
  polls: number,
  row: Pick<ProspectRow, "business_name" | "city" | "state">,
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

type TerritoryScanProgressState = {
  city: string;
  state: string;
  phase: string;
  label: string;
  percent: number;
  queryDone: number;
  queryTotal: number;
  processed: number;
  total: number;
  accepted: number;
};

function buildTerritoryProgressState({
  city,
  state,
  status,
  summary,
  currentCount,
}: {
  city: string;
  state: string;
  status: string;
  summary: Record<string, unknown>;
  currentCount: number;
}): TerritoryScanProgressState {
  const phase = String(summary.phase || (status === "running" ? "candidate_fetch" : "completed"));
  const queryDone = Number(summary.candidate_queries_done || 0);
  const queryTotal = Number(summary.candidate_queries_total || 0);
  const processed = Number(summary.processed || 0);
  const total = Number(summary.scored_candidates || summary.total_candidates || 0);
  const accepted = Number(summary.accepted || currentCount || 0);
  const label =
    phase === "candidate_fetch" && queryTotal > 0
      ? `Discovering candidates across ${queryDone}/${queryTotal} areas`
      : total > 0
        ? `Ranking ${processed}/${total} candidates`
        : status === "completed"
          ? "Scan complete"
          : "Preparing scan";
  const percent =
    phase === "candidate_fetch" && queryTotal > 0
      ? Math.round((queryDone / queryTotal) * 100)
      : total > 0
        ? Math.round((processed / total) * 100)
        : status === "completed"
          ? 100
          : 8;

  return { city, state, phase, label, percent, queryDone, queryTotal, processed, total, accepted };
}

function TerritoryScanProgressPanel({
  progress,
  className = "",
}: {
  progress: TerritoryScanProgressState;
  className?: string;
}) {
  const [subeventIndex, setSubeventIndex] = useState(0);
  const [dotCount, setDotCount] = useState(1);
  const phase = progress.phase.toLowerCase();
  const subevents = phase.includes("candidate")
    ? [
        "Checking the local map for relevant practices",
        "Expanding the search across nearby listings",
        "Pulling the first candidate set into the scan",
      ]
    : [
        "Comparing reviews and local position",
        "Checking website basics and contact readiness",
        "Sorting the strongest prospects into the shortlist",
      ];
  const steps = [
    {
      label: "Discover candidates",
      meta: progress.queryTotal > 0 ? `${progress.queryDone}/${progress.queryTotal} areas` : "starting",
      description: "Search the market before any ranking starts.",
      done: !phase.includes("candidate") && (progress.total > 0 || progress.accepted > 0),
      active: phase.includes("candidate") || (progress.queryDone === 0 && progress.accepted === 0),
    },
    {
      label: "Rank the market",
      meta: progress.total > 0 ? `${progress.processed}/${progress.total} ranked` : "queued",
      description: "Review local position, review gaps, and site readiness.",
      done: progress.processed > 0 && !phase.includes("candidate") && progress.accepted > 0,
      active: !phase.includes("candidate") && (progress.processed < progress.total || progress.total === 0),
    },
    {
      label: "Build shortlist",
      meta: `${progress.accepted} listed`,
      description: "Keep the strongest practices and drop weak fits.",
      done: false,
      active: progress.accepted > 0 && progress.processed >= Math.max(progress.total, 1),
    },
  ];

  useEffect(() => {
    const subeventTimer = window.setInterval(() => {
      setSubeventIndex((current) => (current + 1) % subevents.length);
    }, 1700);
    const dotTimer = window.setInterval(() => {
      setDotCount((current) => (current % 3) + 1);
    }, 420);
    return () => {
      window.clearInterval(subeventTimer);
      window.clearInterval(dotTimer);
    };
  }, [subevents.length]);

  return (
    <div className={`rounded-[24px] border border-[var(--border-default)] bg-[var(--bg-card)] p-4 shadow-[0_24px_60px_rgba(10,10,10,0.04)] sm:p-5 ${className}`}>
      <div className="rounded-[20px] border border-[var(--border-default)] bg-[var(--bg-card)] p-4 sm:p-5">
        <div className="flex flex-col gap-5 sm:flex-row sm:items-start">
          <div className="flex items-center gap-4 sm:w-[260px] sm:flex-col sm:items-start">
            <div className="relative flex h-18 w-18 items-center justify-center rounded-full border border-[var(--border-default)] bg-[var(--muted)] shadow-[inset_0_1px_0_rgba(255,255,255,0.8),0_16px_32px_rgba(10,10,10,0.04)]">
              <span className="absolute inline-flex h-14 w-14 animate-ping rounded-full border border-[var(--border-default)]" />
              <span className="absolute inline-flex h-10 w-10 animate-spin rounded-full border-2 border-[var(--border-default)] border-t-[var(--primary)]" />
              <span className="relative inline-flex h-3.5 w-3.5 rounded-full bg-[var(--primary)] shadow-[0_0_0_6px_rgba(0,0,0,0.04)]" />
            </div>
            <div>
              <div className="flex flex-wrap items-center gap-2">
                <p className="text-sm font-semibold text-[var(--text-primary)]">Running territory scan</p>
                <span className="rounded-full border border-[var(--border-default)] bg-[var(--secondary)] px-2 py-1 text-[11px] font-medium text-[var(--text-secondary)]">
                  {progress.percent}%
                </span>
              </div>
              <p className="mt-1 text-sm text-[var(--text-secondary)]">
                {progress.city || "Selected market"}{progress.state ? `, ${progress.state}` : ""}
              </p>
              <p className="mt-2 text-xs leading-5 text-[var(--text-muted)]">
                {subevents[subeventIndex]}
                <span className="inline-block w-4 text-left text-[var(--text-secondary)]">{ ".".repeat(dotCount) }</span>
              </p>
              <p className="mt-2 text-xs font-medium text-[var(--text-secondary)]">{progress.label}</p>
            </div>
          </div>

          <div className="min-w-0 flex-1">
            <div className="grid gap-2 sm:grid-cols-3">
              <ProgressChip label="Areas" value={progress.queryTotal > 0 ? `${progress.queryDone}/${progress.queryTotal}` : "—"} />
              <ProgressChip label="Ranked" value={progress.total > 0 ? `${progress.processed}/${progress.total}` : "—"} />
              <ProgressChip label="Shortlist" value={String(progress.accepted)} />
            </div>

            <div className="mt-3 h-2.5 overflow-hidden rounded-full bg-[var(--muted)]">
              <div className="h-full rounded-full bg-[var(--primary)] transition-[width] duration-500" style={{ width: `${Math.max(10, progress.percent)}%` }} />
            </div>

            <div className="mt-3 space-y-2 rounded-[20px] border border-[var(--border-default)] bg-[var(--muted)] p-3.5">
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
  );
}

function ProgressChip({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-[18px] border border-[var(--border-default)] bg-[var(--bg-card)] px-3 py-3">
      <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-[var(--text-muted)]">{label}</p>
      <p className="mt-2 text-2xl font-semibold tracking-[-0.04em] text-[var(--text-primary)]">{value}</p>
    </div>
  );
}
