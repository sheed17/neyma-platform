"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import { ExternalLink } from "lucide-react";
import {
  addProspectsToList,
  createProspectList,
  ensureTerritoryProspectBrief,
  getJobStatus,
  getProspectLists,
  getTerritoryScanResults,
} from "@/lib/api";
import { useAuth } from "@/lib/auth";
import { clientFacingAppError, clientFacingBriefError } from "@/lib/present";
import type { ProspectList, ProspectRow, TerritoryScanResultsResponse } from "@/lib/types";
import Button from "@/app/components/ui/Button";
import { Card, CardBody, CardHeader } from "@/app/components/ui/Card";
import ListPickerModal from "@/app/components/ListPickerModal";
import BriefBuildProgress, { type BriefBuildProgressState } from "@/app/components/BriefBuildProgress";
import { Dots_v2 } from "@/components/ui/spinner";

function territoryBriefReady(row: Pick<ProspectRow, "diagnostic_id" | "full_brief_ready">): boolean {
  return Boolean(row.diagnostic_id && row.full_brief_ready);
}

function websiteDomain(raw?: string | null): string | null {
  if (!raw) return null;
  const normalized = raw.startsWith("http://") || raw.startsWith("https://") ? raw : `https://${raw}`;
  try {
    const parsed = new URL(normalized);
    return parsed.hostname.replace(/^www\./, "") || null;
  } catch {
    return raw.replace(/^https?:\/\//, "").split("/")[0]?.replace(/^www\./, "") || null;
  }
}

function statusPillClass({ busy, briefReady }: { busy: boolean; briefReady: boolean }) {
  if (busy) {
    return "border border-[var(--border-default)] bg-[var(--surface)] text-[var(--text-secondary)]";
  }
  if (briefReady) {
    return "border border-transparent bg-[#EAF3DE] text-[#3B6D11]";
  }
  return "border-[0.5px] border-[var(--border-default)] bg-[var(--surface)] text-[var(--text-muted)]";
}

function territoryPhaseLabel(phase: string) {
  const normalized = phase.toLowerCase();
  if (normalized.includes("candidate")) return "Discovering";
  if (normalized.includes("complete")) return "Complete";
  if (normalized.includes("ai")) return "Finalizing";
  if (normalized.includes("rank")) return "Ranking";
  return "In progress";
}

export default function TerritoryResultsPage() {
  const params = useParams();
  const router = useRouter();
  const { access } = useAuth();
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
  const [rowsVisible, setRowsVisible] = useState(false);
  const cacheKey = `territory_scan_${scanId}`;
  const canUseWorkspace = access?.can_use.workspace !== false;
  const canSave = access?.can_use.save !== false;
  const canExport = access?.can_use.export !== false;

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
          if (!cancelled) setError(clientFacingAppError(err instanceof Error ? err.message : "Failed to load scan", "We couldn't load this market scan right now. Please try again."));
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
    if (!canSave) {
      setLists([]);
      return;
    }
    let cancelled = false;
    async function loadLists() {
      const ls = await getProspectLists().catch(() => ({ items: [] }));
      if (!cancelled) setLists(ls.items);
    }
    void loadLists();
    return () => {
      cancelled = true;
    };
  }, [canSave]);

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
        marketHint: data?.market_hint || null,
        elapsedLabel:
          data?.status === "running" && data?.created_at
            ? (() => {
                const startedAt = new Date(data.created_at).getTime();
                if (Number.isNaN(startedAt)) return null;
                const elapsedSeconds = Math.max(0, Math.floor((Date.now() - startedAt) / 1000));
                if (elapsedSeconds < 60) return `${elapsedSeconds}s elapsed`;
                return `${Math.floor(elapsedSeconds / 60)}m ${elapsedSeconds % 60}s elapsed`;
              })()
            : null,
      }),
    [data?.city, data?.state, data?.status, data?.summary, data?.market_hint, data?.created_at, loading, prospects.length],
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

  useEffect(() => {
    if (prospects.length === 0) {
      setRowsVisible(false);
      return;
    }
    const timer = window.setTimeout(() => setRowsVisible(true), 120);
    return () => window.clearTimeout(timer);
  }, [prospects.length]);

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
      if (st.status === "failed") throw new Error(clientFacingBriefError(st.error));
      setBriefProgress(buildBriefProgressState(st.progress, i + 1, row));
      await new Promise((r) => setTimeout(r, 2000));
    }
    throw new Error(clientFacingBriefError("Brief build timed out"));
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
      setError(clientFacingBriefError(err instanceof Error ? err.message : "Failed to open brief"));
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
      setError(clientFacingBriefError(err instanceof Error ? err.message : "Failed to add to list"));
    } finally {
      setActionProspectId(null);
      setActionLabel(null);
      setBriefProgress(null);
    }
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
          <p className="text-[11px] font-medium uppercase tracking-[0.08em] text-[var(--text-muted)]">Territory</p>
          <h1 className="mt-1 text-[20px] font-medium tracking-[-0.01em] text-[var(--text-primary)]">Market Scan: {data?.city || "Market"}{data?.state ? `, ${data.state}` : ""}</h1>
          <p className="mt-1 text-[12px] leading-[1.8] text-[var(--text-muted)]">
            {data?.status === "running"
              ? `${territoryProgress.label}${territoryProgress.supportingLabel ? ` · ${territoryProgress.supportingLabel}` : ""}`
              : `${prospects.length} prospects ready`}
          </p>
          {prospects.length > 0 && (
            <p className="text-[12px] leading-[1.8] text-[var(--text-muted)]">
              <span className="font-semibold text-[var(--text-secondary)]">{readyBriefCount} briefs ready</span> ·{" "}
              <span className="font-semibold text-[var(--text-secondary)]">{pendingBriefCount} not built yet</span>
            </p>
          )}
          {scanTimestamp && (
            <p className="text-[12px] leading-[1.8] text-[var(--text-muted)]">
              {scanAgeDays && scanAgeDays > 0 ? `Last updated ${scanAgeDays} days ago` : "Just updated"} · {new Date(scanTimestamp).toLocaleDateString("en-US")}
            </p>
          )}
        </div>
        <div className="flex flex-wrap gap-2">
          <Link href="/territory/new"><Button variant="secondary">New scan</Button></Link>
          {canUseWorkspace ? <Link href="/lists"><Button variant="secondary">Lists</Button></Link> : null}
          {canExport ? <Button variant="primary" onClick={exportCsv}>Export CSV</Button> : null}
        </div>
      </div>

      {!canUseWorkspace ? (
        <Card className="mb-4 border border-[var(--border-default)] bg-[var(--muted)]">
          <CardBody className="text-sm text-[var(--text-secondary)]">
            Create a <span className="font-semibold text-[var(--text-primary)]">free account</span> to <span className="font-semibold text-[var(--text-primary)]">save leads</span>, reopen scans from the workspace, and export this market.
          </CardBody>
        </Card>
      ) : null}

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
          subtitle={
            data?.status === "running" && prospects.length === 0
              ? "Neyma is still assembling the first ranked results. The table will fill in as the shortlist becomes ready."
              : `Showing top ${Number(data?.summary?.accepted || prospects.length)} of ${Number(data?.summary?.scored_candidates || 0)} scored prospects. ${readyBriefCount} briefs are ready, ${pendingBriefCount} still need to be built.`
          }
          action={
            <select value={sortBy} onChange={(e) => setSortBy(e.target.value as "rank" | "name" | "reviews")} className="h-8 rounded-[var(--radius-sm)] border border-[var(--border-default)] px-2 text-xs">
              <option value="rank">Sort: Rank</option>
              <option value="name">Sort: Name</option>
              <option value="reviews">Sort: Reviews</option>
            </select>
          }
        />
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="text-left text-[11px] uppercase tracking-[0.14em] text-[var(--text-muted)]">
              <tr className="border-b-[0.5px] border-[var(--border-default)]">
                <th className="w-[36px] px-0 py-[11px] text-center font-medium">Rank</th>
                <th className="px-[10px] py-[11px] font-medium">Business</th>
                <th className="px-[10px] py-[11px] font-medium">City</th>
                <th className="px-[10px] py-[11px] text-center font-medium">Rating</th>
                <th className="px-[10px] py-[11px] text-center font-medium">Reviews</th>
                <th className="px-[10px] py-[11px] font-medium">Contact</th>
                <th className="px-[10px] py-[11px] font-medium">Website</th>
                <th className="min-w-[160px] px-[10px] py-[11px] text-right font-medium">Actions</th>
              </tr>
            </thead>
            <tbody>
              {prospects.length === 0 && data?.status === "running" ? (
                <tr>
                  <td colSpan={8} className="px-[18px] py-6">
                    <div className="rounded-[18px] border border-[var(--border-default)] bg-[var(--muted)] p-4">
                      <div className="flex items-center gap-2 text-[13px] font-medium text-[var(--text-primary)]">
                        <span className="shrink-0 scale-[0.5] text-[var(--primary)]">
                          <Dots_v2 />
                        </span>
                        <span>Assembling the first ranked results</span>
                      </div>
                      <p className="mt-2 text-[12px] leading-6 text-[var(--text-muted)]">
                        The shortlist will appear here once the <span className="font-semibold text-[var(--text-secondary)]">strongest candidates</span> are ready for review.
                      </p>
                      <div className="mt-4 space-y-2">
                        {Array.from({ length: 3 }).map((_, index) => (
                          <div
                            key={index}
                            className="flex items-center gap-3 rounded-[14px] border border-[var(--border-default)] bg-[var(--bg-card)] px-3 py-3"
                          >
                            <div className="h-4 w-4 rounded-full bg-[var(--muted)]" />
                            <div className="min-w-0 flex-1">
                              <div className="h-3 w-40 rounded-full bg-[var(--muted)]" />
                              <div className="mt-2 h-3 w-28 rounded-full bg-[var(--muted)]" />
                            </div>
                            <div className="h-8 w-20 rounded-[10px] bg-[var(--muted)]" />
                          </div>
                        ))}
                      </div>
                    </div>
                  </td>
                </tr>
              ) : prospects.length === 0 ? (
                <tr>
                  <td colSpan={8} className="px-[18px] py-8 text-center text-[13px] text-[var(--text-muted)]">
                    No prospects yet.
                  </td>
                </tr>
              ) : prospects.map((row) => {
                const busy = actionProspectId != null && actionProspectId === row.prospect_id;
                const briefReady = territoryBriefReady(row);
                const domain = websiteDomain(row.website);
                const ratingText = typeof row.rating === "number" ? row.rating.toFixed(1) : null;
                const rank = typeof row.rank === "number" ? String(row.rank) : "—";
                const topRank = typeof row.rank === "number" && row.rank <= 3;

                return (
                  <tr
                    key={`${row.prospect_id || row.place_id}`}
                    className={`border-b-[0.5px] border-[var(--border-default)] align-middle transition-all duration-500 hover:bg-[var(--surface)] ${
                      rowsVisible ? "translate-y-0 opacity-100" : "translate-y-1 opacity-0"
                    }`}
                    style={{ transitionDelay: `${Math.min((row.rank || 1) - 1, 5) * 60}ms` }}
                  >
                    <td className={`px-0 py-[11px] text-center align-middle text-[11px] font-medium ${topRank ? "text-[var(--primary)]" : "text-[var(--text-muted)]"}`}>
                      {rank}
                    </td>
                    <td className="px-[10px] py-[11px] align-middle">
                      <div className="flex flex-col gap-1">
                        <span className="text-[13px] font-medium text-[var(--text-primary)]">{row.business_name}</span>
                        <span className={`inline-flex w-fit whitespace-nowrap rounded-full px-[7px] py-[2px] text-[10px] font-medium ${statusPillClass({ busy, briefReady })}`}>
                          {busy ? "Building brief" : briefReady ? "Brief ready" : "Not built"}
                        </span>
                      </div>
                    </td>
                    <td className="px-[10px] py-[11px] align-middle text-[12px] text-[var(--text-secondary)]">
                      {row.city}{row.state ? `, ${row.state}` : ""}
                    </td>
                    <td className="px-[10px] py-[11px] text-center align-middle text-[13px] font-medium text-[var(--text-primary)]">
                      {ratingText || "—"}
                    </td>
                    <td className="px-[10px] py-[11px] text-center align-middle text-[12px] text-[var(--text-muted)]">
                      {typeof row.user_ratings_total === "number" ? row.user_ratings_total : "—"}
                    </td>
                    <td className="px-[10px] py-[11px] align-middle">
                      {row.phone ? (
                        <a href={`tel:${row.phone}`} className="whitespace-nowrap text-[12px] text-[var(--primary)] transition hover:underline">
                          {row.phone}
                        </a>
                      ) : (
                        <span className="text-[12px] text-[var(--text-muted)]">—</span>
                      )}
                    </td>
                    <td className="max-w-[130px] px-[10px] py-[11px] align-middle">
                      {row.website && domain ? (
                        <a
                          href={row.website}
                          target="_blank"
                          rel="noreferrer"
                          className="inline-flex max-w-[130px] items-center gap-1 truncate text-[12px] text-[var(--text-secondary)] transition hover:text-[var(--text-primary)]"
                        >
                          <ExternalLink className="h-[11px] w-[11px] shrink-0" />
                          <span className="truncate">{domain}</span>
                        </a>
                      ) : (
                        <span className="text-[12px] text-[var(--text-muted)]">—</span>
                      )}
                    </td>
                    <td className="min-w-[160px] px-[10px] py-[11px] text-right align-middle">
                      <div className="flex justify-end gap-[6px]">
                        <button
                          onClick={() => void handleViewBrief(row)}
                          disabled={busy}
                          className={`inline-flex h-[31px] items-center justify-center whitespace-nowrap rounded-[8px] px-[11px] text-[12px] font-medium transition disabled:opacity-60 ${
                            briefReady || busy
                              ? "bg-[var(--primary)] text-white hover:brightness-95"
                              : "border border-[var(--border-default)] bg-transparent text-[var(--text-primary)] hover:bg-[var(--surface)]"
                          }`}
                        >
                          {busy ? "Building..." : briefReady ? "Open brief" : "Build brief"}
                        </button>
                        {canSave ? (
                          <button
                            onClick={() => void handleAddToList(row)}
                            disabled={busy}
                            className="inline-flex h-[31px] items-center justify-center whitespace-nowrap rounded-full border border-[var(--border-default)] bg-transparent px-[10px] text-[11px] text-[var(--text-muted)] transition hover:bg-[var(--surface)] hover:text-[var(--text-primary)] disabled:opacity-60"
                          >
                            + List
                          </button>
                        ) : null}
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </Card>

      {listModalOpen && canSave && (
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
  processed: number;
  total: number;
  accepted: number;
  marketHint?: string | null;
  elapsedLabel?: string | null;
  supportingLabel?: string | null;
};

function buildTerritoryProgressState({
  city,
  state,
  status,
  summary,
  currentCount,
  marketHint,
  elapsedLabel,
}: {
  city: string;
  state: string;
  status: string;
  summary: Record<string, unknown>;
  currentCount: number;
  marketHint?: string | null;
  elapsedLabel?: string | null;
}): TerritoryScanProgressState {
  const phase = String(summary.phase || (status === "running" ? "candidate_fetch" : "completed"));
  const queryDone = Number(summary.candidate_queries_done || 0);
  const queryTotal = Number(summary.candidate_queries_total || 0);
  const processed = Number(summary.processed || 0);
  const total = Number(summary.scored_candidates || summary.total_candidates || 0);
  const accepted = Number(summary.accepted || currentCount || 0);
  const label =
    phase === "candidate_fetch" && queryTotal > 0
      ? "Searching the market"
      : phase.includes("ai")
        ? "Finalizing the shortlist"
      : total > 0
        ? "Ranking local candidates"
        : status === "completed"
          ? "Scan complete"
          : "Preparing scan";
  const supportingLabel =
    phase === "candidate_fetch" && queryTotal > 0
      ? `Expanding coverage across ${queryDone}/${queryTotal} nearby areas`
      : phase.includes("ai")
        ? `Ordering the strongest ${total || accepted || currentCount} ranked candidates into the final shortlist`
        : total > 0
          ? `Scoring ${processed}/${total} candidates`
          : null;
  const percent =
    phase === "candidate_fetch" && queryTotal > 0
      ? 12 + Math.round((queryDone / queryTotal) * 34)
      : phase.includes("ai")
        ? 92
        : total > 0
          ? 52 + Math.round((processed / total) * 24)
          : status === "completed"
            ? 100
            : 8;

  return {
    city,
    state,
    phase,
    label,
    percent,
    processed,
    total,
    accepted,
    marketHint,
    elapsedLabel,
    supportingLabel,
  };
}

function TerritoryScanProgressPanel({
  progress,
  className = "",
}: {
  progress: TerritoryScanProgressState;
  className?: string;
}) {
  const [subeventIndex, setSubeventIndex] = useState(0);
  const phase = progress.phase.toLowerCase();
  const subevents = phase.includes("candidate")
    ? [
        "Checking the local map for relevant practices",
        "Expanding the search across nearby listings",
        "Pulling the first candidate set into the scan",
      ]
    : phase.includes("ai")
      ? [
          "Refining the strongest candidates before the final shortlist",
          "Checking which ranked practices deserve the top spots",
          "Finalizing the ordered shortlist for review",
        ]
    : [
        "Comparing reviews and local position",
        "Checking website basics and contact readiness",
        "Sorting the strongest prospects into the shortlist",
      ];
  const steps = [
    {
      label: "Discover candidates",
      meta: phase.includes("candidate") ? "IN PROGRESS" : "DONE",
      description: "Search the market before any ranking starts.",
      done: !phase.includes("candidate") && (progress.total > 0 || progress.accepted > 0),
      active: phase.includes("candidate") || (progress.accepted === 0 && progress.total === 0),
    },
    {
      label: "Rank the market",
      meta: phase.includes("ai")
        ? `${progress.total || progress.accepted} READY`
        : progress.total > 0
          ? `${progress.processed}/${progress.total} RANKED`
          : "QUEUED",
      description: "Review local position, review gaps, and site readiness.",
      done: !phase.includes("candidate") && !phase.includes("ai") && progress.processed > 0,
      active: !phase.includes("candidate") && !phase.includes("ai") && (progress.processed < progress.total || progress.total === 0),
    },
    {
      label: "Build shortlist",
      meta: phase.includes("ai")
        ? "FINALIZING"
        : progress.accepted > 0
          ? `${progress.accepted} READY`
          : "QUEUED",
      description: "Keep the strongest practices and drop weak fits.",
      done: progress.accepted > 0 && !phase.includes("candidate") && !phase.includes("ai"),
      active: phase.includes("ai") || (progress.accepted > 0 && progress.processed >= Math.max(progress.total, 1)),
    },
  ];

  useEffect(() => {
    const subeventTimer = window.setInterval(() => {
      setSubeventIndex((current) => (current + 1) % subevents.length);
    }, 1700);
    return () => {
      window.clearInterval(subeventTimer);
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
                  {territoryPhaseLabel(progress.phase)}
                </span>
              </div>
              <p className="mt-1 text-sm text-[var(--text-secondary)]">
                {progress.city || "Selected market"}{progress.state ? `, ${progress.state}` : ""}
              </p>
              <div className="mt-2 flex items-center gap-2">
                <span className="shrink-0 scale-[0.5] text-[var(--primary)]">
                  <Dots_v2 />
                </span>
                <p className="text-xs leading-5 text-[var(--text-muted)]">{subevents[subeventIndex]}</p>
              </div>
              <p className="mt-2 text-xs font-medium text-[var(--text-secondary)]">{progress.label}</p>
              {progress.supportingLabel ? (
                <p className="mt-1 text-[11px] leading-5 text-[var(--text-muted)]">{progress.supportingLabel}</p>
              ) : null}
              {progress.elapsedLabel ? (
                <p className="mt-1 text-[11px] text-[var(--text-muted)]">{progress.elapsedLabel}</p>
              ) : null}
              {progress.marketHint ? (
                <p className="mt-2 max-w-[240px] text-[11px] leading-5 text-[var(--text-muted)]">
                  {progress.marketHint}
                </p>
              ) : null}
            </div>
          </div>

          <div className="min-w-0 flex-1">
            <div className="grid gap-2 sm:grid-cols-3">
              <ProgressChip label="Phase" value={territoryPhaseLabel(progress.phase)} />
              <ProgressChip
                label="Ranked"
                value={
                  phase.includes("ai")
                    ? (progress.total > 0 ? `${progress.total} ready` : "Finalizing")
                    : progress.total > 0
                      ? `${progress.processed}/${progress.total}`
                      : progress.processed > 0
                        ? String(progress.processed)
                        : "Building"
                }
              />
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
