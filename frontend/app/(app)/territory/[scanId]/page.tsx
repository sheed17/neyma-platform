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

  const scanTimestamp = data?.completed_at || data?.created_at || null;
  const scanAgeDays = useMemo(() => {
    if (!scanTimestamp) return null;
    return Math.max(0, Math.floor((Date.now() - new Date(scanTimestamp).getTime()) / 86400000));
  }, [scanTimestamp]);

  const outcomeLabel = (status?: string) => {
    if (!status) return "Not contacted";
    if (status === "contacted") return "Contacted";
    if (status === "closed_won") return "Won";
    if (status === "closed_lost") return "Lost";
    return "Not contacted";
  };

  async function ensureBrief(row: ProspectRow): Promise<number> {
    if (!row.prospect_id) throw new Error("Prospect id missing");
    const ensured = await ensureTerritoryProspectBrief(row.prospect_id);
    if (ensured.status === "ready" && ensured.diagnostic_id) return ensured.diagnostic_id;
    if (!ensured.job_id) throw new Error("No job id returned");
    for (let i = 0; i < 240; i++) {
      const st = await getJobStatus(ensured.job_id);
      if (st.status === "completed" && st.diagnostic_id) return st.diagnostic_id;
      if (st.status === "failed") throw new Error(st.error || "Failed to build brief");
      await new Promise((r) => setTimeout(r, 2000));
    }
    throw new Error("Brief build timed out");
  }

  async function handleViewBrief(row: ProspectRow) {
    try {
      setActionProspectId(row.prospect_id || -1);
      setActionLabel("Building your brief...");
      const id = row.diagnostic_id && row.full_brief_ready ? row.diagnostic_id : await ensureBrief(row);
      router.push(`/diagnostic/${id}?from=territory&scanId=${encodeURIComponent(scanId)}`);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to open brief");
    } finally {
      setActionProspectId(null);
      setActionLabel(null);
    }
  }

  async function handleAddToList(row: ProspectRow) {
    try {
      setActionProspectId(row.prospect_id || -1);
      setActionLabel("Preparing full brief...");
      const diagId = row.diagnostic_id && row.full_brief_ready ? row.diagnostic_id : await ensureBrief(row);
      const selected = window.prompt(`Add to list by number:\n${lists.map((l, i) => `${i + 1}. ${l.name}`).join("\n")}\nOr type a new list name.`);
      if (!selected) return;
      const n = Number(selected);
      const listId = !Number.isNaN(n) && n >= 1 && n <= lists.length ? lists[n - 1].id : (await createProspectList(selected.trim())).id;
      await addProspectsToList(listId, [diagId]);
      setLists((await getProspectLists()).items);
      alert("Added to list");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to add to list");
    } finally {
      setActionProspectId(null);
      setActionLabel(null);
    }
  }

  async function handleOutcome(row: ProspectRow, status: "contacted" | "closed_won" | "closed_lost") {
    if (!row.diagnostic_id) return;
    await markProspectOutcome({ diagnostic_id: row.diagnostic_id, status });
    setData(await getTerritoryScanResults(scanId));
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

  if (loading) return <div className="mx-auto max-w-6xl py-10 text-sm text-[var(--text-muted)]">Loading scan...</div>;

  return (
    <div className="mx-auto max-w-7xl">
      <div className="mb-4 flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="text-xs text-[var(--text-muted)]">Territory</p>
          <h1 className="text-2xl font-semibold tracking-tight">Territory: {data?.city || "Market"}{data?.state ? `, ${data.state}` : ""}</h1>
          <p className="mt-1 text-sm text-[var(--text-muted)]">{prospects.length} prospects · {data?.status === "running" ? progress.label : "Scan complete"}</p>
          {scanTimestamp && (
            <p className="mt-1 text-xs text-[var(--text-muted)]">
              {scanAgeDays && scanAgeDays > 0 ? `Last updated ${scanAgeDays} days ago` : "Just updated"} · {new Date(scanTimestamp).toLocaleDateString("en-US")}
            </p>
          )}
        </div>
        <div className="flex flex-wrap gap-2">
          <Link href="/territory/new"><Button>New scan</Button></Link>
          <Link href="/lists"><Button>Lists</Button></Link>
          <Button variant="primary" onClick={exportCsv}>Export CSV</Button>
        </div>
      </div>

      {scanAgeDays != null && scanAgeDays > 30 && (
        <Card className="mb-4 border-amber-200 bg-amber-50">
          <CardBody className="text-sm text-amber-900">
            This list is {scanAgeDays} days old. Refresh to see latest review counts and competitors.{" "}
            <Link href={`/territory/new?city=${encodeURIComponent(data?.city || "")}&state=${encodeURIComponent(data?.state || "")}&vertical=${encodeURIComponent(data?.vertical || "")}`} className="app-link font-medium">
              Run new scan
            </Link>
          </CardBody>
        </Card>
      )}

      {error && <Card className="mb-4 border-rose-200 bg-rose-50"><CardBody className="text-sm text-rose-800">{error}</CardBody></Card>}
      {actionLabel && <Card className="mb-4 border-sky-200 bg-sky-50"><CardBody className="text-sm text-sky-800">{actionLabel}</CardBody></Card>}

      <Card>
        <CardHeader
          title="Ranked Prospects"
          subtitle={`Showing top ${Number(data?.summary?.accepted || prospects.length)} of ${Number(data?.summary?.scored_candidates || 0)} scored prospects.`}
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
              <TH>Rank</TH><TH>Business</TH><TH>City</TH><TH>Rating</TH><TH>Reviews</TH><TH>Contact</TH><TH>Website</TH><TH>Email</TH><TH>Key Signal</TH><TH>Status</TH><TH className="text-right">Actions</TH>
            </tr>
          </THead>
          <tbody>
            {prospects.map((row) => {
              const busy = actionProspectId != null && actionProspectId === row.prospect_id;
              return (
                <TR key={`${row.prospect_id || row.place_id}`}>
                  <TD className="font-medium text-[var(--text-primary)]">#{row.rank || "-"}</TD>
                  <TD className="font-medium text-[var(--text-primary)]">{row.business_name}</TD>
                  <TD>{row.city}{row.state ? `, ${row.state}` : ""}</TD>
                  <TD>{row.rating ?? "-"}</TD>
                  <TD>{row.user_ratings_total ?? "-"}</TD>
                  <TD>{row.phone ? <a href={`tel:${row.phone}`} className="app-link">{row.phone}</a> : "—"}</TD>
                  <TD>{row.website ? <a href={row.website} target="_blank" rel="noreferrer" className="app-link break-all">{row.website}</a> : "—"}</TD>
                  <TD>{row.email ? <a href={`mailto:${row.email}`} className="app-link">{row.email}</a> : "—"}</TD>
                  <TD>{row.key_signal ? <Badge>{row.key_signal}</Badge> : "—"}</TD>
                  <TD><Badge tone={row.outcome_status?.status === "closed_won" ? "success" : row.outcome_status?.status === "closed_lost" ? "danger" : "muted"}>{outcomeLabel(row.outcome_status?.status)}</Badge></TD>
                  <TD className="text-right">
                    <button onClick={() => void handleViewBrief(row)} disabled={busy} className="app-link mr-2 font-medium disabled:opacity-60">{busy ? "Building..." : "View brief"}</button>
                    <button onClick={() => void handleAddToList(row)} disabled={busy} className="text-[var(--text-secondary)] hover:underline disabled:opacity-60">Add to list</button>
                    <button onClick={() => void handleOutcome(row, "contacted")} disabled={!row.diagnostic_id} className="ml-2 text-[var(--text-secondary)] hover:underline disabled:opacity-50">Contacted</button>
                  </TD>
                </TR>
              );
            })}
          </tbody>
        </Table>
      </Card>
    </div>
  );
}
