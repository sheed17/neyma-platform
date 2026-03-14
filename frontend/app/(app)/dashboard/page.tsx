"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { deleteDiagnostic, getBaseUrl, getOutcomesSummary, getRecentTerritoryScans, listDiagnostics } from "@/lib/api";
import type { DiagnosticListItem, OutcomesSummaryResponse, TerritoryScanListItem } from "@/lib/types";
import Button from "@/app/components/ui/Button";
import Input from "@/app/components/ui/Input";
import { Card, CardBody, CardHeader } from "@/app/components/ui/Card";
import Badge from "@/app/components/ui/Badge";
import { Table, THead, TH, TR, TD } from "@/app/components/ui/Table";
import EmptyState from "@/app/components/ui/EmptyState";
import { Skeleton } from "@/app/components/ui/Skeleton";
import { BorderTrail } from "@/components/ui/border-trail";

const PAGE_SIZE = 10;

function fmtDate(iso: string) {
  return new Date(iso).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

function summarizeOpportunity(value?: string | null) {
  if (!value) return "No signal summary";
  const cleaned = value.replace(/\s+/g, " ").trim();
  return cleaned.split("(")[0].replace(/[:,-]\s*$/, "").trim() || cleaned;
}

function summarizeConstraint(value?: string | null, fallback?: string | null) {
  const source = value || fallback || "";
  if (!source) return null;
  const cleaned = source.replace(/\s+/g, " ").trim();
  if (cleaned.length <= 88) return cleaned;
  return `${cleaned.slice(0, 85).trim()}...`;
}

function MiniStat({ label, value, className = "" }: { label: string; value: string; className?: string }) {
  return (
    <div className={`bg-white px-4 py-5 ${className}`}>
      <p className="section-kicker">{label}</p>
      <p className="mt-3 text-[2.8rem] font-bold leading-none tracking-[-0.055em] text-[var(--text-primary)]">{value}</p>
    </div>
  );
}

export default function DashboardPage() {
  const [items, setItems] = useState<DiagnosticListItem[]>([]);
  const [scans, setScans] = useState<TerritoryScanListItem[]>([]);
  const [summary, setSummary] = useState<OutcomesSummaryResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(0);

  useEffect(() => {
    async function load() {
      setLoading(true);
      setError(null);
      try {
        const [diag, recent, out] = await Promise.all([
          listDiagnostics(200, 0),
          getRecentTerritoryScans(10),
          getOutcomesSummary().catch(() => null),
        ]);
        setItems(diag.items);
        setScans(recent.items);
        setSummary(out);
      } catch (err) {
        setItems([]);
        setScans([]);
        setSummary(null);
        setError(
          err instanceof Error
            ? err.message
            : `Unable to load workspace data from ${getBaseUrl()}.`,
        );
      } finally {
        setLoading(false);
      }
    }
    void load();
  }, []);

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return items;
    return items.filter((d) => `${d.business_name} ${d.city} ${d.state || ""} ${d.opportunity_profile || ""}`.toLowerCase().includes(q));
  }, [items, search]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const pageItems = filtered.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);
  const cities = new Set(items.map((d) => d.city)).size;
  async function onDelete(id: number) {
    if (!confirm("Delete this brief?")) return;
    await deleteDiagnostic(id);
    setItems((prev) => prev.filter((x) => x.id !== id));
  }

  if (loading) {
    return (
      <div className="mx-auto max-w-[var(--max-content)] space-y-4">
        <Skeleton className="h-28" />
        <div className="grid grid-cols-2 gap-4 xl:grid-cols-4">{[0, 1, 2, 3].map((i) => <Skeleton key={i} className="h-24" />)}</div>
        <Skeleton className="h-72" />
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-[var(--max-content)] space-y-5">
      {error ? (
        <Card>
          <CardBody className="p-5">
            <p className="card-title text-[var(--text-primary)]">Workspace unavailable</p>
            <p className="mt-2 text-sm leading-6 text-[var(--text-secondary)]">{error}</p>
            <p className="mt-2 text-sm leading-6 text-[var(--text-secondary)]">
              The frontend is trying to reach <span className="font-mono text-[var(--text-primary)]">{getBaseUrl()}</span>.
            </p>
          </CardBody>
        </Card>
      ) : null}

      <section className="grid gap-4 xl:grid-cols-[1.15fr_0.85fr]">
        <Card className="relative overflow-hidden">
          <BorderTrail
            className="bg-[var(--primary)] opacity-70"
            size={92}
            style={{
              boxShadow:
                "0 0 22px 10px rgba(139,80,212,0.12), 0 0 44px 18px rgba(139,80,212,0.08)",
            }}
          />
          <CardBody className="p-5 sm:p-6">
            <p className="section-kicker">Workspace</p>
            <h2 className="mt-3 text-4xl font-semibold tracking-[-0.06em] text-[var(--text-primary)] sm:text-[3.3rem]">
              What should happen next?
            </h2>
            <p className="mt-4 max-w-[46ch] text-lg leading-relaxed text-[var(--text-secondary)]">
              Use the workspace to start a market, narrow a shortlist, or open the next brief that needs attention.
            </p>
            <div className="mt-6 flex flex-wrap gap-3">
              <Link href="/territory/new">
                <Button variant="primary" className="h-11 rounded-full px-5">Run territory scan</Button>
              </Link>
              <Link href="/ask">
                <Button variant="secondary" className="h-11 rounded-full px-5">Ask Neyma</Button>
              </Link>
              <Link href="/diagnostic/new">
                <Button variant="secondary" className="h-11 rounded-full px-5">Build brief</Button>
              </Link>
            </div>
          </CardBody>
        </Card>

        <Card className="border-l-2" style={{ borderLeftColor: "var(--primary)", boxShadow: "0 12px 30px rgba(0,0,0,0.05)" }}>
          <CardHeader title="Next Up" subtitle="The cleanest next action based on the current workflow." />
          <CardBody className="space-y-4">
            <div className="app-surface border border-[var(--border-default)]">
              <p className="card-title text-[var(--text-primary)]">
                {scans.length === 0
                  ? "Run a territory scan for the next market."
                  : items.length === 0
                    ? "Use Ask Neyma to narrow the current market."
                    : "Open the next brief or continue narrowing in Ask Neyma."}
              </p>
              <p className="mt-2 text-base leading-7 text-[var(--text-secondary)]">
                {scans.length === 0
                  ? "Start with the market before building any deeper brief."
                  : items.length === 0
                    ? "You already have market coverage. Tighten the shortlist before opening more briefs."
                    : "The system already has active work. Move forward from the current shortlist instead of starting over."}
              </p>
            </div>
            <div className="grid gap-2 sm:grid-cols-2">
              <Link href="/territory/new"><Button variant="primary" className="w-full rounded-full">Start with territory</Button></Link>
              <Link href="/ask"><Button variant="secondary" className="w-full rounded-full">Continue in Ask</Button></Link>
            </div>
          </CardBody>
        </Card>
      </section>

      {summary && (
        <Card>
          <CardHeader title="Pipeline Status" subtitle="Latest outreach state across briefs and lists." />
          <CardBody className="grid gap-0 overflow-hidden rounded-[16px] border border-[var(--border-default)] bg-[var(--surface)] sm:grid-cols-2 xl:grid-cols-5">
            <MiniStat label="Contacted" value={String(summary.contacted || 0)} className="rounded-none sm:border-r sm:border-[var(--border-default)]" />
            <MiniStat label="Won" value={String(summary.closed_won || 0)} className="rounded-none xl:border-r xl:border-[var(--border-default)]" />
            <MiniStat label="Lost" value={String(summary.closed_lost || 0)} className="rounded-none sm:border-r sm:border-[var(--border-default)] xl:border-r xl:border-[var(--border-default)]" />
            <MiniStat label="Not contacted" value={String(summary.not_contacted || 0)} className="rounded-none xl:border-r xl:border-[var(--border-default)]" />
            <MiniStat label="Won (30d)" value={String(summary.closed_this_month || 0)} className="rounded-none" />
          </CardBody>
        </Card>
      )}

      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <MiniStat label="Total briefs" value={String(items.length)} />
        <MiniStat label="Cities covered" value={String(cities)} />
        <MiniStat label="Recent scans" value={String(scans.length)} />
        <MiniStat label="Briefs added (30d)" value={String(items.filter((d) => { const dt = new Date(d.created_at); const n = new Date(); return dt.getMonth() === n.getMonth() && dt.getFullYear() === n.getFullYear(); }).length)} />
      </div>

      <div className="grid gap-4 xl:grid-cols-[1.05fr_0.95fr]">
        {scans.length > 0 && (
          <Card>
            <CardHeader title="Recent Territory Scans" subtitle="Resume active scans or reopen completed market shortlists." />
            <Table>
              <THead><tr><TH>Market</TH><TH>Vertical</TH><TH>Prospects</TH><TH>Status</TH><TH>Date</TH><TH className="text-right">Open</TH></tr></THead>
              <tbody>
                {scans.slice(0, 5).map((s) => (
                  <TR key={s.id}>
                    <TD>{s.city || "—"}{s.state ? `, ${s.state}` : ""}</TD>
                    <TD>{s.vertical || "—"}</TD>
                    <TD>{s.prospects_count ?? Number((s.summary?.accepted as number) || 0)}</TD>
                    <TD><Badge tone={s.status === "completed" ? "success" : "muted"}>{s.status}</Badge></TD>
                    <TD>{fmtDate(s.created_at)}</TD>
                    <TD className="text-right">
                      <Link href={`/territory/${s.id}`} className="inline-flex h-9 items-center justify-center rounded-[8px] border border-[var(--border-default)] bg-white px-4 text-sm font-medium text-[var(--text-primary)] transition hover:bg-[var(--surface)]">
                        Open scan
                      </Link>
                    </TD>
                  </TR>
                ))}
              </tbody>
            </Table>
          </Card>
        )}

        <Card>
          <CardHeader title="Recent Work" subtitle="Jump back into the most recent briefs." />
          <CardBody className="space-y-2">
            {items.slice(0, 5).map((item) => (
              <div key={item.id} className="flex items-center justify-between gap-3 rounded-[12px] border border-[var(--border-default)] bg-white px-4 py-3">
                <div className="min-w-0">
                  <p className="truncate text-sm font-medium text-[var(--text-primary)]">{item.business_name}</p>
                  <p className="mt-1 text-xs text-[var(--text-muted)]">{item.city}{item.state ? `, ${item.state}` : ""} · {fmtDate(item.created_at)}</p>
                </div>
                <Link href={`/diagnostic/${item.id}`} className="inline-flex shrink-0 h-9 items-center justify-center rounded-[8px] border border-[var(--border-default)] bg-white px-4 text-sm font-medium text-[var(--text-primary)] transition hover:bg-[var(--surface)]">
                  Open brief
                </Link>
              </div>
            ))}
          </CardBody>
        </Card>
      </div>

      <Card>
        <CardHeader title="Brief Archive" subtitle="Search, filter, and reopen saved briefs." action={<div className="w-72"><Input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Search business, city, signal..." /></div>} />
        {items.length === 0 ? (
          <CardBody>
            <EmptyState
              title="No briefs yet"
              description="Start with a territory scan or Ask query to generate your first shortlist."
              action={<Link href="/territory/new"><Button variant="primary">Run territory scan</Button></Link>}
            />
          </CardBody>
        ) : (
          <>
            <Table>
              <THead><tr><TH>Business</TH><TH>City</TH><TH>Brief summary</TH><TH>Date</TH><TH className="text-right">Actions</TH></tr></THead>
              <tbody>
                {pageItems.map((item) => (
                  <TR key={item.id}>
                    <TD className="font-medium text-[var(--text-primary)]">{item.business_name}</TD>
                    <TD>{item.city}{item.state ? `, ${item.state}` : ""}</TD>
                    <TD>
                      <div className="max-w-[34rem]">
                        <div className="inline-flex rounded-full border border-[var(--border-default)] bg-[var(--surface)] px-3 py-1 text-xs font-medium text-[var(--text-primary)]">
                          {summarizeOpportunity(item.opportunity_profile)}
                        </div>
                        {summarizeConstraint(item.constraint, item.modeled_revenue_upside) ? (
                          <p className="mt-2 text-xs leading-5 text-[var(--text-muted)]">
                            {summarizeConstraint(item.constraint, item.modeled_revenue_upside)}
                          </p>
                        ) : null}
                      </div>
                    </TD>
                    <TD>{fmtDate(item.created_at)}</TD>
                    <TD className="text-right">
                      <Link
                        href={`/diagnostic/${item.id}`}
                        className="mr-3 inline-flex h-9 items-center justify-center rounded-[8px] border border-[var(--border-default)] bg-white px-4 text-sm font-medium text-[var(--text-primary)] transition hover:bg-[var(--surface)]"
                      >
                        Open brief
                      </Link>
                      <button onClick={() => void onDelete(item.id)} className="text-rose-600 hover:underline">Delete</button>
                    </TD>
                  </TR>
                ))}
              </tbody>
            </Table>
            {totalPages > 1 && (
              <CardBody className="flex items-center justify-between border-t border-white/10">
                <p className="text-sm text-[var(--text-muted)]">
                  Showing {page * PAGE_SIZE + 1}-{Math.min((page + 1) * PAGE_SIZE, filtered.length)} of {filtered.length}
                </p>
                <div className="flex gap-2">
                  <Button disabled={page === 0} onClick={() => setPage((p) => Math.max(0, p - 1))}>Previous</Button>
                  <Button disabled={page >= totalPages - 1} onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}>Next</Button>
                </div>
              </CardBody>
            )}
          </>
        )}
      </Card>
    </div>
  );
}
