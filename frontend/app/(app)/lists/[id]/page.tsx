"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useEffect, useMemo, useState } from "react";
import { getListMembers, removeListMember } from "@/lib/api";
import type { ProspectListMembersResponse, ProspectRow } from "@/lib/types";
import Button from "@/app/components/ui/Button";
import { Card, CardHeader } from "@/app/components/ui/Card";
import Badge from "@/app/components/ui/Badge";
import { Table, THead, TH, TR, TD } from "@/app/components/ui/Table";
import { Skeleton } from "@/app/components/ui/Skeleton";

function briefState(row: ProspectRow) {
  if (row.full_brief_ready || row.diagnostic_id) {
    return { label: "Brief ready", tone: "success" as const };
  }
  return { label: "Needs brief", tone: "muted" as const };
}

export default function ListDetailPage() {
  const params = useParams();
  const listId = Number(params.id);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<ProspectListMembersResponse | null>(null);

  async function load() {
    setLoading(true);
    setError(null);
    try {
      setData(await getListMembers(listId));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load list");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    if (!Number.isNaN(listId)) void load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [listId]);

  const rows = useMemo(() => data?.members || [], [data?.members]);
  const readyCount = useMemo(
    () => rows.filter((row) => row.full_brief_ready || row.diagnostic_id).length,
    [rows],
  );
  const marketsCovered = useMemo(
    () => new Set(rows.map((row) => `${row.city || ""}-${row.state || ""}`)).size,
    [rows],
  );
  const signalSummary = (row: ProspectRow) => row.review_position_summary || row.primary_leverage || row.constraint || "No signal captured yet";

  async function handleRemove(diagnosticId: number) {
    if (!confirm("Remove this lead from list?")) return;
    await removeListMember(listId, diagnosticId);
    await load();
  }

  if (loading) return <div className="mx-auto max-w-7xl"><Skeleton className="h-72 rounded-[32px]" /></div>;
  if (error) return <div className="mx-auto max-w-7xl text-sm text-rose-600">{error}</div>;
  if (!data) return null;

  return (
    <div className="mx-auto max-w-7xl space-y-5">
      <section className="relative overflow-hidden rounded-[32px] border border-[var(--border-default)] bg-[linear-gradient(135deg,rgba(255,255,255,0.95),rgba(245,238,252,0.88))] p-6 shadow-[0_24px_60px_rgba(10,10,10,0.05)]">
        <div
          aria-hidden
          className="pointer-events-none absolute right-[-4%] top-[-20%] h-48 w-48 rounded-full bg-[radial-gradient(circle,rgba(139,80,212,0.16),rgba(255,255,255,0))]"
        />
        <div className="relative flex flex-wrap items-end justify-between gap-4">
          <div>
            <p className="text-xs text-[var(--text-muted)]">
              <Link href="/lists" className="app-link">&larr; Lists</Link>
            </p>
            <h1 className="mt-3 text-[clamp(2.25rem,5vw,3.3rem)] font-medium tracking-[-0.06em] text-[var(--text-primary)]">
              {data.list.name}
            </h1>
            <p className="mt-3 max-w-[54ch] text-sm leading-7 text-[var(--text-secondary)]">
              Keep saved leads together, revisit the strongest accounts, and open briefs only where the opportunity still feels real.
            </p>
          </div>

          <div className="flex w-full flex-col gap-2 sm:w-auto sm:flex-row sm:flex-wrap">
            <Link href="/ask" className="w-full sm:w-auto"><Button className="w-full sm:w-auto">Ask Neyma</Button></Link>
            <Link href="/territory/new" className="w-full sm:w-auto"><Button className="w-full sm:w-auto">Run territory scan</Button></Link>
          </div>
        </div>

        <div className="relative mt-6 grid gap-3 md:grid-cols-3 md:gap-4">
          <div className="rounded-[20px] border border-white/70 bg-white/88 p-3.5 sm:p-4">
            <p className="section-kicker">Saved leads</p>
            <p className="mt-2 text-[2rem] font-semibold tracking-[-0.05em] text-[var(--text-primary)] sm:mt-3 sm:text-4xl">{rows.length}</p>
            <p className="mt-1.5 text-[12px] leading-5 text-[var(--text-secondary)] sm:mt-2 sm:text-sm">Leads saved in this list.</p>
          </div>
          <div className="rounded-[20px] border border-white/70 bg-white/88 p-3.5 sm:p-4">
            <p className="section-kicker">Briefs available</p>
            <p className="mt-2 text-[2rem] font-semibold tracking-[-0.05em] text-[var(--text-primary)] sm:mt-3 sm:text-4xl">{readyCount}</p>
            <p className="mt-1.5 text-[12px] leading-5 text-[var(--text-secondary)] sm:mt-2 sm:text-sm">Ready to open now.</p>
          </div>
          <div className="rounded-[20px] border border-white/70 bg-white/88 p-3.5 sm:p-4">
            <p className="section-kicker">Markets</p>
            <p className="mt-2 text-[2rem] font-semibold tracking-[-0.05em] text-[var(--text-primary)] sm:mt-3 sm:text-4xl">{marketsCovered}</p>
            <p className="mt-1.5 text-[12px] leading-5 text-[var(--text-secondary)] sm:mt-2 sm:text-sm">Markets represented.</p>
          </div>
        </div>
      </section>

      <Card className="overflow-hidden border border-[var(--border-default)] bg-[var(--bg-card)] shadow-[0_18px_40px_rgba(10,10,10,0.04)]">
        <CardHeader title="Saved Leads" subtitle="Focused list view for reopening saved leads, checking the current signal, and jumping into the next brief." />
        <div className="space-y-3 p-4 md:hidden">
          {rows.map((row) => {
            const state = briefState(row);
            return (
              <div key={`${row.diagnostic_id ?? "member"}-${row.business_name}`} className="rounded-[16px] border border-[var(--border-default)] bg-white p-4">
                <p className="text-[14px] font-medium text-[var(--text-primary)]">{row.business_name}</p>
                <p className="mt-1 text-[12px] text-[var(--text-muted)]">
                  {row.city}
                  {row.state ? `, ${row.state}` : ""}
                </p>
                <p className="mt-3 text-[12px] text-[var(--text-secondary)]">{signalSummary(row)}</p>
                {row.constraint && row.constraint !== row.review_position_summary && row.constraint !== row.primary_leverage ? (
                  <p className="mt-1 text-[12px] text-[var(--text-muted)]">{row.constraint}</p>
                ) : null}
                <div className="mt-3 flex flex-wrap items-center gap-2">
                  <Badge tone={state.tone}>{state.label}</Badge>
                  <span className="text-[12px] text-[var(--text-muted)]">
                    Saved {row.added_at ? new Date(row.added_at).toLocaleDateString("en-US") : "-"}
                  </span>
                </div>
                <div className="mt-4 flex flex-col gap-2">
                  {row.diagnostic_id ? (
                    (() => {
                      const diagnosticId = row.diagnostic_id;
                      return (
                    <>
                      <Link
                        href={`/diagnostic/${diagnosticId}?from=list&listId=${listId}`}
                        className="inline-flex h-10 items-center justify-center rounded-full bg-[var(--primary)] px-4 text-sm font-medium text-[var(--primary-foreground)] transition hover:opacity-95"
                      >
                        Open brief
                      </Link>
                      <button onClick={() => void handleRemove(diagnosticId)} className="text-sm text-[var(--text-secondary)] hover:underline">
                        Remove
                      </button>
                    </>
                      );
                    })()
                  ) : (
                    <span className="text-xs text-[var(--text-muted)]">Brief unavailable</span>
                  )}
                </div>
              </div>
            );
          })}
        </div>
        <div className="hidden md:block">
          <Table className="rounded-none border-0 bg-transparent p-0 shadow-none">
            <THead>
              <tr>
                <TH>Lead</TH>
                <TH>Market</TH>
                <TH>Current Signal</TH>
                <TH>Brief</TH>
                <TH>Saved</TH>
                <TH className="text-right">Actions</TH>
              </tr>
            </THead>
            <tbody>
              {rows.map((row) => {
                const state = briefState(row);
                return (
                  <TR key={`${row.diagnostic_id ?? "member"}-${row.business_name}`}>
                    <TD className="font-medium text-[var(--text-primary)]">{row.business_name}</TD>
                    <TD>
                      {row.city}
                      {row.state ? `, ${row.state}` : ""}
                    </TD>
                    <TD>
                      <div>{signalSummary(row)}</div>
                      {row.constraint && row.constraint !== row.review_position_summary && row.constraint !== row.primary_leverage ? (
                        <div className="text-xs text-[var(--text-muted)]">{row.constraint}</div>
                      ) : null}
                    </TD>
                    <TD><Badge tone={state.tone}>{state.label}</Badge></TD>
                    <TD>{row.added_at ? new Date(row.added_at).toLocaleDateString("en-US") : "-"}</TD>
                    <TD className="text-right">
                      {row.diagnostic_id ? (
                        (() => {
                          const diagnosticId = row.diagnostic_id;
                          return (
                        <>
                          <Link
                            href={`/diagnostic/${diagnosticId}?from=list&listId=${listId}`}
                            className="mr-2 inline-flex h-9 items-center justify-center rounded-full bg-[var(--primary)] px-4 text-sm font-medium text-[var(--primary-foreground)] transition hover:opacity-95"
                          >
                            Open brief
                          </Link>
                          <button onClick={() => void handleRemove(diagnosticId)} className="text-[var(--text-secondary)] hover:underline">
                            Remove
                          </button>
                        </>
                          );
                        })()
                      ) : (
                        <span className="text-xs text-[var(--text-muted)]">Brief unavailable</span>
                      )}
                    </TD>
                  </TR>
                );
              })}
            </tbody>
          </Table>
        </div>
      </Card>
    </div>
  );
}
