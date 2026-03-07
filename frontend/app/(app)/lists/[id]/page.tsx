"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useEffect, useMemo, useState } from "react";
import { getListMembers, getTerritoryScanResults, markProspectOutcome, removeListMember, rescanList } from "@/lib/api";
import type { ProspectListMembersResponse, ProspectRow } from "@/lib/types";
import Button from "@/app/components/ui/Button";
import { Card, CardBody, CardHeader } from "@/app/components/ui/Card";
import Badge from "@/app/components/ui/Badge";
import { Table, THead, TH, TR, TD } from "@/app/components/ui/Table";
import { Skeleton } from "@/app/components/ui/Skeleton";

export default function ListDetailPage() {
  const params = useParams();
  const listId = Number(params.id);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<ProspectListMembersResponse | null>(null);
  const [changeSummary, setChangeSummary] = useState<string | null>(null);
  const [rescanning, setRescanning] = useState(false);

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

  const outcomeLabel = (status?: string) => {
    if (!status) return "Not contacted";
    if (status === "contacted") return "Contacted";
    if (status === "closed_won") return "Won";
    if (status === "closed_lost") return "Lost";
    return "Not contacted";
  };

  async function handleRemove(diagnosticId: number) {
    if (!confirm("Remove this member from list?")) return;
    await removeListMember(listId, diagnosticId);
    await load();
  }

  async function handleOutcome(row: ProspectRow, status: "contacted" | "closed_won" | "closed_lost") {
    await markProspectOutcome({ diagnostic_id: row.diagnostic_id, status });
    await load();
  }

  async function handleRescan() {
    setRescanning(true);
    setChangeSummary(null);
    try {
      const { scan_id } = await rescanList(listId);
      for (let i = 0; i < 240; i++) {
        const scan = await getTerritoryScanResults(scan_id);
        if (scan.status === "completed") {
          setChangeSummary(`${Number(scan.summary?.changed || 0)} changed, ${Number(scan.summary?.accepted || 0)} rescanned`);
          await load();
          setRescanning(false);
          return;
        }
        if (scan.status === "failed") throw new Error(scan.error || "Re-scan failed");
        await new Promise((r) => setTimeout(r, 2000));
      }
      throw new Error("Re-scan timed out");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Re-scan failed");
      setRescanning(false);
    }
  }

  if (loading) return <div className="mx-auto max-w-6xl"><Skeleton className="h-64" /></div>;
  if (error) return <div className="mx-auto max-w-6xl text-sm text-rose-600">{error}</div>;
  if (!data) return null;

  return (
    <div className="mx-auto max-w-7xl">
      <div className="mb-5 flex flex-wrap items-end justify-between gap-3">
        <div>
          <p className="text-xs text-[var(--text-muted)]"><Link href="/lists" className="app-link">&larr; Lists</Link></p>
          <h1 className="text-2xl font-semibold tracking-tight">{data.list.name}</h1>
          <p className="text-sm text-[var(--text-muted)]">{rows.length} members</p>
        </div>
        <div className="flex flex-wrap gap-2">
          <Link href="/ask"><Button>Ask Neyma</Button></Link>
          <Link href="/territory/new"><Button>Run territory scan</Button></Link>
          <Button variant="primary" onClick={() => void handleRescan()} disabled={rescanning}>
            {rescanning ? "Refreshing..." : "Refresh list signals"}
          </Button>
        </div>
      </div>

      {changeSummary && <Card className="mb-4 border-sky-200 bg-sky-50"><CardBody className="text-sm text-sky-800">Refresh summary: {changeSummary}</CardBody></Card>}

      <Card>
        <CardHeader title="List Pipeline" />
        <Table>
          <THead><tr><TH>Business</TH><TH>City</TH><TH>Revenue Band</TH><TH>Leverage / Constraint</TH><TH>Status</TH><TH>Updated</TH><TH className="text-right">Actions</TH></tr></THead>
          <tbody>
            {rows.map((row) => (
              <TR key={row.diagnostic_id}>
                <TD className="font-medium text-[var(--text-primary)]">{row.business_name}</TD>
                <TD>{row.city}{row.state ? `, ${row.state}` : ""}</TD>
                <TD>{row.revenue_band || "-"}</TD>
                <TD>
                  <div>{row.primary_leverage || "-"}</div>
                  <div className="text-xs text-[var(--text-muted)]">{row.constraint || "-"}</div>
                </TD>
                <TD><Badge tone={row.outcome_status?.status === "closed_won" ? "success" : row.outcome_status?.status === "closed_lost" ? "danger" : "muted"}>{outcomeLabel(row.outcome_status?.status)}</Badge></TD>
                <TD>{row.added_at ? new Date(row.added_at).toLocaleDateString("en-US") : "-"}</TD>
                <TD className="text-right">
                  <Link
                    href={`/diagnostic/${row.diagnostic_id}?from=list&listId=${listId}`}
                    className="mr-2 inline-flex h-9 items-center justify-center rounded-full bg-black px-4 text-sm font-medium text-white transition hover:bg-[#4f79c7]"
                  >
                    Open brief
                  </Link>
                  <button onClick={() => void handleOutcome(row, "contacted")} className="mr-2 text-[var(--text-secondary)] hover:underline">Contacted</button>
                  <button onClick={() => void handleOutcome(row, "closed_won")} className="mr-2 text-emerald-700 hover:underline">Won</button>
                  <button onClick={() => void handleOutcome(row, "closed_lost")} className="mr-2 text-rose-700 hover:underline">Lost</button>
                  <button onClick={() => void handleRemove(row.diagnostic_id)} className="text-[var(--text-secondary)] hover:underline">Remove</button>
                </TD>
              </TR>
            ))}
          </tbody>
        </Table>
      </Card>
    </div>
  );
}
