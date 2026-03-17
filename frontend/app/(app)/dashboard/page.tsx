"use client";

import { useEffect, useMemo, useState, type ReactNode } from "react";
import Link from "next/link";
import {
  ArrowRight,
  FileStack,
  LayoutGrid,
  Map as MapIcon,
  MessageSquareText,
  Search,
} from "lucide-react";
import { deleteDiagnostic, getBaseUrl, getRecentTerritoryScans, listDiagnostics } from "@/lib/api";
import type { DiagnosticListItem, TerritoryScanListItem } from "@/lib/types";
import Button from "@/app/components/ui/Button";
import Input from "@/app/components/ui/Input";
import { Card, CardBody, CardHeader } from "@/app/components/ui/Card";
import EmptyState from "@/app/components/ui/EmptyState";
import { Skeleton } from "@/app/components/ui/Skeleton";

const PAGE_SIZE = 10;
const WORKSPACE_CACHE_KEY = "neyma_workspace_snapshot_v1";

function fmtDate(iso: string) {
  return new Date(iso).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

function fmtRelativeDate(iso?: string | null) {
  if (!iso) return "No recent activity";
  const eventDate = new Date(iso).getTime();
  const diffDays = Math.max(0, Math.floor((Date.now() - eventDate) / 86400000));
  if (diffDays === 0) return "Updated today";
  if (diffDays === 1) return "Updated yesterday";
  return `Updated ${diffDays} days ago`;
}

function summarizeOpportunity(value?: string | null) {
  if (!value) return "No signal summary";
  const cleaned = value.replace(/\s+/g, " ").trim();
  return cleaned.split("(")[0].replace(/[:,-]\s*$/, "").trim() || cleaned;
}

function MiniStat({
  label,
  value,
  detail,
  mobileDetail,
}: {
  label: string;
  value: string;
  detail: string;
  mobileDetail?: string;
}) {
  return (
    <div className="rounded-[24px] border border-[var(--border-default)] bg-white/85 p-4 shadow-[0_18px_40px_rgba(10,10,10,0.04)] backdrop-blur sm:p-5">
      <p className="section-kicker">{label}</p>
      <p className="mt-2 text-[2.25rem] font-semibold leading-none tracking-[-0.06em] text-[var(--text-primary)] sm:mt-3 sm:text-[2.7rem]">
        {value}
      </p>
      <p className="mt-2 text-[13px] leading-5 text-[var(--text-secondary)] sm:hidden">
        {mobileDetail || detail}
      </p>
      <p className="mt-3 hidden text-sm leading-6 text-[var(--text-secondary)] sm:block">{detail}</p>
    </div>
  );
}

function WorkflowCard({
  href,
  title,
  description,
  mobileDescription,
  accent,
  icon,
}: {
  href: string;
  title: string;
  description: string;
  mobileDescription?: string;
  accent: string;
  icon: ReactNode;
}) {
  return (
    <Link
      href={href}
      className="group rounded-[28px] border border-[var(--border-default)] bg-white/88 p-4 shadow-[0_20px_40px_rgba(10,10,10,0.04)] transition hover:-translate-y-0.5 hover:shadow-[0_24px_60px_rgba(10,10,10,0.08)] sm:p-5"
    >
      <div
        className="inline-flex h-11 w-11 items-center justify-center rounded-full border sm:h-12 sm:w-12"
        style={{ backgroundColor: accent, borderColor: accent }}
      >
        {icon}
      </div>
      <h3 className="mt-4 text-[1.15rem] font-medium tracking-[-0.04em] text-[var(--text-primary)] sm:mt-5 sm:text-[1.35rem]">{title}</h3>
      <p className="mt-2 text-[13px] leading-5 text-[var(--text-secondary)] sm:hidden">
        {mobileDescription || description}
      </p>
      <p className="mt-3 hidden text-sm leading-6 text-[var(--text-secondary)] sm:block">{description}</p>
      <span className="mt-4 inline-flex items-center gap-2 text-sm font-medium text-[var(--text-primary)] sm:mt-5">
        Open
        <ArrowRight className="h-4 w-4 transition group-hover:translate-x-0.5" />
      </span>
    </Link>
  );
}

function SignalBadge({ label, tone = "muted" }: { label: string; tone?: "success" | "muted" | "purple" }) {
  const styles = {
    success: "border border-emerald-500/20 bg-emerald-500/12 text-emerald-700",
    muted: "border border-[var(--border-default)] bg-[var(--surface)] text-[var(--text-secondary)]",
    purple: "border border-[#eadcf8] bg-[#f5eefc] text-[var(--primary)]",
  };

  return (
    <span className={`inline-flex rounded-full px-2.5 py-1 text-xs font-medium ${styles[tone]}`}>
      {label}
    </span>
  );
}

type ArchiveFilter = "all" | "high" | "low";
type ArchiveSortKey = "business" | "city" | "rating" | "date";
type SortDirection = "asc" | "desc";

function archiveLeverage(item: DiagnosticListItem): "high" | "low" | null {
  const source = `${item.opportunity_profile || ""} ${item.constraint || ""} ${item.modeled_revenue_upside || ""}`.toLowerCase();
  if (source.includes("high-leverage") || source.includes("high leverage")) return "high";
  if (source.includes("low-leverage") || source.includes("low leverage")) return "low";
  return null;
}

function leverageBadge(leverage: Exclude<ArchiveFilter, "all">) {
  if (leverage === "high") {
    return "inline-flex whitespace-nowrap rounded-full border border-[#eadcf8] bg-[#f5eefc] px-2 py-[3px] text-[11px] font-medium text-[var(--primary)]";
  }
  return "inline-flex whitespace-nowrap rounded-full bg-[#F1F3F5] px-2 py-[3px] text-[11px] font-medium text-[#4B5563]";
}

function SortButton({
  label,
  sortKey,
  activeKey,
  direction,
  onToggle,
  muted = false,
}: {
  label: string;
  sortKey: ArchiveSortKey;
  activeKey: ArchiveSortKey | null;
  direction: SortDirection;
  onToggle: (key: ArchiveSortKey) => void;
  muted?: boolean;
}) {
  const active = activeKey === sortKey;
  return (
    <button
      type="button"
      onClick={() => onToggle(sortKey)}
      className={`${muted ? "text-[11px] text-[var(--text-muted)]" : "text-[11px] text-[var(--text-secondary)]"} inline-flex items-center gap-1 uppercase tracking-[0.14em] transition hover:text-[var(--text-primary)]`}
    >
      <span>{label}</span>
      {active ? <span>{direction === "asc" ? "↑" : "↓"}</span> : null}
    </button>
  );
}

function formatRating(value?: number | null) {
  if (value == null || Number.isNaN(value)) return "—";
  return value.toFixed(1);
}

function ratingDeltaTone(value?: number | null, localAverage?: number | null) {
  if (value == null || localAverage == null) return "text-[var(--text-muted)]";
  if (value >= localAverage) return "text-emerald-700";
  return "text-amber-700";
}

function ratingComparisonLabel(value?: number | null, localAverage?: number | null) {
  if (value == null || localAverage == null) return "No rating snapshot";
  if (value > localAverage) return "Above market";
  if (value < localAverage) return "Below market";
  return "At market";
}

export default function DashboardPage() {
  const [items, setItems] = useState<DiagnosticListItem[]>([]);
  const [scans, setScans] = useState<TerritoryScanListItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [archiveFilter, setArchiveFilter] = useState<ArchiveFilter>("all");
  const [archiveSortKey, setArchiveSortKey] = useState<ArchiveSortKey | null>(null);
  const [archiveSortDirection, setArchiveSortDirection] = useState<SortDirection>("asc");
  const [page, setPage] = useState(0);

  useEffect(() => {
    if (typeof window === "undefined") return;
    try {
      const cached = window.sessionStorage.getItem(WORKSPACE_CACHE_KEY);
      if (!cached) return;
      const parsed = JSON.parse(cached) as {
        items?: DiagnosticListItem[];
        scans?: TerritoryScanListItem[];
      };
      if (Array.isArray(parsed.items)) setItems(parsed.items);
      if (Array.isArray(parsed.scans)) setScans(parsed.scans);
      if (Array.isArray(parsed.items) || Array.isArray(parsed.scans)) {
        setLoading(false);
      }
    } catch {
      // ignore malformed cache
    }
  }, []);

  useEffect(() => {
    async function load() {
      setError(null);
      try {
        const [diagnostics, recentScans] = await Promise.all([
          listDiagnostics(80, 0),
          getRecentTerritoryScans(10),
        ]);
        setItems(diagnostics.items);
        setScans(recentScans.items);
        if (typeof window !== "undefined") {
          window.sessionStorage.setItem(
            WORKSPACE_CACHE_KEY,
            JSON.stringify({
              items: diagnostics.items,
              scans: recentScans.items,
            }),
          );
        }
      } catch (err) {
        if (items.length === 0 && scans.length === 0) {
          setItems([]);
          setScans([]);
        }
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
  }, [items.length, scans.length]);

  useEffect(() => {
    setPage(0);
  }, [search, archiveFilter, archiveSortDirection, archiveSortKey]);

  const filtered = useMemo(() => {
    const query = search.trim().toLowerCase();
    return items.filter((item) => {
      const leverage = archiveLeverage(item);
      if (archiveFilter !== "all" && leverage !== archiveFilter) return false;
      if (!query) return true;
      return `${item.business_name} ${item.city} ${item.state || ""} ${item.opportunity_profile || ""} ${item.constraint || ""}`
        .toLowerCase()
        .includes(query);
    });
  }, [archiveFilter, items, search]);

  const sortedArchive = useMemo(() => {
    if (!archiveSortKey) return filtered;
    const rows = [...filtered];
    rows.sort((left, right) => {
      const comparison =
        archiveSortKey === "business"
          ? left.business_name.localeCompare(right.business_name)
          : archiveSortKey === "city"
            ? `${left.city || ""}${left.state ? `, ${left.state}` : ""}`.localeCompare(`${right.city || ""}${right.state ? `, ${right.state}` : ""}`)
            : archiveSortKey === "rating"
              ? (left.rating ?? -1) - (right.rating ?? -1)
              : new Date(left.created_at).getTime() - new Date(right.created_at).getTime();
      return archiveSortDirection === "asc" ? comparison : -comparison;
    });
    return rows;
  }, [archiveSortDirection, archiveSortKey, filtered]);

  const totalPages = Math.max(1, Math.ceil(sortedArchive.length / PAGE_SIZE));
  const pageItems = sortedArchive.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);
  const cities = useMemo(() => new Set(items.map((item) => item.city)).size, [items]);
  const briefsThisMonth = useMemo(() => {
    const now = new Date();
    return items.filter((item) => {
      const createdAt = new Date(item.created_at);
      return createdAt.getMonth() === now.getMonth() && createdAt.getFullYear() === now.getFullYear();
    }).length;
  }, [items]);

  const featuredBrief = items[0] || null;
  const featuredScan = scans[0] || null;
  const recommendedAction = useMemo(() => {
    if (!featuredScan) {
      return {
        title: "Start with the next market.",
        body: "Run a territory scan first so the rest of the workspace has a ranked shortlist to build from.",
        href: "/territory/new",
        cta: "Run territory scan",
      };
    }
    if (!featuredBrief) {
      return {
        title: "Use Ask Neyma to narrow the market.",
        body: "You already have scan coverage. Now tighten the shortlist before opening more briefs.",
        href: "/ask",
        cta: "Open Ask Neyma",
      };
    }
    return {
      title: `Open ${featuredBrief.business_name}.`,
      body: "The workspace already has active work. Continue from the strongest current brief instead of starting over.",
      href: `/diagnostic/${featuredBrief.id}`,
      cta: "Open latest brief",
    };
  }, [featuredBrief, featuredScan]);

  async function onDelete(id: number) {
    if (!confirm("Delete this brief?")) return;
    await deleteDiagnostic(id);
    setItems((previous) => previous.filter((item) => item.id !== id));
  }

  function toggleArchiveSort(nextKey: ArchiveSortKey) {
    if (archiveSortKey === nextKey) {
      setArchiveSortDirection((current) => (current === "asc" ? "desc" : "asc"));
      return;
    }
    setArchiveSortKey(nextKey);
    setArchiveSortDirection("asc");
  }

  if (loading) {
    return (
      <div className="mx-auto max-w-[var(--max-content)] space-y-5">
        <Skeleton className="h-64 rounded-[32px]" />
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          {[0, 1, 2, 3].map((index) => (
            <Skeleton key={index} className="h-36 rounded-[24px]" />
          ))}
        </div>
        <Skeleton className="h-80 rounded-[28px]" />
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

      <section className="relative overflow-hidden rounded-[34px] border border-[var(--border-default)] bg-[linear-gradient(135deg,rgba(255,255,255,0.94),rgba(245,238,252,0.92))] p-6 shadow-[0_28px_70px_rgba(10,10,10,0.06)] sm:p-7">
        <div
          aria-hidden
          className="pointer-events-none absolute right-[-6%] top-[-15%] h-56 w-56 rounded-full bg-[radial-gradient(circle,rgba(139,80,212,0.18),rgba(255,255,255,0))]"
        />
        <div className="relative grid gap-5 xl:grid-cols-[1.15fr_0.85fr]">
          <div>
            <p className="section-kicker">Workspace</p>
            <h2 className="mt-3 max-w-[11ch] text-[clamp(2.8rem,6vw,4.6rem)] font-medium leading-[0.98] tracking-[-0.07em] text-[var(--text-primary)]">
              Work the next best move.
            </h2>
            <p className="mt-4 max-w-[52ch] text-[17px] leading-8 text-[var(--text-secondary)]">
              The landing page already tells the story. The workspace should carry the same feeling forward:
              clean territory-first workflow, obvious next actions, and less operational clutter.
            </p>
            <div className="mt-7 flex flex-col gap-3 sm:flex-row sm:flex-wrap">
              <Link href="/territory/new" className="w-full sm:w-auto">
                <Button variant="primary" className="h-11 w-full justify-center rounded-full px-5 sm:w-auto">Run territory scan</Button>
              </Link>
              <Link href="/ask" className="w-full sm:w-auto">
                <Button variant="secondary" className="h-11 w-full justify-center rounded-full px-5 sm:w-auto">Ask Neyma</Button>
              </Link>
              <Link href="/diagnostic/new" className="w-full sm:w-auto">
                <Button variant="secondary" className="h-11 w-full justify-center rounded-full px-5 sm:w-auto">Build brief</Button>
              </Link>
            </div>
          </div>

          <div className="rounded-[30px] border border-[#eadcf8] bg-[linear-gradient(180deg,rgba(245,238,252,0.98),rgba(255,255,255,0.95))] p-5 text-[var(--text-primary)] shadow-[0_22px_60px_rgba(139,80,212,0.12)]">
            <p className="text-[10px] font-medium uppercase tracking-[0.22em] text-[var(--text-muted)]">Recommended next step</p>
            <h3 className="mt-3 text-[1.9rem] font-medium leading-[1.05] tracking-[-0.05em] text-[var(--text-primary)]">
              {recommendedAction.title}
            </h3>
            <p className="mt-4 text-sm leading-7 text-[var(--text-secondary)]">{recommendedAction.body}</p>
            <Link
              href={recommendedAction.href}
              className="mt-6 inline-flex h-11 w-full items-center justify-center gap-2 rounded-full bg-[var(--primary)] px-5 text-sm font-medium text-[var(--primary-foreground)] transition hover:brightness-95 sm:w-auto"
            >
              {recommendedAction.cta}
              <ArrowRight className="h-4 w-4" />
            </Link>

            <div className="mt-6 grid gap-3 sm:grid-cols-2">
              <div className="rounded-[22px] border border-[#eadcf8] bg-white/88 p-4">
                <p className="text-[10px] font-medium uppercase tracking-[0.2em] text-[var(--text-muted)]">Latest scan</p>
                <p className="mt-2 text-base font-medium text-[var(--text-primary)]">
                  {featuredScan ? `${featuredScan.city || "Market"}${featuredScan.state ? `, ${featuredScan.state}` : ""}` : "No scans yet"}
                </p>
                <p className="mt-1 text-sm text-[var(--text-secondary)]">
                  {featuredScan ? fmtRelativeDate(featuredScan.completed_at || featuredScan.created_at) : "Start by naming a market."}
                </p>
              </div>
              <div className="rounded-[22px] border border-[#eadcf8] bg-white/88 p-4">
                <p className="text-[10px] font-medium uppercase tracking-[0.2em] text-[var(--text-muted)]">Latest brief</p>
                <p className="mt-2 text-base font-medium text-[var(--text-primary)]">
                  {featuredBrief ? featuredBrief.business_name : "No briefs yet"}
                </p>
                <p className="mt-1 text-sm text-[var(--text-secondary)]">
                  {featuredBrief ? fmtRelativeDate(featuredBrief.created_at) : "Open a brief when the shortlist is ready."}
                </p>
              </div>
            </div>
          </div>
        </div>
      </section>

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MiniStat
          label="Total briefs"
          value={String(items.length)}
          detail="Full account views currently saved in the workspace."
          mobileDetail="Saved briefs in workspace."
        />
        <MiniStat
          label="Cities covered"
          value={String(cities)}
          detail="Markets represented across the briefs you have built so far."
          mobileDetail="Markets in archive."
        />
        <MiniStat
          label="Recent scans"
          value={String(scans.length)}
          detail="Territory scans available to reopen, continue, or export."
          mobileDetail="Scans ready to reopen."
        />
        <MiniStat
          label="Briefs this month"
          value={String(briefsThisMonth)}
          detail="New briefs added during the current calendar month."
          mobileDetail="Added this month."
        />
      </section>

      <section className="grid gap-4 lg:grid-cols-3">
        <WorkflowCard
          href="/territory/new"
          title="Run Territory Scan"
          description="Start with a city and let Neyma return the ranked market before you touch deeper research."
          mobileDescription="Scan a city and get the ranked market."
          accent="rgba(245,238,252,1)"
          icon={<MapIcon className="h-5 w-5 text-[var(--primary)]" />}
        />
        <WorkflowCard
          href="/ask"
          title="Refine In Ask Neyma"
          description="Describe the kind of practice or gap you want and turn plain language into a tighter shortlist."
          mobileDescription="Use plain language to narrow the shortlist."
          accent="rgba(255,243,221,1)"
          icon={<MessageSquareText className="h-5 w-5 text-[#b7791f]" />}
        />
        <WorkflowCard
          href="/diagnostic/new"
          title="Build One Brief"
          description="Already know the account? Skip the market step and generate the brief directly."
          mobileDescription="Open a brief directly for one account."
          accent="rgba(228,244,238,1)"
          icon={<FileStack className="h-5 w-5 text-[#1f7a52]" />}
        />
      </section>

      <div className="grid gap-4 xl:grid-cols-[1.05fr_0.95fr]">
        <Card className="overflow-hidden">
          <CardHeader title="Recent Territory Scans" subtitle="Resume active scans or reopen completed markets." />
          {scans.length === 0 ? (
            <CardBody>
              <EmptyState
                title="No territory scans yet"
                description="Run your first market scan to start building ranked output."
                action={<Link href="/territory/new"><Button variant="primary">Run territory scan</Button></Link>}
              />
            </CardBody>
          ) : (
            <CardBody className="space-y-3">
              {scans.slice(0, 5).map((scan) => (
                <div
                  key={scan.id}
                  className="flex flex-col gap-4 rounded-xl border border-[var(--border-default)] bg-white px-5 py-4 transition hover:bg-[var(--surface)] md:flex-row md:items-center md:justify-between"
                >
                  <div className="min-w-0 md:flex-1">
                    <p className="truncate text-sm font-medium text-[var(--text-primary)]">
                      {scan.city || "Market"}
                      {scan.state ? `, ${scan.state}` : ""}
                    </p>
                    <p className="mt-1 text-xs text-[var(--text-muted)]">
                      {scan.vertical || "—"} · {scan.prospects_count ?? Number((scan.summary?.accepted as number) || 0)} prospects
                    </p>
                  </div>

                  <div className="flex min-w-0 flex-col gap-2 md:w-[220px] md:items-start">
                    <div className="flex flex-wrap items-center gap-2">
                      <SignalBadge label={scan.status} tone={scan.status === "completed" ? "success" : "muted"} />
                    </div>
                    <p className="text-xs text-[var(--text-muted)]">{fmtDate(scan.created_at)}</p>
                  </div>

                  <div className="flex justify-start md:justify-end">
                    <Link
                      href={`/territory/${scan.id}`}
                      className="inline-flex h-9 items-center justify-center rounded-md border border-[var(--border-default)] bg-white px-3 py-1.5 text-sm font-medium text-[var(--text-primary)] transition hover:bg-gray-100"
                    >
                      Open Scan
                    </Link>
                  </div>
                </div>
              ))}
            </CardBody>
          )}
        </Card>

        <Card className="overflow-hidden">
          <CardHeader title="Recent Work" subtitle="Jump back into the latest briefs without searching for them." />
          <CardBody className="space-y-3">
            {items.length === 0 ? (
              <EmptyState
                title="No briefs yet"
                description="Build a brief from a known practice or from a territory shortlist."
                action={<Link href="/diagnostic/new"><Button variant="primary">Build brief</Button></Link>}
              />
            ) : (
              items.slice(0, 5).map((item) => (
                <div
                  key={item.id}
                  className="flex flex-col gap-4 rounded-xl border border-[var(--border-default)] bg-white px-5 py-4 transition hover:bg-[var(--surface)] md:flex-row md:items-center md:justify-between"
                >
                  <div className="min-w-0 md:flex-1">
                    <p className="truncate text-sm font-medium text-[var(--text-primary)]">{item.business_name}</p>
                    <p className="mt-1 text-xs text-[var(--text-muted)]">
                      {item.city}
                      {item.state ? `, ${item.state}` : ""}
                    </p>
                  </div>

                  <div className="flex min-w-0 flex-col gap-2 md:w-[260px] md:items-start">
                    <div className="flex flex-wrap items-center gap-2">
                      <SignalBadge
                        label={summarizeOpportunity(item.opportunity_profile)}
                        tone={summarizeOpportunity(item.opportunity_profile).toLowerCase().includes("high-leverage") ? "purple" : "muted"}
                      />
                    </div>
                    <p className="text-xs text-[var(--text-muted)]">
                      {fmtRelativeDate(item.created_at)} · {fmtDate(item.created_at)}
                    </p>
                  </div>

                  <div className="flex justify-start md:justify-end">
                    <Link
                      href={`/diagnostic/${item.id}`}
                      className="inline-flex h-9 items-center justify-center rounded-md border border-[var(--border-default)] bg-white px-3 py-1.5 text-sm font-medium text-[var(--text-primary)] transition hover:bg-gray-100"
                    >
                      Open
                    </Link>
                  </div>
                </div>
              ))
            )}
          </CardBody>
        </Card>
      </div>

      <Card className="overflow-hidden">
        <CardHeader title="Brief Archive" subtitle="Search, review, and reopen saved briefs." />
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
            <CardBody className="border-b border-[var(--border-default)]">
              <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
              <div className="flex flex-1 flex-col gap-3 sm:flex-row sm:items-center">
                  <div className="relative w-full sm:max-w-[320px]">
                    <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-[var(--text-muted)]" />
                    <Input
                      value={search}
                      onChange={(event) => setSearch(event.target.value)}
                      placeholder="Search business, city, signal…"
                      className="pl-10"
                    />
                  </div>
                  <div className="flex flex-wrap gap-2">
                    {[
                      { label: "All", value: "all" as const },
                      { label: "High-leverage", value: "high" as const },
                      { label: "Low-leverage", value: "low" as const },
                    ].map((option) => {
                      const active = archiveFilter === option.value;
                      return (
                        <button
                          key={option.value}
                          type="button"
                          onClick={() => setArchiveFilter(option.value)}
                          className={`rounded-full border px-3 py-1.5 text-sm transition ${
                            active
                              ? "border-[var(--primary)] bg-[var(--primary)] text-white"
                              : "border-[var(--border-default)] bg-white text-[var(--text-secondary)] hover:bg-[var(--surface)]"
                          }`}
                        >
                          {option.label}
                        </button>
                      );
                    })}
                  </div>
                </div>
                <p className="text-sm text-[var(--text-muted)]">{sortedArchive.length} briefs</p>
              </div>
            </CardBody>

            <div className="hidden px-6 pt-4 md:block">
              <div
                className="grid grid-cols-[minmax(0,38%)_minmax(0,22%)_minmax(0,14%)_minmax(220px,26%)] gap-4 border-b border-[rgba(10,10,10,0.08)] pb-3"
                style={{ borderBottomWidth: "0.5px" }}
              >
                <div>
                  <SortButton
                    label="Business"
                    sortKey="business"
                    activeKey={archiveSortKey}
                    direction={archiveSortDirection}
                    onToggle={toggleArchiveSort}
                  />
                </div>
                <div className="flex items-start">
                  <SortButton
                    label="Rating"
                    sortKey="rating"
                    activeKey={archiveSortKey}
                    direction={archiveSortDirection}
                    onToggle={toggleArchiveSort}
                  />
                </div>
                <div className="flex items-start">
                  <SortButton
                    label="Date"
                    sortKey="date"
                    activeKey={archiveSortKey}
                    direction={archiveSortDirection}
                    onToggle={toggleArchiveSort}
                  />
                </div>
                <div className="min-w-[220px] text-right text-[11px] uppercase tracking-[0.14em] text-[var(--text-secondary)]">Actions</div>
              </div>
            </div>

            {sortedArchive.length === 0 ? (
              <CardBody className="py-14">
                <div className="text-center">
                  <p className="text-sm font-medium text-[var(--text-primary)]">No briefs match this filter</p>
                </div>
              </CardBody>
            ) : (
              <div className="px-4 sm:px-6">
                {pageItems.map((item) => {
                  const leverage = archiveLeverage(item);
                  return (
                    <div
                      key={item.id}
                      className="group flex flex-col gap-3 border-b border-[rgba(10,10,10,0.08)] py-4 transition hover:bg-[rgba(10,10,10,0.018)] md:grid md:grid-cols-[minmax(0,38%)_minmax(0,22%)_minmax(0,14%)_minmax(220px,26%)] md:gap-4 md:py-3"
                      style={{ borderBottomWidth: "0.5px" }}
                    >
                      <div className="min-w-0">
                        <p className="text-[10px] font-medium uppercase tracking-[0.14em] text-[var(--text-muted)] md:hidden">Business</p>
                        <p className="truncate text-[14px] font-medium text-[var(--text-primary)]">{item.business_name}</p>
                        <p className="mt-0.5 text-[12px] text-[var(--text-muted)]">
                          {item.city}
                          {item.state ? `, ${item.state}` : ""}
                        </p>
                      </div>

                      <div className="min-w-0">
                        <p className="mb-1 text-[10px] font-medium uppercase tracking-[0.14em] text-[var(--text-muted)] md:hidden">Rating</p>
                        <div className="flex flex-col leading-tight">
                          <span className="font-semibold text-[var(--text-primary)]">
                            {formatRating(item.rating)} ★
                          </span>
                          <span className={`mt-1 text-xs ${ratingDeltaTone(item.rating, item.local_avg_rating)}`}>
                            {ratingComparisonLabel(item.rating, item.local_avg_rating)}
                          </span>
                          <span className="mt-1 text-xs text-[var(--text-muted)]">
                            avg {formatRating(item.local_avg_rating)}
                          </span>
                        </div>
                      </div>

                      <div className="pt-0.5 text-[12px] text-[var(--text-muted)]">
                        <p className="mb-1 text-[10px] font-medium uppercase tracking-[0.14em] text-[var(--text-muted)] md:hidden">Date</p>
                        {fmtDate(item.created_at)}
                      </div>

                      <div className="flex min-w-0 flex-wrap items-center gap-2 whitespace-nowrap md:min-w-[220px] md:justify-end">
                        <p className="mr-auto text-[10px] font-medium uppercase tracking-[0.14em] text-[var(--text-muted)] md:hidden">Actions</p>
                        {leverage ? (
                          <span className={leverageBadge(leverage)}>
                            {leverage === "high" ? "High-Leverage" : "Low-Leverage"}
                          </span>
                        ) : null}
                        <Link
                          href={`/diagnostic/${item.id}`}
                          className="inline-flex h-9 items-center justify-center rounded-md border border-[var(--border-default)] bg-white px-3 py-1.5 text-sm font-medium text-[var(--text-primary)] transition hover:bg-gray-100"
                        >
                          Open brief →
                        </Link>
                        <button
                          type="button"
                          onClick={() => void onDelete(item.id)}
                          className="inline-flex h-9 w-[52px] items-center justify-center rounded-md border border-transparent px-2 text-sm text-[var(--text-muted)] opacity-0 transition hover:bg-white hover:text-rose-600 group-hover:opacity-100"
                          aria-label={`Delete ${item.business_name}`}
                        >
                          Delete
                        </button>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}

            {totalPages > 1 ? (
              <CardBody className="flex flex-col gap-3 border-t border-[var(--border-default)] sm:flex-row sm:items-center sm:justify-between">
                <p className="text-sm text-[var(--text-muted)]">
                  Showing {page * PAGE_SIZE + 1}-{Math.min((page + 1) * PAGE_SIZE, sortedArchive.length)} of {sortedArchive.length}
                </p>
                <div className="flex w-full gap-2 sm:w-auto">
                  <Button className="flex-1 sm:flex-none" disabled={page === 0} onClick={() => setPage((value) => Math.max(0, value - 1))}>Previous</Button>
                  <Button className="flex-1 sm:flex-none" disabled={page >= totalPages - 1} onClick={() => setPage((value) => Math.min(totalPages - 1, value + 1))}>Next</Button>
                </div>
              </CardBody>
            ) : null}
          </>
        )}
      </Card>

      <div className="hidden rounded-[28px] border border-[var(--border-default)] bg-white/70 p-5 text-sm text-[var(--text-secondary)] xl:flex xl:items-center xl:justify-between">
        <div className="flex items-center gap-3">
          <span className="inline-flex h-11 w-11 items-center justify-center rounded-full bg-[#f5eefc] text-[var(--primary)]">
            <LayoutGrid className="h-5 w-5" />
          </span>
          <div>
            <p className="font-medium text-[var(--text-primary)]">Workspace focus</p>
            <p className="mt-1">Less CRM tracking, more ranked workflow and clearer next actions.</p>
          </div>
        </div>
        <Link href="/lists" className="inline-flex items-center gap-2 font-medium text-[var(--text-primary)]">
          Review saved lists
          <ArrowRight className="h-4 w-4" />
        </Link>
      </div>
    </div>
  );
}
