"use client";

import { canonicalStatus, statusBadge, evidenceLabel, scanQualityLabel } from "@/lib/service_ui";
import type { CanonicalServiceStatus } from "@/lib/service_ui";

type ServiceRow = Record<string, unknown>;

type Props = {
  services: ServiceRow[];
  crawlConfidence: string;
  suppressServiceGap: boolean;
  pagesCrawled?: number;
};

export default function ServiceChecklist({
  services,
  crawlConfidence,
  suppressServiceGap,
  pagesCrawled,
}: Props) {
  const lowCrawl = ["low", "unknown", ""].includes(String(crawlConfidence || "").toLowerCase());
  const scanQuality = scanQualityLabel(crawlConfidence, pagesCrawled);

  const resolved = services.map((svc, idx) => ({
    name: String(svc.display_name || svc.service || `Service ${idx + 1}`),
    status: resolveStatus(svc, lowCrawl, suppressServiceGap),
    url: String(svc.url || "").trim(),
    raw: svc,
  }));

  const found = resolved.filter((s) => s.status === "dedicated" || s.status === "mention_only");
  const notFound = resolved.filter((s) => s.status === "missing");
  const notScanned = resolved.filter((s) => s.status === "unknown");

  return (
    <div className="space-y-3">
      {/* Scan quality badge */}
      <div className="flex items-center gap-2 text-xs text-[var(--text-muted)]">
        <span className="inline-flex items-center rounded-full border border-[var(--border-default)] px-2 py-0.5 font-medium">
          {scanQuality.label}
        </span>
        <span>{scanQuality.note}</span>
      </div>

      {/* SECTION 1: Services Found (always visible, provable) */}
      {found.length > 0 && (
        <div className="rounded-[var(--radius-md)] border border-[var(--border-default)] p-3">
          <h3 className="text-sm font-semibold">Services Found on Site</h3>
          <p className="mt-1 text-xs text-[var(--text-muted)]">
            {found.filter((s) => s.status === "dedicated").length} dedicated page{found.filter((s) => s.status === "dedicated").length !== 1 ? "s" : ""} found
            {found.some((s) => s.status === "mention_only")
              ? ` · ${found.filter((s) => s.status === "mention_only").length} referenced on site`
              : ""}
          </p>
          <div className="mt-2 grid gap-2 md:grid-cols-2">
            {found.map((svc, idx) => (
              <ServiceItem key={`found-${idx}`} name={svc.name} status={svc.status} url={svc.url} />
            ))}
          </div>
        </div>
      )}

      {found.length === 0 && !lowCrawl && !suppressServiceGap && (
        <div className="rounded-[var(--radius-md)] border border-[var(--border-default)] p-3">
          <h3 className="text-sm font-semibold">Services Found on Site</h3>
          <p className="mt-2 text-xs text-[var(--text-secondary)]">
            No dedicated service pages were found during this scan.
          </p>
        </div>
      )}

      {/* SECTION 2: Not Found in Scan (collapsed, hedged) */}
      {notFound.length > 0 && (
        <details className="rounded-[var(--radius-md)] border border-[var(--border-default)] p-3">
          <summary className="cursor-pointer text-sm font-semibold text-[var(--text-secondary)]">
            Not Found in This Scan ({notFound.length})
          </summary>
          <p className="mt-2 text-xs text-[var(--text-muted)]">
            These services were not found during our scan. This does not necessarily mean pages don&apos;t exist — they may be behind JavaScript rendering or on pages we couldn&apos;t reach.
          </p>
          <div className="mt-2 grid gap-1.5 md:grid-cols-2">
            {notFound.map((svc, idx) => (
              <div key={`nf-${idx}`} className="flex items-center gap-2 px-2 py-1 text-sm text-[var(--text-muted)]">
                <span>⚪</span>
                <span>{svc.name}</span>
              </div>
            ))}
          </div>
        </details>
      )}

      {/* SECTION 3: Not Scanned (only when crawl was limited) */}
      {notScanned.length > 0 && (
        <div className="rounded-[var(--radius-md)] border border-[var(--border-default)] bg-zinc-50/50 p-3">
          <p className="text-xs text-[var(--text-muted)]">
            {notScanned.length} service{notScanned.length !== 1 ? "s" : ""} could not be evaluated due to limited scan access.
          </p>
        </div>
      )}
    </div>
  );
}

function ServiceItem({ name, status, url }: { name: string; status: CanonicalServiceStatus; url: string }) {
  const badge = statusBadge(status);
  const evidence = evidenceLabel(status, status === "dedicated" && url ? url : undefined);

  return (
    <details className="rounded-[var(--radius-md)] border border-[var(--border-default)] px-3 py-2">
      <summary className="flex cursor-pointer items-center justify-between gap-2 text-sm">
        <span className="truncate">
          {badge.marker} {name}
        </span>
        <span className="text-xs text-[var(--text-muted)]">{badge.label}</span>
      </summary>
      <div className="mt-2 space-y-1 text-xs text-[var(--text-secondary)]">
        {status === "dedicated" && url && (
          <p><strong>URL:</strong> {url}</p>
        )}
        <p><strong>Evidence:</strong> {evidence}</p>
      </div>
    </details>
  );
}

function resolveStatus(svc: ServiceRow, lowCrawl: boolean, suppressGap: boolean): CanonicalServiceStatus {
  if (lowCrawl || suppressGap) return "unknown";
  return canonicalStatus(String(svc.service_status || ""));
}
