"use client";

import { categoryEvidence, clientFacingAuditText, dedupeStrings, isSchemaRelated, isUnknownLike } from "@/lib/present";

type EvidenceItem = { label?: string; value?: string };

type Props = {
  pagesCrawled?: number;
  crawlConfidence?: string;
  jsDetected?: boolean;
  evidenceBullets?: string[];
  evidenceItems?: EvidenceItem[];
  riskFlags?: string[];
};

export default function AuditAccordion(props: Props) {
  const rawEvidence = [
    ...(props.evidenceBullets || []),
    ...((props.evidenceItems || []).map((e) => `${String(e.label || "")}: ${String(e.value || "")}`)),
  ];
  const cleanedEvidence = dedupeStrings(rawEvidence).map(clientFacingAuditText).filter((x) => x && !isSchemaRelated(x));
  const cleanedRisk = dedupeStrings(props.riskFlags || []).map(clientFacingAuditText).filter((x) => x && !isSchemaRelated(x));
  const grouped = categoryEvidence(cleanedEvidence);

  return (
    <details className="rounded-[22px] border border-[var(--border-default)] bg-[var(--bg-card)] p-4 sm:p-5">
      <summary className="cursor-pointer text-sm font-semibold text-[var(--text-primary)]">Full Audit (Advanced)</summary>
      <div className="mt-4 space-y-4 text-sm text-[var(--text-secondary)]">
        <div className="grid gap-3 md:grid-cols-2">
          <p><strong>Pages checked:</strong> {props.pagesCrawled ?? "—"}</p>
          {!isUnknownLike(props.crawlConfidence) ? <p><strong>Coverage confidence:</strong> {props.crawlConfidence}</p> : null}
          {typeof props.jsDetected === "boolean" ? <p><strong>JS detected:</strong> {props.jsDetected ? "Yes" : "No"}</p> : null}
        </div>

        <Group title="Reviews & Reputation" items={grouped.reputation} />
        <Group title="Site Capture" items={grouped.capture} />
        <Group title="Market Context" items={grouped.market} />
        <Group title="Verification Details" items={grouped.crawl} />

        {cleanedRisk.length > 0 ? (
          <div>
            <p className="mb-1 text-xs uppercase tracking-wide text-[var(--text-muted)]">Risk Flags</p>
            <ul className="list-disc space-y-1 pl-5">
              {cleanedRisk.map((item, i) => <li key={i}>{item}</li>)}
            </ul>
          </div>
        ) : null}
      </div>
    </details>
  );
}

function Group({ title, items }: { title: string; items: string[] }) {
  if (!items.length) return null;
  return (
    <div className="rounded-[18px] border border-[var(--border-default)] bg-[var(--surface)] p-4">
      <p className="mb-2 text-xs uppercase tracking-wide text-[var(--text-muted)]">{title}</p>
      <ul className="list-disc space-y-1.5 pl-5">
        {items.map((item, i) => <li key={i}>{item}</li>)}
      </ul>
    </div>
  );
}
