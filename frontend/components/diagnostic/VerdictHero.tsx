"use client";

import Button from "@/app/components/ui/Button";

type Props = {
  verdict: "GO" | "SOFT_PASS" | "PASS";
  label: string;
  businessName: string;
  city: string;
  state?: string | null;
  opportunityBand?: string;
  reviewsText?: string;
  paidAdsStatus: string;
  topGap: string;
  phone?: string | null;
  websiteLabel?: string;
  websiteHref?: string | null;
  onAddToPipeline: () => void;
  onLogOutreach: () => void;
};

function verdictClass(verdict: "GO" | "SOFT_PASS" | "PASS"): string {
  if (verdict === "GO") return "border-emerald-200 bg-emerald-50 text-emerald-700";
  if (verdict === "SOFT_PASS") return "border-amber-200 bg-amber-50 text-amber-700";
  return "border-zinc-200 bg-zinc-50 text-zinc-700";
}

export default function VerdictHero(props: Props) {
  return (
    <div className="sticky top-0 z-40 rounded-[var(--radius-md)] border border-[var(--border-default)] bg-[var(--bg-card)]/95 p-3 backdrop-blur">
      <div className="grid gap-3 lg:grid-cols-[1.6fr_2.3fr_1.4fr] lg:items-center">
        <div>
          <span className={`inline-flex items-center rounded-full border px-2 py-1 text-xs font-semibold ${verdictClass(props.verdict)}`}>
            {props.label}
          </span>
          <p className="mt-1 text-sm font-semibold">{props.businessName}</p>
          <p className="text-xs text-[var(--text-muted)]">{props.city}{props.state ? `, ${props.state}` : ""}</p>
        </div>

        <div className="grid grid-cols-2 gap-2 xl:grid-cols-4">
          <CompactStat label="Opportunity" value={props.opportunityBand || "—"} />
          <CompactStat label="Reviews" value={props.reviewsText || "—"} />
          <CompactStat label="Paid Ads" value={props.paidAdsStatus} />
          <CompactStat label="Top Gap" value={props.topGap || "Top Gap: —"} />
        </div>

        <div className="flex flex-wrap justify-start gap-2 lg:justify-end">
          <Button onClick={props.onAddToPipeline}>Add to Pipeline</Button>
          <Button onClick={props.onLogOutreach} className="border-[var(--border-default)]">Log Outreach</Button>
        </div>
      </div>

      <div className="mt-2 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-[var(--text-secondary)]">
        {props.phone ? <a href={`tel:${props.phone}`} className="app-link">Phone: {props.phone}</a> : null}
        {props.websiteHref && props.websiteLabel ? <a href={props.websiteHref} target="_blank" rel="noreferrer" className="app-link">Website: {props.websiteLabel}</a> : null}
      </div>
    </div>
  );
}

function CompactStat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-[var(--radius-md)] border border-[var(--border-default)] px-2 py-1.5">
      <p className="text-[10px] uppercase tracking-wide text-[var(--text-muted)]">{label}</p>
      <p className="truncate text-xs font-semibold">{value || "—"}</p>
    </div>
  );
}
