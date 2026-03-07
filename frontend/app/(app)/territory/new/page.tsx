"use client";

import { useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { createTerritoryScan } from "@/lib/api";
import Button from "@/app/components/ui/Button";
import { Card, CardBody, CardHeader } from "@/app/components/ui/Card";
import Input from "@/app/components/ui/Input";
import EmptyState from "@/app/components/ui/EmptyState";

export default function NewTerritoryPage() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [city, setCity] = useState(searchParams.get("city") || "");
  const [state, setState] = useState(searchParams.get("state") || "");
  const [vertical, setVertical] = useState(searchParams.get("vertical") || "dentist");
  const [limit, setLimit] = useState(20);
  const [belowReviewAvg, setBelowReviewAvg] = useState(false);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    setRunning(true);
    try {
      const cappedLimit = Math.max(1, Math.min(20, Number(limit) || 20));
      const res = await createTerritoryScan({
        city: city.trim(),
        state: state.trim() || undefined,
        vertical,
        limit: cappedLimit,
        filters: { below_review_avg: belowReviewAvg || undefined },
      });
      router.push(`/territory/${res.scan_id}`);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to run territory scan");
      setRunning(false);
    }
  }

  return (
    <div className="mx-auto max-w-6xl">
      <section className="rounded-[36px] border border-black/6 bg-[linear-gradient(135deg,#f7f3ea_0%,#ffffff_60%,#f2f7ff_100%)] p-5 shadow-[0_18px_50px_rgba(23,20,17,0.05)] sm:p-7">
        <div className="grid gap-6 lg:grid-cols-[0.9fr_1.1fr]">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.22em] text-[var(--text-muted)]">Territory Scan</p>
            <h1 className="display-title mt-3 max-w-[11ch] text-4xl font-black tracking-tight text-[var(--text-primary)] sm:text-6xl">
              Start with the market, not the lead.
            </h1>
            <p className="mt-4 max-w-[42ch] text-sm leading-relaxed text-[var(--text-secondary)] sm:text-base">
              Build a ranked shortlist first. Then use Ask Neyma for more nuanced filtering only after the territory has been mapped.
            </p>

            <div className="mt-6 space-y-3">
              {[
                "Scan one local market at a time",
                "Return up to 20 ranked prospects",
                "Use deterministic market and site signals before opening any brief",
              ].map((line) => (
                <div key={line} className="rounded-[22px] border border-black/6 bg-white px-4 py-3 text-sm text-[var(--text-secondary)]">
                  {line}
                </div>
              ))}
            </div>
          </div>

          <Card className="overflow-hidden border border-black/8 bg-white shadow-[0_18px_40px_rgba(23,20,17,0.05)]">
            <CardHeader title="Scan Parameters" subtitle="Set market, vertical, and shortlist size. Territory scans currently return up to 20 ranked prospects." />
            <CardBody className="p-4 sm:p-5">
              <form onSubmit={handleSubmit} className="space-y-4">
                <div className="grid gap-4 sm:grid-cols-2">
                  <Input label="City *" value={city} onChange={(e) => setCity(e.target.value)} required placeholder="Austin" />
                  <Input label="State" value={state} onChange={(e) => setState(e.target.value)} placeholder="TX" />
                </div>
                <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
                  <label className="block">
                    <span className="mb-1 block text-sm font-medium text-[var(--text-secondary)]">Vertical</span>
                    <select
                      value={vertical}
                      onChange={(e) => setVertical(e.target.value)}
                      className="h-11 w-full rounded-[18px] border border-[var(--border-default)] bg-[#fbfaf7] px-3 text-sm focus:border-[var(--accent)] focus:outline-none"
                    >
                      <option value="dentist">Dentist</option>
                      <option value="dental">Dental</option>
                      <option value="orthodontist">Orthodontist</option>
                    </select>
                  </label>
                  <Input
                    label="Limit"
                    type="number"
                    min={1}
                    max={20}
                    value={limit}
                    onChange={(e) => setLimit(Math.max(1, Math.min(20, Number(e.target.value || 20))))}
                    className="h-11 rounded-[18px] bg-[#fbfaf7]"
                  />
                </div>
                <label className="flex items-center gap-2 rounded-[18px] border border-black/6 bg-[#fbfaf7] px-3 py-3 text-sm text-[var(--text-secondary)]">
                  <input type="checkbox" checked={belowReviewAvg} onChange={(e) => setBelowReviewAvg(e.target.checked)} />
                  Below review average
                </label>
                <div className="rounded-[20px] border border-black/6 bg-[#fbfaf7] px-4 py-3 text-xs leading-5 text-[var(--text-muted)]">
                  For service-gap and more nuanced criteria, continue in Ask Neyma after the territory scan finishes.
                </div>
                <div className="pt-1">
                  <Button type="submit" variant="primary" disabled={running} className="h-11 rounded-full px-5">
                    {running ? "Running..." : "Start scan"}
                  </Button>
                </div>
              </form>
            </CardBody>
          </Card>
        </div>
      </section>

      <Card className="mt-5 border border-black/8 bg-white shadow-[0_16px_38px_rgba(23,20,17,0.04)]">
        <CardHeader title="How Territory Ranking Works" subtitle="The shortlist is not random. We rank practices using lightweight, deterministic signals before any deeper brief is generated." />
        <CardBody className="space-y-4 p-4 sm:p-5">
          <div className="rounded-[24px] border border-black/6 bg-[linear-gradient(135deg,#fbfaf7_0%,#f3f7ff_100%)] p-4 text-sm leading-relaxed text-[var(--text-secondary)]">
            Territory ranking looks first at local market position and basic website quality, then returns the strongest candidates for deeper review.
          </div>
          <div className="grid gap-4 lg:grid-cols-3">
            <div className="rounded-[24px] border border-black/6 bg-white p-4">
              <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-[var(--text-muted)]">Market position</p>
              <p className="mt-2 text-sm text-[var(--text-secondary)]">We compare review count and rating against the local field to surface practices that appear weaker than nearby competitors.</p>
            </div>
            <div className="rounded-[24px] border border-black/6 bg-white p-4">
              <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-[var(--text-muted)]">Website basics</p>
              <p className="mt-2 text-sm text-[var(--text-secondary)]">We look for lightweight infrastructure signals such as SSL, contact paths, phone presence, viewport coverage, and basic site readiness.</p>
            </div>
            <div className="rounded-[24px] border border-black/6 bg-white p-4">
              <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-[var(--text-muted)]">What comes later</p>
              <p className="mt-2 text-sm text-[var(--text-secondary)]">Full service-page analysis, richer evidence, and revenue framing happen only after you open a brief or continue with Ask Neyma.</p>
            </div>
          </div>
        </CardBody>
      </Card>

      {error && <div className="mt-4"><EmptyState title="Scan failed" description={error} /></div>}
    </div>
  );
}
