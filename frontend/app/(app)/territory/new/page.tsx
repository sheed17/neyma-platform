"use client";

import { type ReactNode, useState } from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { createTerritoryScan } from "@/lib/api";
import EmptyState from "@/app/components/ui/EmptyState";
import { useAuth } from "@/lib/auth";

const US_STATE_CODES = new Set([
  "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
  "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
  "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
  "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
  "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
  "DC",
]);

function TerritoryHeroTitle() {
  return (
    <h1 className="mt-3 max-w-[12ch] text-[32px] font-medium leading-[1.02] tracking-[-0.02em] text-[var(--text-primary)]">
      Start with the market, <span className="text-[var(--primary)]">not</span> the lead.
    </h1>
  );
}

function FeatureItem({ children }: { children: ReactNode }) {
  return (
    <li className="flex items-center gap-3 text-[13px] text-[var(--text-secondary)]">
      <span aria-hidden className="h-[6px] w-[6px] rounded-full bg-[var(--primary)] opacity-70" />
      <span>{children}</span>
    </li>
  );
}

function FieldLabel({ htmlFor, children }: { htmlFor: string; children: ReactNode }) {
  return (
    <label htmlFor={htmlFor} className="mb-[5px] block text-[11px] font-medium text-[var(--text-secondary)]">
      {children}
    </label>
  );
}

function TerritoryRankingCard({
  title,
  body,
  bordered = true,
}: {
  title: string;
  body: string;
  bordered?: boolean;
}) {
  return (
    <div className={bordered ? "border-b-[0.5px] border-[var(--border-default)] px-5 py-4 md:border-b-0 md:border-r-[0.5px]" : "px-5 py-4"}>
      <p className="text-[10px] uppercase tracking-[0.1em] text-[var(--primary)]">{title}</p>
      <p className="mt-2 text-[12px] leading-[1.6] text-[var(--text-secondary)]">{body}</p>
    </div>
  );
}

export default function NewTerritoryPage() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const { access, accessLoading } = useAuth();
  const [city, setCity] = useState(searchParams.get("city") || "");
  const [state, setState] = useState(searchParams.get("state") || "");
  const [vertical, setVertical] = useState(searchParams.get("vertical") || "dentist");
  const [limit, setLimit] = useState("20");
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const scanLimitReached = !accessLoading && access?.can_use.territory_scan === false;
  const accessMessage = territoryUsageMessage(access);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    if (scanLimitReached) {
      setError(access?.viewer.is_guest ? "Guest territory scan limit reached. Create a free account to keep scanning." : "Free plan territory scan limit reached for this month.");
      return;
    }
    setRunning(true);
    try {
      const trimmedCity = city.trim();
      const trimmedState = state.trim().toUpperCase();
      if (!trimmedCity) throw new Error("Enter a city to run the scan.");
      if (!trimmedState) throw new Error("Enter a 2-letter US state code to run the scan.");
      if (!US_STATE_CODES.has(trimmedState)) throw new Error("Enter a valid 2-letter US state code.");
      const cappedLimit = Math.max(1, Math.min(20, Number(limit) || 20));
      const res = await createTerritoryScan({
        city: trimmedCity,
        state: trimmedState,
        vertical,
        limit: cappedLimit,
      });
      router.push(`/territory/${res.scan_id}`);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to run territory scan");
      setRunning(false);
    }
  }

  return (
    <div className="mx-auto max-w-6xl">
      <section className="grid items-stretch gap-4 lg:grid-cols-[0.92fr_1.08fr]">
        <div className="rounded-[var(--radius-lg)] border-[0.5px] border-[var(--border-default)] bg-[var(--muted)] p-5 sm:p-6">
          <div className="flex h-full flex-col">
            <p className="section-kicker">Territory Scan</p>
            <TerritoryHeroTitle />
            <p className="mt-4 max-w-[42ch] text-sm leading-relaxed text-[var(--text-secondary)] sm:text-base">
              Territory Scan maps one market at a time, ranks the field, and shows where the strongest opportunities are sitting first.
            </p>

            <ul className="mt-8 space-y-3">
              <FeatureItem>Scan one local market at a time</FeatureItem>
              <FeatureItem>Return up to 20 ML-ranked prospects</FeatureItem>
              <FeatureItem>Open briefs only where the signal earns deeper work</FeatureItem>
            </ul>
          </div>
        </div>

        <div className="rounded-[var(--radius-lg)] border-[0.5px] border-[var(--border-default)] bg-[var(--bg-card)] p-5 sm:p-6">
          <div className="border-b-[0.5px] border-[var(--border-default)] pb-4">
            <p className="text-[15px] font-medium text-[var(--text-primary)]">Scan Parameters</p>
            <p className="mt-2 text-sm text-[var(--text-secondary)]">
              Set the market, vertical, and shortlist size. Territory Scan gives you the first ranked view of the market, not the final answer.
            </p>
          </div>

          <form onSubmit={handleSubmit} noValidate className="space-y-4 pt-4">
            <div className="grid gap-4 sm:grid-cols-2">
              <div>
                <FieldLabel htmlFor="territory-city">City *</FieldLabel>
                <input
                  id="territory-city"
                  value={city}
                  onChange={(e) => setCity(e.target.value)}
                  required
                  placeholder=""
                  className="h-11 w-full rounded-[8px] border border-[var(--border-default)] bg-[var(--bg-card)] px-3 text-sm text-[var(--text-primary)] outline-none transition placeholder:text-[var(--text-muted)] focus:border-[var(--ring)]"
                />
              </div>
              <div>
                <FieldLabel htmlFor="territory-state">State *</FieldLabel>
                <input
                  id="territory-state"
                  value={state}
                  onChange={(e) => setState(e.target.value.replace(/[^a-z]/gi, "").slice(0, 2).toUpperCase())}
                  required
                  placeholder=""
                  maxLength={2}
                  inputMode="text"
                  autoCapitalize="characters"
                  pattern="[A-Z]{2}"
                  className="h-11 w-full rounded-[8px] border border-[var(--border-default)] bg-[var(--bg-card)] px-3 text-sm text-[var(--text-primary)] outline-none transition placeholder:text-[var(--text-muted)] focus:border-[var(--ring)]"
                />
              </div>
            </div>

            <div className="grid gap-4 sm:grid-cols-2">
              <div>
                <FieldLabel htmlFor="territory-vertical">Vertical</FieldLabel>
                <select
                  id="territory-vertical"
                  value={vertical}
                  onChange={(e) => setVertical(e.target.value)}
                  className="h-11 w-full rounded-[8px] border border-[var(--border-default)] bg-[var(--bg-card)] px-3 text-sm text-[var(--text-primary)] focus:border-[var(--ring)] focus:outline-none"
                >
                  <option value="dentist">Dentist</option>
                  <option value="dental">Dental</option>
                  <option value="orthodontist">Orthodontist</option>
                </select>
              </div>
              <div>
                <FieldLabel htmlFor="territory-limit">Limit</FieldLabel>
                <input
                  id="territory-limit"
                  type="text"
                  inputMode="numeric"
                  pattern="[0-9]*"
                  value={limit}
                  onChange={(e) => {
                    const next = e.target.value.replace(/\D/g, "").slice(0, 2);
                    setLimit(next);
                  }}
                  onBlur={() => {
                    const normalized = Math.max(1, Math.min(20, Number(limit) || 20));
                    setLimit(String(normalized));
                  }}
                  aria-describedby="territory-limit-hint"
                  className="h-11 w-full rounded-[8px] border border-[var(--border-default)] bg-[var(--bg-card)] px-3 text-sm text-[var(--text-primary)] outline-none transition placeholder:text-[var(--text-muted)] focus:border-[var(--ring)]"
                />
                <p
                  id="territory-limit-hint"
                  className="mt-2 text-[11px] uppercase tracking-[0.06em] text-[var(--text-muted)]/80"
                >
                  Max 20 prospects per scan
                </p>
              </div>
            </div>

            <div className="rounded-[12px] bg-[var(--muted)] px-3 py-[10px] text-[12px] leading-5 text-[var(--text-muted)]">
              Start with a city, set the vertical, and let Neyma return the first ranked pass across the market.
            </div>

            {accessMessage ? (
              <div className="rounded-[12px] border border-[var(--border-default)] bg-white px-3 py-[10px] text-[12px] leading-5 text-[var(--text-secondary)]">
                {accessMessage}
              </div>
            ) : null}

            <button
              type="submit"
              disabled={running || scanLimitReached}
              className="inline-flex h-11 w-full items-center justify-center rounded-[var(--radius)] bg-[var(--primary)] px-5 text-sm font-medium text-white transition hover:brightness-95 disabled:cursor-not-allowed disabled:opacity-60"
            >
              {running ? "Running..." : scanLimitReached ? (access?.viewer.is_guest ? "Sign up to continue →" : "Monthly limit reached") : "Start scan →"}
            </button>

            {scanLimitReached ? (
              <Link
                href={access?.viewer.is_guest ? "/register" : "/settings"}
                className="inline-flex h-11 w-full items-center justify-center rounded-[var(--radius)] border border-[var(--border-default)] bg-white px-5 text-sm font-medium text-[var(--text-primary)] transition hover:bg-[var(--surface)]"
              >
                {access?.viewer.is_guest ? "Create free account" : "View plan and usage"}
              </Link>
            ) : null}
          </form>
        </div>
      </section>

      <section className="mt-5 overflow-hidden rounded-[var(--radius-lg)] border-[0.5px] border-[var(--border-default)] bg-[var(--bg-card)]">
        <div className="border-b-[0.5px] border-[var(--border-default)] px-5 py-4">
          <p className="text-[15px] font-medium text-[var(--text-primary)]">How Territory Ranking Works</p>
          <p className="mt-2 text-sm text-[var(--text-secondary)]">
            The shortlist is not random. We rank practices using lightweight signals, feature engineering, and ML scoring before any deeper brief is generated.
          </p>
        </div>

        <div className="border-b-[0.5px] border-[var(--border-default)] px-5 py-[14px] text-[13px] leading-6 text-[var(--text-secondary)]">
          Territory ranking looks first at local market position and basic website quality, then uses ML scoring to return the strongest candidates for deeper review.
        </div>

        <div className="grid md:grid-cols-3">
          <TerritoryRankingCard
            title="Market position"
            body="We compare review count and rating against the local field to surface practices that appear weaker than nearby competitors."
          />
          <TerritoryRankingCard
            title="Website basics"
            body="We look for lightweight infrastructure signals such as SSL, contact paths, phone presence, viewport coverage, and basic site readiness."
          />
          <TerritoryRankingCard
            title="What comes later"
            body="Full brief generation, richer evidence, and deeper reasoning happen when you open a brief from the shortlist."
            bordered={false}
          />
        </div>
      </section>

      {error && (
        <div className="mt-4">
          <EmptyState title="Scan failed" description={error} />
        </div>
      )}
    </div>
  );
}

function territoryUsageMessage(access: ReturnType<typeof useAuth>["access"]) {
  if (!access) return null;
  if (access.viewer.is_guest) {
    return `Guest plan: ${access.remaining.territory_scan ?? 0} of ${access.limits.territory_scan ?? 0} territory scans left on this device. Max ${access.limits.territory_scan ?? 0}.`;
  }
  if (String(access.plan_tier) === "free") {
    return `Free plan: ${access.remaining.territory_scan ?? 0} of ${access.limits.territory_scan ?? 0} territory scans left this month. Max ${access.limits.territory_scan ?? 0} each month.`;
  }
  return `${String(access.plan_tier).toUpperCase()} plan: territory scans are available without a usage cap.`;
}
