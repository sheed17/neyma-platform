"use client";

import { type ReactNode, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { pollUntilDone, submitDiagnostic } from "@/lib/api";
import { useAuth } from "@/lib/auth";
import { clientFacingBriefError } from "@/lib/present";
import type { JobStatusResponse } from "@/lib/types";
import BriefBuildProgress, { type BriefBuildProgressState } from "@/app/components/BriefBuildProgress";

const US_STATE_CODES = new Set([
  "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
  "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
  "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
  "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
  "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
  "DC",
]);

function BuildBriefTitle() {
  return (
    <h1 className="mt-3 max-w-[12ch] text-[32px] font-medium leading-[1.02] tracking-[-0.02em] text-[var(--text-primary)]">
      Build one full brief <span className="text-[var(--primary)]">directly</span>.
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

function Eyebrow({ children }: { children: ReactNode }) {
  return <p className="text-[11px] font-medium uppercase tracking-[0.08em] text-[var(--text-muted)]">{children}</p>;
}

function FieldLabel({ htmlFor, children }: { htmlFor: string; children: ReactNode }) {
  return (
    <label htmlFor={htmlFor} className="mb-[5px] block text-[11px] font-medium text-[var(--text-secondary)]">
      {children}
    </label>
  );
}

export default function NewDiagnosticPage() {
  const router = useRouter();
  const { access, accessLoading } = useAuth();
  const [businessName, setBusinessName] = useState("");
  const [city, setCity] = useState("");
  const [state, setState] = useState("");
  const [website, setWebsite] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [status, setStatus] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [progress, setProgress] = useState<BriefBuildProgressState | null>(null);
  const briefLimitReached = !accessLoading && access?.can_use.diagnostic === false;
  const accessMessage = briefUsageMessage(access);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    setStatus(null);
    setProgress(null);
    if (briefLimitReached) {
      setError(access?.viewer.is_guest ? "Guest brief limit reached. Create a free account to build more briefs." : "Free plan brief limit reached for this month.");
      return;
    }
    setSubmitting(true);
    try {
      const trimmedBusinessName = businessName.trim();
      const trimmedCity = city.trim();
      const trimmedState = state.trim().toUpperCase();
      const trimmedWebsite = website.trim();
      if (!trimmedBusinessName) throw new Error("Enter a business name to build the brief.");
      if (!trimmedCity) throw new Error("Enter a city to build the brief.");
      if (!trimmedState) throw new Error("Enter a 2-letter US state code to build the brief.");
      if (!US_STATE_CODES.has(trimmedState)) {
        throw new Error("Enter a valid 2-letter US state code.");
      }
      const { job_id } = await submitDiagnostic({
        business_name: trimmedBusinessName,
        city: trimmedCity,
        state: trimmedState,
        ...(trimmedWebsite ? { website: trimmedWebsite } : {}),
      });
      setStatus("Brief pipeline running...");
      setProgress({
        phase: "preparing_brief",
        businessName: trimmedBusinessName,
        city: trimmedCity,
        state: trimmedState,
        polls: 0,
      });
      const result: JobStatusResponse = await pollUntilDone(job_id, (s) => {
        if (s.status === "running") {
          setStatus("Building full brief...");
          setProgress(buildBriefProgressState(s, trimmedBusinessName, trimmedCity, trimmedState));
        }
      });
      if (result.status === "failed") throw new Error(clientFacingBriefError(result.error));
      if (!result.diagnostic_id) throw new Error("No diagnostic ID returned");
      router.push(`/diagnostic/${result.diagnostic_id}`);
    } catch (err) {
      setError(clientFacingBriefError(err instanceof Error ? err.message : "Request failed"));
      setSubmitting(false);
      setProgress(null);
    }
  }

  return (
    <div className="mx-auto max-w-6xl">
      <section className="grid items-stretch gap-4 lg:grid-cols-[0.92fr_1.08fr]">
        <div className="rounded-[var(--radius-lg)] border-[0.5px] border-[var(--border-default)] bg-[var(--muted)] p-5 sm:p-6">
          <div className="flex h-full flex-col">
            <Eyebrow>Build Brief</Eyebrow>
            <BuildBriefTitle />
            <p className="mt-4 max-w-[42ch] text-sm leading-relaxed text-[var(--text-secondary)] sm:text-base">
              Use this when you already know the business and want the full AI-assisted brief without starting from a territory scan or Ask query.
            </p>

            <ul className="mt-8 space-y-3">
              <FeatureItem>Best when the target business is already known</FeatureItem>
              <FeatureItem>Builds the same AI- and ML-backed brief as the rest of the workflow</FeatureItem>
              <FeatureItem>Use website if you have it, but it is optional</FeatureItem>
            </ul>
          </div>
        </div>

        <div className="rounded-[var(--radius-lg)] border-[0.5px] border-[var(--border-default)] bg-[var(--bg-card)] p-5 sm:p-6">
          <div className="border-b-[0.5px] border-[var(--border-default)] pb-4">
            <Eyebrow>Brief Input</Eyebrow>
            <p className="mt-2 text-sm text-[var(--text-secondary)]">
              Enter the business details and Neyma will build the full AI-assisted brief.
            </p>
          </div>

          <form onSubmit={handleSubmit} noValidate className="space-y-4 pt-4">
            <div className="grid gap-4 sm:grid-cols-2">
              <div>
                <FieldLabel htmlFor="brief-business-name">Business name *</FieldLabel>
                <input
                  id="brief-business-name"
                  required
                  value={businessName}
                  onChange={(e) => setBusinessName(e.target.value)}
                  placeholder=""
                  className="h-11 w-full rounded-[8px] border border-[var(--border-default)] bg-[var(--bg-card)] px-3 text-sm text-[var(--text-primary)] outline-none transition placeholder:text-[var(--text-muted)] focus:border-[var(--ring)]"
                />
              </div>
              <div>
                <FieldLabel htmlFor="brief-website">Website (optional)</FieldLabel>
                <input
                  id="brief-website"
                  value={website}
                  onChange={(e) => setWebsite(e.target.value)}
                  placeholder=""
                  className="h-11 w-full rounded-[8px] border border-[var(--border-default)] bg-[var(--bg-card)] px-3 text-sm text-[var(--text-primary)] outline-none transition placeholder:text-[var(--text-muted)] focus:border-[var(--ring)]"
                />
              </div>
            </div>

            <div className="grid gap-4 sm:grid-cols-[minmax(0,1fr)_80px]">
              <div>
                <FieldLabel htmlFor="brief-city">City *</FieldLabel>
                <input
                  id="brief-city"
                  required
                  value={city}
                  onChange={(e) => setCity(e.target.value)}
                  placeholder=""
                  className="h-11 w-full rounded-[8px] border border-[var(--border-default)] bg-[var(--bg-card)] px-3 text-sm text-[var(--text-primary)] outline-none transition placeholder:text-[var(--text-muted)] focus:border-[var(--ring)]"
                />
              </div>
              <div>
                <FieldLabel htmlFor="brief-state">State *</FieldLabel>
                <input
                  id="brief-state"
                  required
                  value={state}
                  onChange={(e) => setState(e.target.value.replace(/[^a-z]/gi, "").slice(0, 2).toUpperCase())}
                  placeholder=""
                  maxLength={2}
                  inputMode="text"
                  autoCapitalize="characters"
                  pattern="[A-Z]{2}"
                  className="h-11 w-full rounded-[8px] border border-[var(--border-default)] bg-[var(--bg-card)] px-3 text-sm text-[var(--text-primary)] outline-none transition placeholder:text-[var(--text-muted)] focus:border-[var(--ring)]"
                />
              </div>
            </div>

            <div className="rounded-[12px] bg-[var(--muted)] px-3 py-[10px] text-[12px] leading-5 text-[var(--text-muted)]">
              This path skips territory ranking and goes straight into the full AI and ML brief pipeline.
            </div>

            {accessMessage ? (
              <div className="rounded-[12px] border border-[var(--border-default)] bg-white px-3 py-[10px] text-[12px] leading-5 text-[var(--text-secondary)]">
                {accessMessage}
              </div>
            ) : null}

            <button
              type="submit"
              disabled={submitting || briefLimitReached}
              className="inline-flex h-11 w-full items-center justify-center rounded-[var(--radius)] bg-[var(--primary)] px-5 text-sm font-medium text-white transition hover:brightness-95 disabled:cursor-not-allowed disabled:opacity-60"
            >
              {submitting ? "Building..." : briefLimitReached ? (access?.viewer.is_guest ? "Sign up to continue →" : "Monthly limit reached") : "Build brief →"}
            </button>

            {briefLimitReached ? (
              <Link
                href={access?.viewer.is_guest ? "/register" : "/settings"}
                className="inline-flex h-11 w-full items-center justify-center rounded-[var(--radius)] border border-[var(--border-default)] bg-white px-5 text-sm font-medium text-[var(--text-primary)] transition hover:bg-[var(--surface)]"
              >
                {access?.viewer.is_guest ? "Create free account" : "View plan and usage"}
              </Link>
            ) : null}
          </form>

          {status && !error && !progress ? (
            <p className="mt-3 text-sm text-[var(--text-secondary)]">{status}</p>
          ) : null}
          {error ? <p className="mt-3 text-sm text-rose-600">{error}</p> : null}
        </div>
      </section>

      {progress ? <BriefBuildProgress progress={progress} className="mx-auto mt-4 max-w-5xl" /> : null}
    </div>
  );
}

function buildBriefProgressState(
  job: JobStatusResponse,
  businessName: string,
  city: string,
  state: string,
): BriefBuildProgressState {
  const payload = (job.progress || {}) as Record<string, unknown>;
  const inner = (payload.progress || {}) as Record<string, unknown>;

  return {
    phase: String(payload.phase || "building_brief"),
    businessName,
    city,
    state,
    polls: inner.polls ? Number(inner.polls) : undefined,
    pagesChecked: Number(inner.pages_crawled || inner.pages_checked || 0) || undefined,
    signalsFound: Number(inner.signals_found || inner.signals || inner.findings || 0) || undefined,
  };
}

function briefUsageMessage(access: ReturnType<typeof useAuth>["access"]) {
  if (!access) return null;
  if (access.viewer.is_guest) {
    return `${access.remaining.diagnostic ?? 0} of ${access.limits.diagnostic ?? 0} guest brief builds left on this device. Create a free account to save and reopen briefs.`;
  }
  if (String(access.plan_tier) === "free") {
    return `${access.remaining.diagnostic ?? 0} of ${access.limits.diagnostic ?? 0} brief builds left this month on the free plan.`;
  }
  return `${String(access.plan_tier).toUpperCase()} plan: brief builds are available without a usage cap.`;
}
