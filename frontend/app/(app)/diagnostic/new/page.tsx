"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { pollUntilDone, submitDiagnostic } from "@/lib/api";
import type { JobStatusResponse } from "@/lib/types";
import Button from "@/app/components/ui/Button";
import { Card, CardBody, CardHeader } from "@/app/components/ui/Card";
import Input from "@/app/components/ui/Input";
import BriefBuildProgress, { type BriefBuildProgressState } from "@/app/components/BriefBuildProgress";

export default function NewDiagnosticPage() {
  const router = useRouter();
  const [businessName, setBusinessName] = useState("");
  const [city, setCity] = useState("");
  const [state, setState] = useState("");
  const [website, setWebsite] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [status, setStatus] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [progress, setProgress] = useState<BriefBuildProgressState | null>(null);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    setStatus(null);
    setProgress(null);
    setSubmitting(true);
    try {
      const trimmedBusinessName = businessName.trim();
      const trimmedCity = city.trim();
      const trimmedState = state.trim();
      const trimmedWebsite = website.trim();
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
      if (result.status === "failed") throw new Error(result.error || "Diagnostic failed");
      if (!result.diagnostic_id) throw new Error("No diagnostic ID returned");
      router.push(`/diagnostic/${result.diagnostic_id}`);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Request failed");
      setSubmitting(false);
      setProgress(null);
    }
  }

  return (
    <div className="mx-auto max-w-6xl">
      <section className="rounded-[24px] border border-[var(--border-default)] bg-[var(--bg-card)] p-5 shadow-[0_18px_50px_rgba(10,10,10,0.04)] sm:p-7">
        <div className="grid gap-6 lg:grid-cols-[0.92fr_1.08fr]">
          <div>
            <p className="section-kicker">Build Brief</p>
            <h1 className="display-title mt-3 max-w-[11ch] text-4xl text-[var(--text-primary)] sm:text-6xl">
              Build one full brief directly.
            </h1>
            <p className="mt-4 max-w-[42ch] text-sm leading-relaxed text-[var(--text-secondary)] sm:text-base">
              Use this when you already know the business and want the full AI-assisted brief without starting from a territory scan or Ask query.
            </p>

            <div className="mt-6 space-y-3">
              {[
                "Best when the target business is already known",
                "Builds the same AI- and ML-backed brief as the rest of the workflow",
                "Use website if you have it, but it is optional",
              ].map((line) => (
                <div key={line} className="rounded-[18px] border border-[var(--border-default)] bg-[var(--muted)] px-4 py-3 text-sm text-[var(--text-secondary)]">
                  {line}
                </div>
              ))}
            </div>
          </div>

          <Card className="overflow-hidden border border-[var(--border-default)] bg-[var(--bg-card)] shadow-[0_18px_40px_rgba(10,10,10,0.04)]">
            <CardHeader title="Brief Input" subtitle="Enter the business details and Neyma will build the full AI-assisted brief." />
            <CardBody className="p-4 sm:p-5">
              <form onSubmit={handleSubmit} className="space-y-4">
                <div className="grid gap-4 sm:grid-cols-2">
                  <Input
                    label="Business name *"
                    required
                    value={businessName}
                    onChange={(e) => setBusinessName(e.target.value)}
                    placeholder="Japantown Dental"
                    className="h-11"
                  />
                  <Input
                    label="Website (optional)"
                    value={website}
                    onChange={(e) => setWebsite(e.target.value)}
                    placeholder="japantowndental.com"
                    className="h-11"
                  />
                </div>
                <div className="grid gap-4 sm:grid-cols-2">
                  <Input
                    label="City *"
                    required
                    value={city}
                    onChange={(e) => setCity(e.target.value)}
                    placeholder="San Jose"
                    className="h-11"
                  />
                  <Input
                    label="State *"
                    required
                    value={state}
                    onChange={(e) => setState(e.target.value)}
                    placeholder="CA"
                    className="h-11"
                  />
                </div>

                <div className="rounded-[16px] border border-[var(--border-default)] bg-[var(--muted)] px-4 py-3 text-xs leading-5 text-[var(--text-muted)]">
                  This path skips territory ranking and goes straight into the full AI and ML brief pipeline.
                </div>

                <Button
                  type="submit"
                  disabled={submitting}
                  className="h-11 rounded-full px-5"
                >
                  {submitting ? "Building..." : "Build brief"}
                </Button>
              </form>

              {status && !error && !progress && (
                <p className="mt-3 text-sm text-[var(--text-secondary)]">{status}</p>
              )}
              {error && <p className="mt-3 text-sm text-rose-600">{error}</p>}
            </CardBody>
          </Card>
        </div>
      </section>

      {progress && (
        <BriefBuildProgress progress={progress} className="mx-auto mt-4 max-w-5xl" />
      )}
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
