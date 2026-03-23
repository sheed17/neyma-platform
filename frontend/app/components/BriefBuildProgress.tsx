"use client";

import { useEffect, useMemo, useState } from "react";

export type BriefBuildProgressState = {
  phase: string;
  businessName?: string | null;
  city?: string | null;
  state?: string | null;
  polls?: number;
  pagesChecked?: number;
  signalsFound?: number;
};

function formatMetricValue(value?: number) {
  return typeof value === "number" && Number.isFinite(value) ? String(value) : "—";
}

function formatPhaseLabel(phase: string) {
  const normalized = phase.replaceAll("_", " ").trim().toLowerCase();
  if (!normalized) return "Preparing brief";
  if (normalized.includes("crawl") || normalized.includes("site") || normalized.includes("service")) return "Reviewing website and service signals";
  if (normalized.includes("market") || normalized.includes("compet")) return "Comparing the local market";
  if (normalized.includes("brief") || normalized.includes("render") || normalized.includes("assemble")) return "Assembling the brief";
  if (normalized.includes("final")) return "Finalizing output";
  return normalized.charAt(0).toUpperCase() + normalized.slice(1);
}

function getSubevents(phase: string) {
  const label = formatPhaseLabel(phase);
  if (label === "Reviewing website and service signals") {
    return [
      "Checking service coverage and on-page depth",
      "Looking for conversion paths and trust elements",
      "Resolving site signals into the brief",
    ];
  }
  if (label === "Comparing the local market") {
    return [
      "Comparing review position against nearby competitors",
      "Checking the local field for structural weakness",
      "Balancing market context with site findings",
    ];
  }
  if (label === "Assembling the brief") {
    return [
      "Building the executive summary and leverage framing",
      "Sorting evidence into the final brief sections",
      "Preparing the outreach-ready output",
    ];
  }
  if (label === "Finalizing output") {
    return [
      "Saving the brief and preparing the route",
      "Locking the final output",
      "Opening the finished brief",
    ];
  }
  return [
    "Preparing the brief request",
    "Starting the diagnostic workflow",
    "Warming up the job run",
  ];
}

function getStepState(phase: string, polls: number) {
  const lower = phase.toLowerCase();
  const syntheticPhase =
    lower.includes("crawl") || lower.includes("site") || lower.includes("service")
      ? 1
      : lower.includes("market") || lower.includes("compet")
        ? 2
        : lower.includes("brief") || lower.includes("render") || lower.includes("assemble") || lower.includes("final")
          ? 3
          : polls > 6
            ? 3
            : polls > 2
              ? 2
              : 1;

  return [
    {
      label: "Resolve the business",
      done: syntheticPhase > 1,
      active: syntheticPhase === 1,
      meta: polls > 0 ? "ready" : "starting",
      description: "Confirming the business, website, and territory context.",
    },
    {
      label: "Analyze market and site",
      done: syntheticPhase > 2,
      active: syntheticPhase === 2,
      meta: polls > 1 ? "running" : "queued",
      description: "Checking service gaps, conversion paths, and competitor position.",
    },
    {
      label: "Assemble the brief",
      done: false,
      active: syntheticPhase === 3,
      meta: polls > 5 ? "compiling" : "queued",
      description: "Packaging the final brief with evidence and leverage notes.",
    },
  ];
}

export default function BriefBuildProgress({
  progress,
  className = "",
}: {
  progress: BriefBuildProgressState;
  className?: string;
}) {
  const [subeventIndex, setSubeventIndex] = useState(0);
  const [dotCount, setDotCount] = useState(1);
  const subevents = useMemo(() => getSubevents(progress.phase), [progress.phase]);
  const steps = useMemo(() => getStepState(progress.phase, progress.polls || 0), [progress.phase, progress.polls]);

  useEffect(() => {
    const subeventTimer = window.setInterval(() => {
      setSubeventIndex((current) => (current + 1) % subevents.length);
    }, 1700);
    const dotTimer = window.setInterval(() => {
      setDotCount((current) => (current % 3) + 1);
    }, 420);

    return () => {
      window.clearInterval(subeventTimer);
      window.clearInterval(dotTimer);
    };
  }, [subevents.length]);

  return (
    <div className={`rounded-[32px] border border-[#1f57c3]/10 bg-[linear-gradient(135deg,rgba(0,0,0,0.035)_0%,rgba(79,121,199,0.09)_55%,rgba(242,191,47,0.08)_100%)] p-4 shadow-[0_24px_60px_rgba(23,20,17,0.08)] sm:p-5 ${className}`}>
      <div className="rounded-[26px] border border-white/70 bg-[rgba(255,255,255,0.88)] p-4 backdrop-blur-sm sm:p-5">
        <div className="flex flex-col gap-5 sm:flex-row sm:items-start">
          <div className="flex items-center gap-4 sm:w-[250px] sm:flex-col sm:items-start">
            <div className="relative flex h-[72px] w-[72px] shrink-0 items-center justify-center rounded-full border border-black/10 bg-[radial-gradient(circle_at_30%_30%,rgba(79,121,199,0.16),transparent_45%),radial-gradient(circle_at_70%_70%,rgba(242,191,47,0.18),transparent_48%),#fcfbf8] shadow-[inset_0_1px_0_rgba(255,255,255,0.8),0_16px_32px_rgba(23,20,17,0.08)]">
              <span className="absolute inline-flex h-14 w-14 animate-ping rounded-full border border-black/10" />
              <span className="absolute inline-flex h-10 w-10 animate-spin rounded-full border-2 border-black/15 border-t-[#4f79c7]" />
              <span className="relative inline-flex h-3.5 w-3.5 rounded-full bg-black shadow-[0_0_0_6px_rgba(0,0,0,0.06)]" />
            </div>
            <div className="min-w-0">
              <div className="flex flex-wrap items-center gap-2">
                <p className="text-sm font-semibold text-[var(--text-primary)]">Building brief</p>
                {progress.polls ? (
                  <span className="rounded-full border border-black/6 bg-white px-2 py-1 text-[11px] font-medium text-[var(--text-secondary)]">
                    Poll {progress.polls}
                  </span>
                ) : null}
              </div>
              <p className="mt-1 text-sm text-[var(--text-secondary)]">
                {progress.businessName || "Selected business"}
                {progress.city ? ` · ${progress.city}${progress.state ? `, ${progress.state}` : ""}` : ""}
              </p>
              <p className="mt-2 text-xs leading-5 text-[var(--text-muted)]">
                {subevents[subeventIndex]}
                <span className="inline-block w-4 text-left text-[var(--text-secondary)]">{ ".".repeat(dotCount) }</span>
              </p>
              <p className="mt-2 text-xs font-medium text-[var(--text-secondary)]">{formatPhaseLabel(progress.phase)}</p>
            </div>
          </div>

          <div className="min-w-0 flex-1">
            <div className="grid gap-2 sm:grid-cols-2">
              <div className="rounded-[22px] border border-black/6 bg-white px-3 py-3">
                <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-[var(--text-muted)]">Signals</p>
                <p className="mt-2 text-2xl font-semibold text-[var(--text-primary)]">{formatMetricValue(progress.signalsFound)}</p>
                <p className="mt-1 text-[11px] text-[var(--text-muted)]">
                  {typeof progress.signalsFound === "number" ? "Live evidence count" : "Waiting for live evidence telemetry"}
                </p>
              </div>
              <div className="rounded-[22px] border border-black/6 bg-white px-3 py-3">
                <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-[var(--text-muted)]">Pages checked</p>
                <p className="mt-2 text-2xl font-semibold text-[var(--text-primary)]">{formatMetricValue(progress.pagesChecked)}</p>
                <p className="mt-1 text-[11px] text-[var(--text-muted)]">
                  {typeof progress.pagesChecked === "number" ? "Live crawl count" : "Updates when the crawl reports in"}
                </p>
              </div>
            </div>

            <div className="mt-3 space-y-2 rounded-[24px] border border-black/6 bg-[#fbfaf7] p-3.5">
              {steps.map((step) => (
                <div key={step.label} className="flex items-start gap-3 rounded-2xl border border-black/6 bg-white px-3 py-3">
                  <span className="mt-0.5 inline-flex h-5 w-5 shrink-0 items-center justify-center rounded-full border border-black/8 bg-[#fcfbf8]">
                    {step.done ? (
                      <span className="inline-flex h-2.5 w-2.5 rounded-full bg-black" />
                    ) : step.active ? (
                      <span className="inline-flex h-2.5 w-2.5 animate-pulse rounded-full bg-[#4f79c7]" />
                    ) : (
                      <span className="inline-flex h-2.5 w-2.5 rounded-full bg-black/15" />
                    )}
                  </span>
                  <div className="min-w-0 flex-1">
                    <div className="flex flex-wrap items-center justify-between gap-2">
                      <p className="text-sm font-medium text-[var(--text-primary)]">{step.label}</p>
                      <span className="text-xs text-[var(--text-muted)]">{step.meta}</span>
                    </div>
                    <p className="mt-1 text-xs leading-5 text-[var(--text-muted)]">{step.description}</p>
                  </div>
                </div>
              ))}
            </div>

            <p className="mt-3 text-xs text-[var(--text-muted)]">
              This only appears when the brief still needs to be generated.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
