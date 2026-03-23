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

type StepTone = "active" | "queued" | "queued-faded";

type ProgressStep = {
  label: string;
  description: string;
  badge: string;
  tone: StepTone;
};

function clamp(value: number, min: number, max: number) {
  return Math.min(max, Math.max(min, value));
}

function getMetricProgress(kind: "signals" | "pages", phase: string, polls: number, liveValue?: number) {
  if (typeof liveValue === "number" && Number.isFinite(liveValue) && liveValue > 0) {
    const multiplier = kind === "signals" ? 9 : 14;
    const floor = kind === "signals" ? 26 : 18;
    return clamp(floor + liveValue * multiplier, floor, kind === "signals" ? 92 : 88);
  }

  const stepIndex = getBriefStepIndex(phase, polls);
  const seed = kind === "signals" ? 20 : 12;
  const perPoll = kind === "signals" ? 6 : 5;
  const perStep = kind === "signals" ? 18 : 20;
  return clamp(seed + polls * perPoll + stepIndex * perStep, seed, kind === "signals" ? 88 : 82);
}

function getBriefStepIndex(phase: string, polls: number) {
  const lower = phase.toLowerCase().trim();

  if (!lower || lower === "preparing_brief" || lower === "building_brief") {
    return 0;
  }

  if (
    lower.includes("crawl") ||
    lower.includes("site") ||
    lower.includes("service") ||
    lower.includes("lightweight") ||
    lower.includes("verification") ||
    lower.includes("verified") ||
    lower.includes("details_scoring")
  ) {
    return 1;
  }

  if (
    lower.includes("brief") ||
    lower.includes("render") ||
    lower.includes("assemble") ||
    lower.includes("final") ||
    lower.includes("deep_brief_build") ||
    lower.includes("completed") ||
    lower.includes("done")
  ) {
    return 2;
  }

  if (lower.includes("market") || lower.includes("compet")) {
    return 1;
  }

  if (lower.includes("moderating") || lower.includes("resolving_intent")) {
    return 0;
  }

  return polls > 2 ? 1 : 0;
}

function getStepBadge(tone: StepTone, isResolved: boolean) {
  if (tone === "active") return "starting";
  if (isResolved) return "ready";
  return "queued";
}

function formatPhaseLabel(phase: string) {
  const normalized = phase.replaceAll("_", " ").trim().toLowerCase();
  if (!normalized) return "Preparing brief";
  if (normalized.includes("crawl") || normalized.includes("site") || normalized.includes("service")) {
    return "Reviewing website and service signals";
  }
  if (normalized.includes("market") || normalized.includes("compet")) {
    return "Comparing the local market";
  }
  if (normalized.includes("brief") || normalized.includes("render") || normalized.includes("assemble")) {
    return "Assembling the brief";
  }
  if (normalized.includes("final")) return "Finalizing output";
  return normalized.charAt(0).toUpperCase() + normalized.slice(1);
}

function getStatusLines(phase: string) {
  const label = formatPhaseLabel(phase);
  if (label === "Reviewing website and service signals") {
    return [
      "Checking service coverage and on-page depth",
      "Looking for conversion paths and trust elements",
    ];
  }
  if (label === "Comparing the local market") {
    return [
      "Comparing review position against nearby competitors",
      "Balancing market context with site findings",
    ];
  }
  if (label === "Assembling the brief") {
    return [
      "Sorting evidence into the final brief sections",
      "Packaging the final brief for delivery",
    ];
  }
  if (label === "Finalizing output") {
    return [
      "Saving the brief and preparing the route",
      "Opening the finished brief",
    ];
  }
  return [
    "Preparing the brief request",
    "Warming up the diagnostic workflow",
  ];
}

function getSteps(phase: string, polls: number): ProgressStep[] {
  const activeIndex = getBriefStepIndex(phase, polls);

  return [
    {
      label: "Resolve the business",
      description: "Confirming the business, website, and territory context.",
      badge: getStepBadge(activeIndex === 0 ? "active" : "queued", true),
      tone: activeIndex === 0 ? "active" : "queued",
    },
    {
      label: "Analyze market and site",
      description: "Checking service gaps, conversion paths, and competitor position.",
      badge: getStepBadge(activeIndex === 1 ? "active" : "queued", activeIndex > 1),
      tone: activeIndex === 1 ? "active" : "queued",
    },
    {
      label: "Assemble the brief",
      description: "Packaging the final brief with evidence and leverage notes.",
      badge: getStepBadge(activeIndex === 2 ? "active" : "queued-faded", false),
      tone: activeIndex === 2 ? "active" : "queued-faded",
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
  const [lineIndex, setLineIndex] = useState(0);
  const statusLines = useMemo(() => getStatusLines(progress.phase), [progress.phase]);
  const steps = useMemo(() => getSteps(progress.phase, progress.polls || 0), [progress.phase, progress.polls]);
  const location = [progress.city, progress.state].filter(Boolean).join(", ");
  const signalProgress = useMemo(
    () => getMetricProgress("signals", progress.phase, progress.polls || 0, progress.signalsFound),
    [progress.phase, progress.polls, progress.signalsFound],
  );
  const pagesProgress = useMemo(
    () => getMetricProgress("pages", progress.phase, progress.polls || 0, progress.pagesChecked),
    [progress.pagesChecked, progress.phase, progress.polls],
  );
  const signalStatus =
    typeof progress.signalsFound === "number"
      ? `${progress.signalsFound} signals found so far`
      : "Pulling together the strongest evidence for the brief";
  const pagesStatus =
    typeof progress.pagesChecked === "number"
      ? `${progress.pagesChecked} page${progress.pagesChecked === 1 ? "" : "s"} checked`
      : "Reviewing the website and organizing what matters most";

  useEffect(() => {
    const timer = window.setInterval(() => {
      setLineIndex((current) => (current + 1) % statusLines.length);
    }, 1900);
    return () => window.clearInterval(timer);
  }, [statusLines.length]);

  return (
    <div className={`brief-progress mx-auto w-full max-w-[420px] ${className}`}>
      <div className="brief-progress__surface">
        <section className="brief-progress__card brief-progress__card--header">
          <div className="brief-progress__header-row">
            <div className="brief-progress__avatar" aria-hidden="true">
              <svg className="brief-progress__spinner" viewBox="0 0 48 48">
                <circle className="brief-progress__spinner-track" cx="24" cy="24" r="18" />
                <circle className="brief-progress__spinner-arc" cx="24" cy="24" r="18" pathLength="100" />
              </svg>
            </div>
            <div className="brief-progress__header-copy">
              <p className="brief-progress__title">Building brief</p>
              <p className="brief-progress__subtitle">
                {progress.businessName || "Selected business"}
                {location ? ` · ${location}` : ""}
              </p>
              <p className="brief-progress__status-line">{statusLines[lineIndex]}</p>
              <p className="brief-progress__status-line">{formatPhaseLabel(progress.phase)}</p>
            </div>
          </div>
        </section>

        <section className="brief-progress__card">
          <p className="brief-progress__section-label">Signals</p>
          <div className="brief-progress__metric-row">
            <span className="brief-progress__dot brief-progress__dot--active" aria-hidden="true" />
            <div className="brief-progress__bar-track">
              <div
                className="brief-progress__bar brief-progress__bar--active"
                style={{ width: `${signalProgress}%` }}
              />
            </div>
          </div>
          <p className="brief-progress__section-status">{signalStatus}</p>
        </section>

        <section className="brief-progress__card">
          <p className="brief-progress__section-label">Pages checked</p>
          <div className="brief-progress__metric-row">
            <span className="brief-progress__dot brief-progress__dot--muted-loading" aria-hidden="true" />
            <div className="brief-progress__bar-track">
              <div
                className="brief-progress__bar brief-progress__bar--muted"
                style={{ width: `${pagesProgress}%` }}
              />
            </div>
          </div>
          <p className="brief-progress__section-status">{pagesStatus}</p>
        </section>

        <div className="brief-progress__steps">
          {steps.map((step, index) => (
            <section
              key={step.label}
              className={`brief-progress__step brief-progress__step--${step.tone}`}
              style={{ animationDelay: `${index * 70}ms` }}
            >
              <div
                className={`brief-progress__step-dot brief-progress__step-dot--${step.tone}`}
                aria-hidden="true"
              />
              <div className="brief-progress__step-copy">
                <div className="brief-progress__step-head">
                  <p className="brief-progress__step-title">{step.label}</p>
                  <span className={`brief-progress__badge brief-progress__badge--${step.tone}`}>
                    {step.badge}
                  </span>
                </div>
                <p className="brief-progress__step-description">{step.description}</p>
              </div>
            </section>
          ))}
        </div>

        <p className="brief-progress__footer">
          This renderer only appears while the brief is still being generated.
        </p>
      </div>

      <style jsx>{`
        .brief-progress {
          --bp-text-primary: var(--color-text-primary, var(--text-primary));
          --bp-text-secondary: var(--color-text-secondary, var(--text-secondary));
          --bp-text-tertiary: var(--color-text-tertiary, var(--muted-foreground));
          --bp-background-primary: var(--color-background-primary, var(--card));
          --bp-background-secondary: var(--color-background-secondary, var(--surface));
          --bp-background-info: var(--color-background-info, color-mix(in srgb, var(--primary) 10%, white));
          --bp-border-secondary: var(--color-border-secondary, var(--border));
          --bp-border-tertiary: var(--color-border-tertiary, var(--border));
          --bp-info: var(--color-info, var(--primary));
          --bp-radius-lg: var(--border-radius-lg, var(--radius-lg));
        }

        .brief-progress__surface {
          display: grid;
          gap: 8px;
        }

        .brief-progress__card,
        .brief-progress__step {
          background: var(--bp-background-primary);
          border: 0.5px solid var(--bp-border-tertiary);
          border-radius: var(--bp-radius-lg);
          padding: 1rem 1.25rem;
        }

        .brief-progress__card--header {
          background: var(--bp-background-secondary);
        }

        .brief-progress__header-row {
          display: flex;
          align-items: flex-start;
          gap: 0.875rem;
        }

        .brief-progress__avatar {
          display: flex;
          height: 52px;
          width: 52px;
          flex: 0 0 52px;
          align-items: center;
          justify-content: center;
          border-radius: 999px;
          background: var(--bp-background-primary);
          border: 0.5px solid var(--bp-border-tertiary);
        }

        .brief-progress__spinner {
          height: 30px;
          width: 30px;
          animation: brief-progress-spin 1.2s linear infinite;
        }

        .brief-progress__spinner-track {
          fill: none;
          stroke: color-mix(in srgb, var(--bp-text-tertiary) 18%, transparent);
          stroke-width: 3;
        }

        .brief-progress__spinner-arc {
          fill: none;
          stroke: var(--bp-text-primary);
          stroke-linecap: round;
          stroke-width: 3;
          stroke-dasharray: 38 100;
          stroke-dashoffset: 8;
        }

        .brief-progress__header-copy {
          min-width: 0;
        }

        .brief-progress__title {
          margin: 0;
          color: var(--bp-text-primary);
          font-size: 15px;
          font-weight: 500;
          line-height: 1.35;
        }

        .brief-progress__subtitle {
          margin: 4px 0 0;
          color: var(--bp-text-secondary);
          font-size: 12.5px;
          line-height: 1.45;
        }

        .brief-progress__status-line {
          margin: 4px 0 0;
          color: var(--bp-text-tertiary);
          font-size: 12px;
          line-height: 1.45;
        }

        .brief-progress__section-label {
          margin: 0;
          color: var(--bp-text-primary);
          font-size: 10.5px;
          font-weight: 500;
          letter-spacing: 0.08em;
          text-transform: uppercase;
        }

        .brief-progress__metric-row {
          margin-top: 10px;
          display: flex;
          align-items: center;
          gap: 10px;
        }

        .brief-progress__dot {
          height: 8px;
          width: 8px;
          flex: 0 0 8px;
          border-radius: 999px;
        }

        .brief-progress__dot--active {
          background: var(--bp-text-primary);
          animation: brief-progress-pulse 1.6s ease-in-out infinite;
        }

        .brief-progress__dot--muted {
          background: color-mix(in srgb, var(--bp-text-tertiary) 45%, transparent);
        }

        .brief-progress__dot--muted-loading {
          background: color-mix(in srgb, var(--bp-text-tertiary) 45%, transparent);
          animation: brief-progress-pulse 1.9s ease-in-out infinite;
        }

        .brief-progress__bar-track {
          position: relative;
          height: 8px;
          flex: 1;
          overflow: hidden;
          border-radius: 999px;
          background: var(--bp-background-secondary);
          border: 0.5px solid var(--bp-border-tertiary);
        }

        .brief-progress__bar {
          position: absolute;
          left: 0;
          top: 0;
          bottom: 0;
          border-radius: 999px;
        }

        .brief-progress__bar--active {
          width: 15%;
          background:
            linear-gradient(
              90deg,
              color-mix(in srgb, var(--bp-text-primary) 88%, transparent) 0%,
              color-mix(in srgb, var(--bp-info) 60%, white) 52%,
              color-mix(in srgb, var(--bp-text-primary) 88%, transparent) 100%
            );
          animation: brief-progress-bar-glow 2.6s ease-in-out infinite;
        }

        .brief-progress__bar--muted {
          width: 12%;
          background:
            linear-gradient(
              90deg,
              color-mix(in srgb, var(--bp-text-tertiary) 18%, transparent) 0%,
              color-mix(in srgb, var(--bp-text-tertiary) 34%, white) 50%,
              color-mix(in srgb, var(--bp-text-tertiary) 18%, transparent) 100%
            );
          animation: brief-progress-bar-glow 3.2s ease-in-out infinite;
        }

        .brief-progress__section-status {
          margin: 10px 0 0;
          color: var(--bp-text-tertiary);
          font-size: 12px;
          line-height: 1.45;
        }

        .brief-progress__steps {
          display: grid;
          gap: 8px;
        }

        .brief-progress__step {
          display: flex;
          align-items: flex-start;
          gap: 0.75rem;
          opacity: 0;
          transform: translateY(6px);
          animation: brief-progress-fadein 280ms ease-out forwards;
        }

        .brief-progress__step--active {
          border-color: var(--bp-border-secondary);
          opacity: 1;
        }

        .brief-progress__step--queued {
          opacity: 0.7;
        }

        .brief-progress__step--queued-faded {
          opacity: 0.55;
        }

        .brief-progress__step-dot {
          margin-top: 3px;
          height: 10px;
          width: 10px;
          flex: 0 0 10px;
          border-radius: 999px;
        }

        .brief-progress__step-dot--active {
          background: var(--bp-text-primary);
          animation: brief-progress-pulse 1.6s ease-in-out infinite;
        }

        .brief-progress__step-dot--queued,
        .brief-progress__step-dot--queued-faded {
          background: color-mix(in srgb, var(--bp-text-tertiary) 45%, transparent);
        }

        .brief-progress__step-copy {
          min-width: 0;
          flex: 1;
        }

        .brief-progress__step-head {
          display: flex;
          align-items: flex-start;
          justify-content: space-between;
          gap: 0.75rem;
        }

        .brief-progress__step-title {
          margin: 0;
          color: var(--bp-text-primary);
          font-size: 14px;
          font-weight: 500;
          line-height: 1.4;
        }

        .brief-progress__badge {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          border-radius: 999px;
          padding: 4px 8px;
          font-size: 11px;
          line-height: 1;
          text-transform: lowercase;
          white-space: nowrap;
        }

        .brief-progress__badge--active {
          background: var(--bp-background-info);
          color: var(--bp-info);
        }

        .brief-progress__badge--queued,
        .brief-progress__badge--queued-faded {
          background: var(--bp-background-secondary);
          color: var(--bp-text-tertiary);
        }

        .brief-progress__step-description {
          margin: 8px 0 0;
          color: var(--bp-text-secondary);
          font-size: 12.5px;
          line-height: 1.5;
        }

        .brief-progress__footer {
          margin: 4px 0 0;
          color: var(--bp-text-tertiary);
          font-size: 11.5px;
          line-height: 1.45;
          text-align: center;
        }

        @keyframes brief-progress-spin {
          to {
            transform: rotate(360deg);
          }
        }

        @keyframes brief-progress-pulse {
          0%,
          100% {
            opacity: 1;
          }

          50% {
            opacity: 0.4;
          }
        }

        @keyframes brief-progress-fadein {
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }

        @keyframes brief-progress-bar-glow {
          0%,
          100% {
            filter: brightness(0.98);
          }

          50% {
            filter: brightness(1.06);
          }
        }
      `}</style>
    </div>
  );
}
