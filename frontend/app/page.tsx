"use client";

import Link from "next/link";
import { useAuth } from "@/lib/auth";

const navItems = [
  { label: "Product", href: "#product" },
  { label: "Workflow", href: "#workflow" },
  { label: "Why Neyma", href: "#why" },
  { label: "Integrations", href: "#integrations" },
];

const rankedPractices = [
  { rank: "01", name: "Willow Glen Dental", signal: "Review gap + weak implants depth", action: "Open brief" },
  { rank: "02", name: "Evergreen Smiles", signal: "Strong demand, weak conversion path", action: "Add to list" },
  { rank: "03", name: "Almaden Family Dental", signal: "Service depth under-supported", action: "Watch" },
  { rank: "04", name: "Rose Garden Dental", signal: "Booking friction on high-intent pages", action: "Open brief" },
];

const workflow = [
  {
    step: "01",
    title: "Run a territory scan",
    text: "Start with a ranked market view, not a cold list.",
  },
  {
    step: "02",
    title: "Use Ask Neyma",
    text: "Narrow the shortlist with plain-English intent.",
  },
  {
    step: "03",
    title: "Open briefs on demand",
    text: "Go deep only where the opportunity justifies it.",
  },
];

const reasons = [
  "Ranked prospects instead of undifferentiated contacts",
  "Real structural signals instead of a generic lead score",
  "Briefs, lists, and outcomes connected in one workflow",
];

const integrations = [
  "high demand",
  "weak service depth",
  "review gap",
  "booking friction",
  "implant opportunity",
  "conversion weakness",
  "dense market",
  "list ready",
];

const faqs = [
  {
    q: "What is Neyma?",
    a: "A territory-first prospecting system for agencies focused on finding the practices most worth pursuing.",
  },
  {
    q: "Who is it for today?",
    a: "Dental is the strongest vertical today. The workflow is designed to expand into adjacent local-service categories over time.",
  },
  {
    q: "What does Ask Neyma do?",
    a: "It turns a plain-English prospecting request into a tighter shortlist using the same ranking and signal system as the rest of the product.",
  },
];

export default function LandingPage() {
  const { user } = useAuth();

  return (
    <div className="min-h-screen bg-[radial-gradient(circle_at_top,#f6f0d9_0%,#ede9df_38%,#e8ece6_100%)] text-[#171411]">
      <div className="bg-[#111111] text-white">
        <div className="mx-auto flex h-12 max-w-[1280px] items-center justify-center gap-3 px-4 text-sm">
          <span className="inline-flex h-2 w-2 rounded-full bg-[#f2bf2f]" />
          <span className="text-white/70">Territory-first prospecting for agencies working local markets</span>
          <a href="#product" className="font-medium text-white">See how it works</a>
        </div>
      </div>

      <header className="px-4 pb-4 pt-6 sm:px-6">
        <div className="mx-auto flex max-w-[1280px] items-center justify-between">
          <Link href="/" className="text-[34px] font-semibold tracking-[-0.04em] text-[#171411]">
            neyma
          </Link>
          <nav className="hidden items-center gap-8 text-sm text-[#5d564c] lg:flex">
            {navItems.map((item) => (
              <a key={item.href} href={item.href} className="transition hover:text-[#171411]">
                {item.label}
              </a>
            ))}
          </nav>
          <div className="flex items-center gap-3">
            {!user && (
              <Link href="/login" className="hidden text-sm text-[#5d564c] transition hover:text-[#171411] sm:inline-flex">
                Log in
              </Link>
            )}
            <Link
              href={user ? "/dashboard" : "/register"}
              className="inline-flex h-11 items-center rounded-full bg-[#171411] px-5 text-sm font-medium text-white transition hover:opacity-90"
            >
              {user ? "Open workspace" : "Start free"}
            </Link>
          </div>
        </div>
      </header>

      <main>
        <section id="product" className="px-4 pb-8 sm:px-6 sm:pb-12">
          <div className="mx-auto max-w-[1280px] rounded-[36px] border border-black/8 bg-[#f5f2ea] p-3 sm:p-4">
            <div className="rounded-[30px] border border-black/6 bg-[linear-gradient(135deg,#f8f5ee_0%,#f7f4ea_48%,#eef3ed_100%)] p-6 sm:p-8 lg:p-10">
              <div className="grid gap-10 lg:grid-cols-[0.95fr_1.05fr]">
                <div className="max-w-[520px]">
                  <p className="text-sm text-[#7c7468]">Dentist-first today. Expanding outward carefully.</p>
                  <h1 className="mt-6 max-w-[12ch] text-5xl font-semibold leading-[0.96] tracking-[-0.055em] text-[#171411] sm:text-7xl">
                    Find the practices most worth pursuing.
                  </h1>
                  <p className="mt-6 max-w-[44ch] text-lg leading-relaxed text-[#5d564c]">
                    Neyma scans a market, ranks the strongest opportunities, narrows the shortlist with Ask Neyma, and opens full briefs only when deeper work is justified.
                  </p>
                  <div className="mt-8 flex flex-col gap-3 sm:flex-row sm:items-center">
                    <Link
                      href={user ? "/territory/new" : "/register"}
                      className="inline-flex h-12 items-center justify-center rounded-full bg-[#171411] px-6 text-sm font-medium text-white transition hover:opacity-90"
                    >
                      Run territory scan
                    </Link>
                    <Link
                      href={user ? "/ask" : "/login"}
                      className="inline-flex h-12 items-center justify-center rounded-full border border-black/10 bg-white px-6 text-sm font-medium text-[#171411] transition hover:bg-black/[0.03]"
                    >
                      Ask Neyma
                    </Link>
                  </div>
                </div>

                <div className="rounded-[28px] border border-black/6 bg-white p-4 shadow-[0_20px_60px_rgba(23,20,17,0.06)] sm:p-5">
                  <div className="grid gap-4">
                    <div className="flex flex-wrap gap-2 text-xs">
                      <span className="rounded-full bg-[#1b2432]/8 px-3 py-1 text-[#1b2432]">Territory scan</span>
                      <span className="rounded-full bg-[#f2bf2f]/16 px-3 py-1 text-[#7c6111]">Ask Neyma</span>
                      <span className="rounded-full bg-[#1f57c3]/10 px-3 py-1 text-[#1f57c3]">Brief</span>
                    </div>

                    <div className="rounded-[24px] border border-black/6 bg-[#fbfaf7] p-4">
                      <div className="flex items-center justify-between gap-3 border-b border-black/6 pb-3">
                        <div>
                          <p className="text-sm font-medium text-[#171411]">San Jose, CA</p>
                          <p className="text-xs text-[#7c7468]">Top 20 ranked prospects</p>
                        </div>
                        <span className="rounded-full bg-[#1b2432]/8 px-3 py-1 text-xs font-medium text-[#1b2432]">Market scan complete</span>
                      </div>

                      <div className="mt-3 space-y-2">
                        {rankedPractices.map((row) => (
                          <div key={row.rank} className="grid grid-cols-[36px_1fr_auto] items-center gap-3 rounded-2xl border border-black/6 bg-white px-3 py-3">
                            <span className="text-xs font-medium text-[#7c7468]">{row.rank}</span>
                            <div className="min-w-0">
                              <p className="truncate text-sm font-medium text-[#171411]">{row.name}</p>
                              <p className="truncate text-xs text-[#7c7468]">{row.signal}</p>
                            </div>
                            <span className="rounded-full border border-black/8 px-3 py-1 text-[11px] text-[#5d564c]">{row.action}</span>
                          </div>
                        ))}
                      </div>
                    </div>

                    <div className="grid gap-3 sm:grid-cols-[0.95fr_1.05fr]">
                      <div className="rounded-[24px] border border-black/6 bg-[#fbfaf7] p-4">
                        <p className="text-[11px] uppercase tracking-[0.14em] text-[#7c7468]">Ask Neyma</p>
                        <div className="mt-3 rounded-2xl border border-black/6 bg-white p-3">
                          <p className="font-mono text-[12px] text-[#171411]">
                            Find dentists with strong demand but weak service depth.
                          </p>
                        </div>
                        <div className="mt-3 space-y-2 text-xs text-[#5d564c]">
                          <div className="rounded-xl bg-white px-3 py-2">city: San Jose, CA</div>
                          <div className="rounded-xl bg-white px-3 py-2">vertical: dentist</div>
                          <div className="rounded-xl bg-white px-3 py-2">output: narrowed shortlist with reasons</div>
                        </div>
                      </div>

                      <div className="rounded-[24px] border border-black/6 bg-[#fbfaf7] p-4">
                        <p className="text-[11px] uppercase tracking-[0.14em] text-[#7c7468]">Brief snapshot</p>
                        <div className="mt-3 space-y-2">
                          <PreviewMetric label="Opportunity" value="$72k-$118k" tone="green" />
                          <PreviewMetric label="Top gap" value="Implants page missing" tone="gold" />
                          <PreviewMetric label="Constraint" value="Conversion + service depth" tone="blue" />
                          <PreviewMetric label="Next step" value="Add to list and work outreach" tone="neutral" />
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>

              <div className="mt-4 grid gap-3 sm:grid-cols-3">
                <StatCard value="40+" label="Signals per brief" />
                <StatCard value="Top 20" label="Ranked prospects returned" />
                <StatCard value="Dental" label="Focused vertical today" />
              </div>
            </div>
          </div>
        </section>

        <section id="workflow" className="px-4 py-8 sm:px-6 sm:py-10">
          <div className="mx-auto max-w-[1280px]">
            <div className="max-w-[680px]">
              <p className="text-sm text-[#7c7468]">How it works</p>
              <h2 className="mt-2 text-4xl font-semibold tracking-[-0.045em] text-[#171411] sm:text-5xl">
                Simplicity first. Context built in.
              </h2>
            </div>

            <div className="mt-8 grid gap-4 lg:grid-cols-3">
              {workflow.map((item) => (
                <article key={item.step} className="rounded-[28px] border border-black/8 bg-[#f5f2ea] p-5">
                  <p className="text-xs font-medium text-[#7c7468]">{item.step}</p>
                  <h3 className="mt-6 text-2xl font-semibold tracking-[-0.04em] text-[#171411]">{item.title}</h3>
                  <p className="mt-3 max-w-[30ch] text-sm leading-relaxed text-[#5d564c]">{item.text}</p>
                </article>
              ))}
            </div>
          </div>
        </section>

        <section id="why" className="px-4 py-8 sm:px-6 sm:py-10">
          <div className="mx-auto max-w-[1280px] rounded-[36px] bg-[#171411] px-6 py-8 text-white sm:px-8 sm:py-10">
            <div className="grid gap-8 lg:grid-cols-[0.9fr_1.1fr]">
              <div>
                <p className="text-sm text-white/50">Why Neyma</p>
                <h2 className="mt-2 max-w-[12ch] text-4xl font-semibold leading-[0.98] tracking-[-0.05em] sm:text-5xl">
                  Less noise. Better decisions.
                </h2>
              </div>
              <div className="grid gap-3">
                {reasons.map((item) => (
                  <div key={item} className="rounded-[24px] border border-white/10 bg-white/5 px-4 py-4 text-sm text-white/80">
                    {item}
                  </div>
                ))}
              </div>
            </div>
          </div>
        </section>

        <section id="integrations" className="px-4 py-8 sm:px-6 sm:py-10">
          <div className="mx-auto max-w-[1280px] rounded-[36px] border border-black/8 bg-[#f5f2ea] px-6 py-8 sm:px-8 sm:py-10">
            <div className="grid gap-6 lg:grid-cols-[0.82fr_1.18fr] lg:items-center">
              <div>
                <p className="text-sm text-[#7c7468]">Ask Neyma</p>
                <h2 className="mt-2 text-4xl font-semibold tracking-[-0.045em] text-[#171411] sm:text-5xl">
                  Less talking. More narrowing.
                </h2>
                <p className="mt-4 max-w-[42ch] text-sm leading-relaxed text-[#5d564c] sm:text-base">
                  Ask Neyma should feel like a sharp command surface. You describe the kind of practice you want, and the shortlist gets tighter around the issues that actually matter.
                </p>
                <div className="mt-6 rounded-[24px] border border-black/8 bg-white p-4">
                  <p className="font-mono text-[13px] text-[#171411]">
                    Find dentists in Austin with high demand, thin implant coverage, and weak conversion paths.
                  </p>
                </div>
              </div>
              <div className="rounded-[28px] border border-black/8 bg-white p-4 shadow-[0_20px_50px_rgba(23,20,17,0.05)]">
                <div className="flex items-center justify-between gap-3 border-b border-black/6 pb-3">
                  <div>
                    <p className="text-sm font-medium text-[#171411]">Ask output</p>
                    <p className="text-xs text-[#7c7468]">6 matches after narrowing</p>
                  </div>
                  <span className="rounded-full bg-[#1b2432]/8 px-3 py-1 text-[11px] font-medium text-[#1b2432]">Shortlist ready</span>
                </div>

                <div className="mt-4 flex flex-wrap gap-2">
                  {integrations.map((item, index) => (
                    <span
                      key={item}
                      className={`rounded-full border px-3 py-1.5 text-[12px] ${
                        index === 0 || index === 1
                          ? "border-[#1b2432]/16 bg-[#1b2432]/8 text-[#1b2432]"
                          : index === 2 || index === 3
                            ? "border-[#f2bf2f]/24 bg-[#f2bf2f]/12 text-[#7c6111]"
                            : index === 4 || index === 5
                              ? "border-[#1f57c3]/18 bg-[#1f57c3]/8 text-[#1f57c3]"
                              : "border-black/8 bg-[#fbfaf7] text-[#5d564c]"
                      }`}
                    >
                      {item}
                    </span>
                  ))}
                </div>

                <div className="mt-4 space-y-2">
                  <AskRow
                    name="Barton Creek Dental"
                    summary="Strong demand, implant gap, weak conversion path"
                    tone="green"
                  />
                  <AskRow
                    name="Westlake Family Smiles"
                    summary="Review deficit in dense market, weak service depth"
                    tone="gold"
                  />
                  <AskRow
                    name="South Lamar Dental"
                    summary="High-intent traffic, shallow page coverage"
                    tone="blue"
                  />
                </div>
              </div>
            </div>
          </div>
        </section>

        <section className="px-4 py-8 sm:px-6 sm:py-10">
          <div className="mx-auto max-w-[1280px]">
            <div className="grid gap-4 lg:grid-cols-3">
              {faqs.map((item) => (
                <article key={item.q} className="rounded-[28px] border border-black/8 bg-white px-5 py-5">
                  <h3 className="text-lg font-medium text-[#171411]">{item.q}</h3>
                  <p className="mt-3 text-sm leading-relaxed text-[#5d564c]">{item.a}</p>
                </article>
              ))}
            </div>
          </div>
        </section>
      </main>
    </div>
  );
}

function StatCard({ value, label }: { value: string; label: string }) {
  return (
    <div className="rounded-[24px] border border-black/6 bg-white px-4 py-5">
      <p className="text-3xl font-semibold tracking-[-0.04em] text-[#171411]">{value}</p>
      <p className="mt-2 text-sm text-[#7c7468]">{label}</p>
    </div>
  );
}

function PreviewMetric({
  label,
  value,
  tone,
}: {
  label: string;
  value: string;
  tone: "green" | "gold" | "blue" | "neutral";
}) {
  const toneClass =
    tone === "green"
      ? "border-[#1b2432]/16 bg-[#1b2432]/8"
      : tone === "gold"
        ? "border-[#d8b429]/24 bg-[#d8b429]/12"
        : tone === "blue"
          ? "border-[#205ecf]/16 bg-[#205ecf]/8"
          : "border-black/6 bg-white";

  return (
    <div className={`rounded-2xl border px-3 py-3 ${toneClass}`}>
      <p className="text-[11px] uppercase tracking-[0.12em] text-[#7c7468]">{label}</p>
      <p className="mt-1 text-sm font-medium text-[#171411]">{value}</p>
    </div>
  );
}

function AskRow({
  name,
  summary,
  tone,
}: {
  name: string;
  summary: string;
  tone: "green" | "gold" | "blue";
}) {
  const dotClass =
    tone === "green" ? "bg-[#1b2432]" : tone === "gold" ? "bg-[#f2bf2f]" : "bg-[#3c5b8a]";

  return (
    <div className="flex items-center gap-3 rounded-2xl border border-black/8 bg-[#fbfaf7] px-3 py-3">
      <span className={`inline-flex h-2.5 w-2.5 rounded-full ${dotClass}`} />
      <div className="min-w-0">
        <p className="truncate text-sm font-medium text-[#171411]">{name}</p>
        <p className="truncate text-xs text-[#7c7468]">{summary}</p>
      </div>
    </div>
  );
}
