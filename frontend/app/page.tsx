"use client";

import { ArrowRight, CheckCircle2, Mail } from "lucide-react";
import Link from "next/link";

import { FaqSection } from "@/components/ui/faq";
import { Footer } from "@/components/ui/footer";
import { HeroSection } from "@/components/ui/hero-section-1";
import { NeymaButton } from "@/components/ui/neyma-button";
import ShineHoverButton from "@/components/ui/shine-hover";
import { useAuth } from "@/lib/auth";

const rankedPractices = [
  {
    rank: "01",
    name: "Northside Dental Studio",
    signal: "High-Leverage",
    note: "strong demand, weak implants coverage",
  },
  {
    rank: "02",
    name: "Riverwalk Family Dental",
    signal: "High-Leverage",
    note: "high review volume, weak booking flow",
  },
  {
    rank: "03",
    name: "Summit Lane Dental",
    signal: "Moderate",
    note: "dense market, shallow service depth",
  },
];

const briefReasons = [
  { label: "Demand is there", tone: "amber" as const },
  { label: "Coverage is thin", tone: "amber" as const },
  { label: "Conversion path is active", tone: "green" as const },
];

const competitorRows = [
  { name: "Northside Dental Studio", reviews: 278, distance: "You", width: "100%" },
  { name: "Riverwalk Family Dental", reviews: 25, distance: "0.9 mi", width: "18%" },
  { name: "Summit Lane Dental", reviews: 18, distance: "1.2 mi", width: "13%" },
];

const verificationItems = [
  "Scheduling CTA found",
  "Booking flow detected",
  "Contact form present",
  "Phone prominent",
];

const landingMenuItems = [
  { name: "Run Territory Scan", href: "/territory/new" },
  { name: "Open Workspace", href: "/dashboard" },
  { name: "Build Brief", href: "/diagnostic/new" },
  { name: "Ask Neyma", href: "/ask" },
];

const askSteps = [
  "Start with a city and a state.",
  "Describe the type of practice or gap you want.",
  "Neyma returns a ranked list you can work from.",
];

const landingFaqs = [
  {
    question: "Does Neyma only work for dental right now?",
    answer:
      "Yes. Neyma is focused on dental practices first. The workflow is designed to expand later, but the product story and ranking system are built around dental today.",
  },
  {
    question: "Do I need a practice name to start?",
    answer:
      "No. Territory Scan starts with the market. Drop in a city, state, and specialty focus, and Neyma returns ranked practices for you.",
  },
  {
    question: "Where does Ask Neyma fit?",
    answer:
      "Ask Neyma turns a plain-English request into ranked output. Use it when you know the kind of practice, service gap, or opportunity you want to target.",
  },
  {
    question: "Can I build a brief without scanning a market first?",
    answer:
      "Yes. If you already know the practice, you can go straight to Build Brief and generate the intelligence view directly.",
  },
];

export default function LandingPage() {
  const { user } = useAuth();

  return (
    <div className="theme-light min-h-screen bg-[radial-gradient(circle_at_top,rgba(139,80,212,0.14)_0%,#ffffff_34%,#ffffff_100%)] text-[#0a0a0a]">
      <main>
        <section id="product" className="page-section-tight">
          <HeroSection
            badge="AI-ranked prospect intelligence"
            menuItems={landingMenuItems}
            title={
              <>
                <span className="block">Name the market.</span>
                <span className="block">We’ll find and rank the leads.</span>
              </>
            }
            description={
              <>
                <span className="block">No practice name needed. Neyma finds them for you.</span>
                <span className="mt-2 block text-[15px] text-[#7A7A7A]">
                  Built for dental practices first. Expanding beyond that later.
                </span>
              </>
            }
            primaryCta={{
              label: "Run Territory Scan",
              href: "/territory/new",
            }}
            secondaryCta={{
              label: "Open Workspace",
              href: user ? "/dashboard" : "/login",
            }}
          >
            <TerritoryHeroPreview />
          </HeroSection>
        </section>

        <section className="page-section pt-8">
          <div className="app-container">
            <div className="mx-auto max-w-[660px] text-center">
              <p className="section-kicker">The brief</p>
              <h2 className="section-title mt-2 text-black">
                Click a lead. Get the full picture.
              </h2>
              <p className="mt-4 text-[16px] leading-7 text-[var(--text-secondary)] sm:text-[18px]">
                The scan gets you the list. The brief gets you the pitch.
              </p>
            </div>

            <div className="mx-auto mt-10 max-w-[980px]">
              <BriefSectionPreview signedIn={Boolean(user)} />
            </div>
          </div>
        </section>

        <section id="integrations" className="page-section">
          <div className="app-container">
            <div className="grid gap-8 rounded-[24px] border border-[var(--border)] bg-[var(--surface)] p-6 shadow-[0_10px_30px_rgba(0,0,0,0.05)] lg:grid-cols-[0.88fr_1.12fr] lg:p-8">
              <div>
                <p className="section-kicker">Ask Neyma</p>
                <h2 className="section-title mt-2 max-w-[13ch] text-black">
                  Tell Neyma what you want. Get a ranked list back.
                </h2>
                <p className="mt-4 max-w-[42ch] text-[16px] leading-7 text-[var(--text-secondary)]">
                  Ask Neyma turns a plain-English request into ranked output. Give it a city, a state, and the kind of practice, service gap, or opportunity you care about. Then build the brief from the names that come back.
                </p>
                <div className="mt-8 space-y-3">
                  {askSteps.map((step, index) => (
                    <div
                      key={step}
                      className="flex gap-4 rounded-[16px] border border-[var(--border)] bg-white px-4 py-4 shadow-[0_1px_3px_rgba(0,0,0,0.05)]"
                    >
                      <span className="inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-[#F5EEFC] text-sm font-medium text-[#8B50D4]">
                        0{index + 1}
                      </span>
                      <p className="text-sm leading-6 text-[var(--text-primary)]">{step}</p>
                    </div>
                  ))}
                </div>
              </div>

              <AskRefinePreview signedIn={Boolean(user)} />
            </div>
          </div>
        </section>

        <section id="workflow" className="page-section pt-0">
          <div className="app-container">
            <div className="mx-auto max-w-[760px] text-center">
              <p className="section-kicker">Build one brief</p>
              <h2 className="section-title mt-2 text-black">
                Already have the practice? Start there.
              </h2>
              <p className="mt-4 text-[16px] leading-7 text-[var(--text-secondary)] sm:text-[18px]">
                Neyma does not have to start with a market. If you already know the account, just enter the practice details and generate the brief directly.
              </p>
            </div>
            <div className="mx-auto mt-10 max-w-[920px]">
              <SingleBriefInputPreview signedIn={Boolean(user)} />
            </div>
          </div>
        </section>

        <FaqSection
          title="Questions, answered."
          description="The essentials on how Neyma works today."
          items={landingFaqs}
          contactInfo={{
            title: "Still have questions?",
            description: "Reach out directly and we’ll help.",
            buttonText: "Contact",
            onContact: () => {
              window.location.href = "mailto:rasheed@tryneyma.com";
            },
          }}
        />

        <section className="mt-10 bg-[#151122] px-6 py-16 sm:px-10 sm:py-20">
          <div className="mx-auto max-w-[760px] text-center">
            <p className="section-kicker text-white/65">The next move</p>
            <h2
              className="mt-3 text-[48px] font-medium leading-[1.05] tracking-[-0.04em] sm:text-[64px]"
              style={{ color: "#ffffff" }}
            >
              Start with the market.
              <span className="block">Follow the signal.</span>
            </h2>
            <p className="mx-auto mt-5 max-w-[620px] text-[16px] leading-7 text-white/70 sm:text-[18px]">
              Run a scan, see what rises to the top, and open the brief only when it is worth the deeper work.
            </p>
            <div className="mt-8 flex flex-wrap items-center justify-center gap-4">
              <ShineHoverButton asChild>
                <Link href="/territory/new">Run Territory Scan</Link>
              </ShineHoverButton>
              <NeymaButton
                asChild
                variant="secondary"
                className="border-white/12 bg-white text-[#0A0A0A] hover:bg-white/92"
              >
                <Link href="/diagnostic/new">
                  <span className="text-[#0A0A0A]">Build Brief</span>
                </Link>
              </NeymaButton>
            </div>
          </div>

          <div className="app-container mt-16">
            <Footer
              theme="dark"
              className="pb-0 pt-0"
              logo={null}
              brandName="neyma"
              socialLinks={[
                {
                  icon: <Mail className="h-5 w-5" />,
                  href: "mailto:rasheed@tryneyma.com",
                  label: "Email",
                },
              ]}
              mainLinks={[
                { href: "/territory/new", label: "Run Territory Scan" },
                { href: "/dashboard", label: "Open Workspace" },
                { href: "/diagnostic/new", label: "Build Brief" },
                { href: "/ask", label: "Ask Neyma" },
              ]}
              legalLinks={[
                { href: "/login", label: "Log in" },
                { href: "/register", label: "Sign up" },
              ]}
              copyright={{
                text: "© 2026 Neyma",
                license: "Built for dental practices first.",
              }}
            />
          </div>
        </section>
      </main>
    </div>
  );
}

function TerritoryHeroPreview() {
  return (
    <div className="overflow-hidden rounded-[26px] border border-[#E6E6E6] bg-white p-4 shadow-[0_25px_60px_rgba(0,0,0,0.08)] sm:p-6">
      <div className="space-y-5">
        <div className="grid gap-3 lg:grid-cols-[1fr_1fr_1.08fr_auto]">
          <ScanField label="City" value="Austin" />
          <ScanField label="State" value="TX" />
          <ScanField label="Specialty focus" value="Dental implants" />
          <div className="flex items-end">
            <ShineHoverButton asChild className="w-full justify-center">
              <Link href="/territory/new">Run scan</Link>
            </ShineHoverButton>
          </div>
        </div>

        <div className="flex items-center justify-between gap-3 rounded-[16px] border border-[#E6E6E6] bg-[#F8F8FB] px-4 py-3">
          <div>
            <p className="text-[11px] font-medium uppercase tracking-[0.14em] text-[#6B6B6B]">
              Territory results
            </p>
            <p className="mt-1 text-sm text-[#0A0A0A]">Machine learning ranks the market before you open a single brief.</p>
          </div>
          <span className="rounded-full border border-[#E7D8FB] bg-[#F5EEFC] px-3 py-1 text-[11px] font-medium uppercase tracking-[0.12em] text-[#8B50D4]">
            top 20 returned
          </span>
        </div>

        <div className="space-y-3">
          {rankedPractices.map((practice) => (
            <div
              key={practice.rank}
              className="grid gap-3 rounded-[16px] border border-[#E6E6E6] bg-white px-4 py-4 shadow-[0_1px_3px_rgba(0,0,0,0.05)] sm:grid-cols-[auto_1fr_auto]"
            >
              <div className="flex items-center gap-3">
                <span className="text-[11px] font-medium uppercase tracking-[0.14em] text-[#6B6B6B]">{practice.rank}</span>
                <span className="rounded-full border border-[#E6E6E6] bg-[#F8F8FB] px-3 py-1 text-[11px] font-medium uppercase tracking-[0.12em] text-[#0A0A0A]">
                  {practice.signal}
                </span>
              </div>
              <div>
                <p className="text-[18px] font-medium tracking-[-0.02em] text-[#0A0A0A]">{practice.name}</p>
                <p className="mt-1 text-sm text-[#6B6B6B]">{practice.note}</p>
              </div>
              <div className="flex items-center gap-2 sm:justify-end">
                <span className="rounded-full border border-[#E7D8FB] bg-[#F5EEFC] px-3 py-1 text-[11px] font-medium uppercase tracking-[0.12em] text-[#8B50D4]">
                  ranked
                </span>
                <span className="rounded-full border border-[#E6E6E6] bg-white px-3 py-1 text-[11px] font-medium uppercase tracking-[0.12em] text-[#6B6B6B]">
                  build brief
                </span>
              </div>
            </div>
          ))}
        </div>

        <div className="rounded-[18px] border border-[#E6E6E6] bg-[#F8F8FB] px-4 py-5 text-center">
          <p className="text-sm font-medium text-[#0A0A0A]">Sign up to get your ranked list</p>
          <div className="mt-2 flex justify-center">
            <ArrowRight className="h-4 w-4 rotate-90 text-[#8B50D4]" />
          </div>
          <div className="mt-3 flex justify-center">
            <ShineHoverButton asChild>
              <Link href="/territory/new">Run scan</Link>
            </ShineHoverButton>
          </div>
        </div>
      </div>
    </div>
  );
}

function BriefSectionPreview({ signedIn }: { signedIn: boolean }) {
  return (
    <div className="relative overflow-hidden rounded-[24px] border border-[#E6E6E6] bg-white p-4 shadow-[0_25px_60px_rgba(0,0,0,0.08)] sm:p-6">
      <div className="space-y-5">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <div className="flex flex-wrap items-center gap-2">
              <span className="rounded-full border border-[#BFE9D2] bg-[#EAFBF2] px-3 py-1 text-[11px] font-medium uppercase tracking-[0.12em] text-[#15803D]">
                Qualified lead
              </span>
              <span className="rounded-full border border-[#E6E6E6] bg-[#F8F8FB] px-3 py-1 text-[11px] font-medium uppercase tracking-[0.12em] text-[#0A0A0A]">
                High-leverage
              </span>
            </div>
            <h3 className="mt-4 text-[32px] font-medium tracking-[-0.03em] text-[#0A0A0A] sm:text-[40px]">
              Northside Dental Studio
            </h3>
            <div className="mt-2 flex flex-wrap items-center gap-2 text-sm text-[#6B6B6B]">
              <span>Austin, TX</span>
              <span>·</span>
              <span className="font-medium text-[#0A0A0A]">northsidedentalstudio.com</span>
            </div>
          </div>
          <div className="flex flex-wrap gap-2">
            <button
              type="button"
              className="rounded-[10px] border border-[#E6E6E6] bg-white px-4 py-2.5 text-sm font-medium text-[#0A0A0A]"
            >
              Log outreach
            </button>
            <button
              type="button"
              className="rounded-[10px] bg-[#8B50D4] px-4 py-2.5 text-sm font-medium text-white shadow-sm"
            >
              Add to pipeline
            </button>
          </div>
        </div>

        <div className="grid gap-2 lg:grid-cols-5">
          {[
            ["Reviews", "278"],
            ["Review delta", "+253 vs nearest"],
            ["Paid ads", "Active"],
            ["Market density", "High"],
            ["Geo coverage", "80% · 24/30 pages"],
          ].map(([label, value]) => (
            <div key={label} className="rounded-[14px] bg-[#F8F8FB] px-4 py-3">
              <p className="text-[10px] font-medium uppercase tracking-[0.14em] text-[#6B6B6B]">{label}</p>
              <p className="mt-1 text-sm font-medium text-[#0A0A0A]">{value}</p>
            </div>
          ))}
        </div>

        <div className="rounded-[16px] border border-[#F3D39A] bg-[#FFF7E8] px-4 py-4">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <p className="text-[11px] font-medium uppercase tracking-[0.14em] text-[#9A6700]">Top gap</p>
              <p className="mt-1 text-sm font-medium text-[#0A0A0A]">No dedicated implants page found</p>
            </div>
            <span className="rounded-full border border-[#F4D9A7] bg-white/80 px-3 py-1 text-[11px] font-medium uppercase tracking-[0.12em] text-[#9A6700]">
              Implant capture gap
            </span>
          </div>
        </div>

        <div className="grid gap-4 lg:grid-cols-2">
          <div className="rounded-[16px] border border-[#E6E6E6] bg-[#F8F8FB] px-4 py-4">
            <p className="text-[11px] font-medium uppercase tracking-[0.14em] text-[#6B6B6B]">Why now</p>
            <div className="mt-4 space-y-3">
              {briefReasons.map((reason) => (
                <div key={reason.label} className="flex items-center gap-3">
                  <span className={`inline-flex h-2.5 w-2.5 rounded-full ${reason.tone === "amber" ? "bg-[#D97706]" : "bg-[#16A34A]"}`} />
                  <p className="text-sm text-[#0A0A0A]">{reason.label}</p>
                </div>
              ))}
            </div>
          </div>

          <div className="rounded-[16px] border border-[#E6E6E6] bg-[#F8F8FB] px-4 py-4">
            <p className="text-[11px] font-medium uppercase tracking-[0.14em] text-[#6B6B6B]">Competitors nearby</p>
            <div className="mt-4 space-y-3">
              {competitorRows.map((row) => (
                <div key={row.name}>
                  <div className="flex items-center justify-between gap-3 text-sm text-[#0A0A0A]">
                    <span className="truncate">{row.name}</span>
                    <span className="shrink-0 text-[#6B6B6B]">{row.reviews} · {row.distance}</span>
                  </div>
                  <div className="mt-2 h-2 rounded-full bg-white">
                    <div className="h-2 rounded-full bg-[#8B50D4]" style={{ width: row.width }} />
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        <div className="grid gap-4 lg:grid-cols-2">
          <div className="rounded-[16px] border border-[#E6E6E6] bg-[#F8F8FB] px-4 py-4">
            <p className="text-[11px] font-medium uppercase tracking-[0.14em] text-[#6B6B6B]">Capture verification</p>
            <div className="mt-4 space-y-3">
              {verificationItems.map((item) => (
                <div key={item} className="flex items-center gap-3 text-sm text-[#0A0A0A]">
                  <CheckCircle2 className="h-4 w-4 text-[#16A34A]" />
                  <span>{item}</span>
                </div>
              ))}
            </div>
          </div>

          <div className="rounded-[16px] border border-[#E6E6E6] bg-[#F8F8FB] px-4 py-4">
            <p className="text-[11px] font-medium uppercase tracking-[0.14em] text-[#6B6B6B]">Recommendation</p>
            <p className="mt-4 text-sm leading-7 text-[#0A0A0A]">
              Strong local demand is already in the market. The site still leaves implants demand under-served, which makes this a clean outreach angle.
            </p>
            <div className="mt-5 flex flex-wrap gap-2">
              <NeymaButton asChild variant="secondary">
                <Link href={signedIn ? "/ask" : "/register"}>Draft outreach</Link>
              </NeymaButton>
              <ShineHoverButton asChild>
                <Link href="/diagnostic/new">Full audit</Link>
              </ShineHoverButton>
            </div>
          </div>
        </div>
      </div>

      <div className="pointer-events-none absolute inset-x-0 bottom-0 h-40 bg-gradient-to-b from-transparent via-white/88 to-white" />
      <div className="absolute inset-x-0 bottom-0 flex justify-center px-6 pb-6">
        <Link href="/register" className="inline-flex items-center gap-2 rounded-full bg-white px-4 py-2 text-sm font-medium text-[#8B50D4] shadow-[0_10px_30px_rgba(0,0,0,0.08)]">
          Sign up to see the full brief
          <ArrowRight className="h-4 w-4" />
        </Link>
      </div>
    </div>
  );
}

function AskRefinePreview({ signedIn }: { signedIn: boolean }) {
  return (
    <div className="overflow-hidden rounded-[20px] border border-[#E6E6E6] bg-white shadow-[0_12px_30px_rgba(0,0,0,0.05)]">
      <div className="flex items-center justify-between border-b border-[#E6E6E6] px-4 py-3">
        <div>
          <p className="text-[11px] uppercase tracking-[0.14em] text-[#6B6B6B]">Ask Neyma</p>
          <p className="mt-1 text-sm font-medium text-[#0A0A0A]">Describe the market and the kind of lead you want</p>
        </div>
        <span className="rounded-full border border-[#E7D8FB] bg-[#F5EEFC] px-3 py-1 text-[11px] font-medium uppercase tracking-[0.12em] text-[#8B50D4]">
          ranked output
        </span>
      </div>

      <div className="space-y-4 p-4">
        <div className="rounded-[14px] border border-[#E6E6E6] bg-[#F8F8FB] p-4">
          <p className="font-mono text-[12px] leading-6 text-[#0A0A0A]">
            Austin, TX. Find dental practices with active demand, thin implants coverage, and weak conversion paths.
          </p>
        </div>

        <div className="flex flex-wrap gap-2">
          {["city: Austin", "state: TX", "dental", "implant gap", "weak conversion"].map((chip) => (
            <span
              key={chip}
              className="rounded-full border border-[#E6E6E6] bg-white px-3 py-1.5 text-[11px] text-[#6B6B6B]"
            >
              {chip}
            </span>
          ))}
        </div>

        <div className="space-y-2">
          {[
            ["Northside Dental Studio", "Best fit for the request: demand is present, implants depth is thin, and the path still leaks intent."],
            ["Riverwalk Family Dental", "Strong local visibility, but the service gap is weaker than the lead above."],
            ["West Harbor Dental", "Worth a brief if you want another implants angle after the top-ranked account."],
          ].map(([name, summary]) => (
            <div key={name} className="rounded-[14px] border border-[#E6E6E6] bg-[#F8F8FB] px-4 py-3">
              <p className="text-sm font-medium text-[#0A0A0A]">{name}</p>
              <p className="mt-1 text-xs leading-5 text-[#6B6B6B]">{summary}</p>
            </div>
          ))}
        </div>

        <div className="flex justify-end">
          <ShineHoverButton asChild>
            <Link href={signedIn ? "/ask" : "/register"}>Ask Neyma</Link>
          </ShineHoverButton>
        </div>
      </div>
    </div>
  );
}

function SingleBriefInputPreview({ signedIn }: { signedIn: boolean }) {
  return (
    <div className="overflow-hidden rounded-[24px] border border-[#E6E6E6] bg-white p-4 shadow-[0_25px_60px_rgba(0,0,0,0.08)] sm:p-6">
      <div className="grid gap-6 lg:grid-cols-[0.88fr_1.12fr]">
        <div>
          <p className="text-[11px] font-medium uppercase tracking-[0.14em] text-[#6B6B6B]">Direct brief input</p>
          <h3 className="mt-3 text-[30px] font-medium tracking-[-0.03em] text-[#0A0A0A] sm:text-[36px]">
            One practice in. One brief out.
          </h3>
          <p className="mt-4 max-w-[42ch] text-sm leading-7 text-[#6B6B6B] sm:text-base">
            If you already know the practice, enter the business details and let Neyma build the brief without running a territory scan first.
          </p>
          <div className="mt-6 flex flex-wrap gap-2">
            <ShineHoverButton asChild>
              <Link href="/diagnostic/new">Generate Brief</Link>
            </ShineHoverButton>
            <NeymaButton asChild variant="secondary" className="text-[#0A0A0A]">
              <Link href={signedIn ? "/dashboard" : "/login"}>
                <span className="text-[#0A0A0A]">Open Workspace</span>
              </Link>
            </NeymaButton>
          </div>
        </div>

        <div className="grid gap-3">
          <ScanField label="Practice name" value="Northside Dental Studio" />
          <div className="grid gap-3 sm:grid-cols-2">
            <ScanField label="City" value="Austin" />
            <ScanField label="State" value="TX" />
          </div>
          <ScanField label="Website" value="northsidedentalstudio.com" />
          <div className="rounded-[16px] border border-[#E6E6E6] bg-[#F8F8FB] px-4 py-4">
            <p className="text-[11px] font-medium uppercase tracking-[0.14em] text-[#6B6B6B]">Required elements</p>
            <div className="mt-3 flex flex-wrap gap-2">
              {["practice", "city", "state"].map((item) => (
                <span
                  key={item}
                  className="rounded-full border border-[#E6E6E6] bg-white px-3 py-1.5 text-[11px] font-medium uppercase tracking-[0.12em] text-[#0A0A0A]"
                >
                  {item}
                </span>
              ))}
              <span className="rounded-full border border-[#E7D8FB] bg-[#F5EEFC] px-3 py-1.5 text-[11px] font-medium uppercase tracking-[0.12em] text-[#8B50D4]">
                website optional
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function ScanField({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-[14px] border border-[#E6E6E6] bg-[#F8F8FB] px-4 py-3">
      <p className="text-[10px] font-medium uppercase tracking-[0.14em] text-[#6B6B6B]">{label}</p>
      <p className="mt-1 text-sm font-medium text-[#0A0A0A]">{value}</p>
    </div>
  );
}
