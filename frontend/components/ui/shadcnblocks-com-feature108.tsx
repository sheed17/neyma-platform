/* eslint-disable @next/next/no-img-element */
"use client";

import * as Tabs from "@radix-ui/react-tabs";
import Link from "next/link";
import { FileText, Layout, Sparkles } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { NeymaButton } from "@/components/ui/neyma-button";

interface TabContent {
  badge: string;
  title: string;
  description: string;
  buttonText: string;
  buttonHref: string;
  preview?: React.ReactNode;
  imageSrc?: string;
  imageAlt?: string;
}

interface Tab {
  value: string;
  icon: React.ReactNode;
  label: string;
  content: TabContent;
}

interface Feature108Props {
  badge?: string;
  heading?: string;
  description?: string;
  tabs?: Tab[];
}

const defaultTabs: Tab[] = [
  {
    value: "territory-scan",
    icon: <Layout className="h-4 w-4 shrink-0" />,
    label: "Territory Scan",
    content: {
      badge: "Market Discovery",
      title: "Start with ranked markets instead of cold lists.",
      description:
        "Run territory scans to surface the strongest practices by demand, service depth, and conversion risk before you spend time on outreach.",
      buttonText: "Run Territory Scan",
      buttonHref: "/territory/new",
    },
  },
  {
    value: "build-brief",
    icon: <FileText className="h-4 w-4 shrink-0" />,
    label: "Build Brief",
    content: {
      badge: "Revenue Intelligence",
      title: "Open deep briefs only when the opportunity justifies it.",
      description:
        "Turn shortlist candidates into high-signal briefs with commercial context, page gaps, offer coverage, and outreach-ready talking points.",
      buttonText: "Generate Brief",
      buttonHref: "/diagnostic/new",
    },
  },
  {
    value: "ask-neyma",
    icon: <Sparkles className="h-4 w-4 shrink-0" />,
    label: "Ask Neyma",
    content: {
      badge: "AI Narrowing",
      title: "Use plain-English prompts to tighten the shortlist.",
      description:
        "Ask Neyma to narrow candidates by intent, service gaps, market density, or revenue potential so the next action is immediately obvious.",
      buttonText: "Ask Neyma",
      buttonHref: "/ask",
    },
  },
];

function Feature108({
  badge = "Workflow",
  heading = "Move from scan to shortlist to brief in one flow.",
  description = "Territory Scan, Build Brief, and Ask Neyma work together as one decision system instead of disconnected tools.",
  tabs = defaultTabs,
}: Feature108Props) {
  return (
    <section className="py-24">
      <div className="app-container">
        <div className="flex flex-col items-center gap-4 text-center">
          <Badge
            variant="outline"
            className="rounded-full border-[#E6E6E6] bg-white px-3 py-1 text-[11px] font-medium uppercase tracking-[0.12em] text-[#6B6B6B]"
          >
            {badge}
          </Badge>
          <h2 className="max-w-[720px] text-[36px] font-medium tracking-[-0.03em] text-[#0A0A0A] sm:text-[44px]">
            {heading}
          </h2>
          <p className="max-w-[620px] text-[16px] leading-7 text-[#6B6B6B] sm:text-[18px]">
            {description}
          </p>
        </div>

        <Tabs.Root defaultValue={tabs[0]?.value} className="mt-10">
          <Tabs.List className="mx-auto flex w-full max-w-[760px] flex-col gap-3 rounded-[20px] border border-[#E6E6E6] bg-white p-3 shadow-[0_12px_30px_rgba(0,0,0,0.05)] sm:flex-row">
            {tabs.map((tab) => (
              <Tabs.Trigger
                key={tab.value}
                value={tab.value}
                className="flex flex-1 items-center justify-center gap-2 rounded-[14px] px-4 py-3 text-sm font-medium text-[#6B6B6B] transition data-[state=active]:bg-[#F3EBFF] data-[state=active]:text-[#8B50D4]"
              >
                {tab.icon}
                {tab.label}
              </Tabs.Trigger>
            ))}
          </Tabs.List>

          <div className="mt-8">
            {tabs.map((tab) => (
              <Tabs.Content key={tab.value} value={tab.value}>
                <div className="grid gap-6 rounded-[24px] border border-[#E6E6E6] bg-white p-6 shadow-[0_25px_60px_rgba(0,0,0,0.08)] lg:grid-cols-[0.92fr_1.08fr] lg:items-center lg:p-8">
                  <div className="flex flex-col gap-5">
                    <Badge
                      variant="outline"
                      className="w-fit rounded-full border-[#E7D8FB] bg-[#F8F3FE] px-3 py-1 text-[11px] font-medium uppercase tracking-[0.12em] text-[#8B50D4]"
                    >
                      {tab.content.badge}
                    </Badge>
                    <h3 className="max-w-[16ch] text-[32px] font-medium tracking-[-0.03em] text-[#0A0A0A] sm:text-[40px]">
                      {tab.content.title}
                    </h3>
                    <p className="max-w-[48ch] text-[16px] leading-7 text-[#6B6B6B] sm:text-[18px]">
                      {tab.content.description}
                    </p>
                    <NeymaButton asChild variant="primary" className="mt-2 w-fit">
                      <Link href={tab.content.buttonHref}>{tab.content.buttonText}</Link>
                    </NeymaButton>
                  </div>

                  <div className="relative overflow-hidden rounded-[20px] border border-[#E6E6E6] bg-[linear-gradient(180deg,#FCFAFF_0%,#F7F2FD_100%)] p-3">
                    <div
                      aria-hidden="true"
                      className="pointer-events-none absolute inset-x-10 top-0 h-24 rounded-full bg-[radial-gradient(circle,rgba(139,80,212,0.18)_0%,rgba(139,80,212,0)_72%)] blur-2xl"
                    />
                    {tab.content.preview ? (
                      <div className="relative z-[1]">{tab.content.preview}</div>
                    ) : tab.content.imageSrc ? (
                      <img
                        src={tab.content.imageSrc}
                        alt={tab.content.imageAlt || tab.content.title}
                        className="relative z-[1] aspect-[16/10] w-full rounded-[16px] object-cover"
                      />
                    ) : null}
                  </div>
                </div>
              </Tabs.Content>
            ))}
          </div>
        </Tabs.Root>
      </div>
    </section>
  );
}

export { Feature108 };
