/* eslint-disable @next/next/no-img-element */
"use client";

import React from "react";
import Link from "next/link";
import { ChevronRight, Menu, X } from "lucide-react";
import type { Variants } from "framer-motion";

import { AnimatedGroup } from "@/components/ui/animated-group";
import { Button } from "@/components/ui/button";
import { NeymaButton } from "@/components/ui/neyma-button";
import ShineHoverButton from "@/components/ui/shine-hover";
import { cn } from "@/lib/utils";

type NavItem = {
  name: string;
  href: string;
};

type HeroSectionProps = {
  badge?: string;
  title?: React.ReactNode;
  description?: React.ReactNode;
  primaryCta?: {
    label: string;
    href: string;
  };
  secondaryCta?: {
    label: string;
    href: string;
  };
  menuItems?: NavItem[];
  showBrandStrip?: boolean;
  children?: React.ReactNode;
};

const transitionVariants: { item: Variants } = {
  item: {
    hidden: {
      opacity: 0,
      filter: "blur(12px)",
      y: 12,
    },
    visible: {
      opacity: 1,
      filter: "blur(0px)",
      y: 0,
      transition: {
        type: "spring" as const,
        bounce: 0.3,
        duration: 1.5,
      },
    },
  },
};

const defaultMenuItems: NavItem[] = [
  { name: "Product", href: "#product" },
  { name: "Workflow", href: "#workflow" },
  { name: "Why Neyma", href: "#why" },
  { name: "Integrations", href: "#integrations" },
];

const customerLogos = [
  {
    alt: "Nvidia Logo",
    className: "h-5",
    src: "https://html.tailus.io/blocks/customers/nvidia.svg",
  },
  {
    alt: "GitHub Logo",
    className: "h-4",
    src: "https://html.tailus.io/blocks/customers/github.svg",
  },
  {
    alt: "Laravel Logo",
    className: "h-4",
    src: "https://html.tailus.io/blocks/customers/laravel.svg",
  },
  {
    alt: "OpenAI Logo",
    className: "h-6",
    src: "https://html.tailus.io/blocks/customers/openai.svg",
  },
];

export function HeroSection({
  badge = "AI-ranked for dental today",
  title = "Let AI surface the practices most worth pursuing",
  description = "Neyma scans a market, ranks the strongest opportunities with machine learning, narrows the shortlist with AI reasoning, and opens briefs only where deeper work is justified.",
  primaryCta = {
    label: "Start free",
    href: "/register",
  },
  secondaryCta = {
    label: "Log in",
    href: "/login",
  },
  menuItems = defaultMenuItems,
  showBrandStrip = false,
  children,
}: HeroSectionProps) {
  void badge;

  return (
    <>
      <HeroHeader menuItems={menuItems} />
      <main className="overflow-hidden">
        <section id="product">
          <div className="relative px-6 py-[120px]">
            <div
              aria-hidden
              className="absolute inset-0 -z-10 bg-[radial-gradient(circle_at_top,#F3EBFF_0%,#FFFFFF_68%)]"
            />

            <div className="mx-auto max-w-[1200px]">
              <div className="flex flex-col items-center text-center">
                <AnimatedGroup variants={transitionVariants}>
                  <p className="mx-auto mb-5 text-[11px] font-medium uppercase tracking-[0.18em] text-[#6B6B6B]">
                    {badge}
                  </p>
                  <h1 className="mx-auto max-w-[720px] text-[56px] font-semibold leading-[1.1] tracking-[-0.02em] text-[#0A0A0A] md:text-[64px]">
                    {title}
                  </h1>

                  <p className="mx-auto mt-6 max-w-[600px] text-[18px] leading-[1.6] text-[#6B6B6B]">
                    {description}
                  </p>
                </AnimatedGroup>

                <AnimatedGroup
                  variants={{
                    container: {
                      visible: {
                        transition: {
                          staggerChildren: 0.05,
                          delayChildren: 0.75,
                        },
                      },
                    },
                    ...transitionVariants,
                  }}
                  className="mt-8 flex flex-wrap justify-center gap-4"
                >
                  <ShineHoverButton
                    asChild
                    className="min-h-12"
                  >
                    <Link href={primaryCta.href}>
                      <span className="text-nowrap">{primaryCta.label}</span>
                    </Link>
                  </ShineHoverButton>
                  <NeymaButton
                    asChild
                    variant="secondary"
                    className="min-h-12 border-[#E7D8FB] bg-[#F5EEFC] text-[#6F42C1] hover:bg-[#EFE4FD]"
                  >
                    <Link href={secondaryCta.href}>
                      <span className="text-nowrap text-[#6F42C1]">{secondaryCta.label}</span>
                    </Link>
                  </NeymaButton>
                </AnimatedGroup>
              </div>
            </div>

            <AnimatedGroup
              variants={{
                container: {
                  visible: {
                    transition: {
                      staggerChildren: 0.05,
                      delayChildren: 0.75,
                    },
                  },
                },
                ...transitionVariants,
              }}
            >
              {children ? (
                <div className="relative mt-16">
                  <div className="relative mx-auto max-w-[900px]">{children}</div>
                </div>
              ) : null}
            </AnimatedGroup>
          </div>
        </section>

        {showBrandStrip ? (
          <section className="bg-background pb-16 pt-16 md:pb-32">
            <div className="group relative m-auto max-w-5xl px-6">
              <div className="absolute inset-0 z-10 flex scale-95 items-center justify-center opacity-0 duration-500 group-hover:scale-100 group-hover:opacity-100">
                <Link href="/" className="block text-sm duration-150 hover:opacity-75">
                  <span>Meet Our Customers</span>
                  <ChevronRight className="ml-1 inline-block size-3" />
                </Link>
              </div>

              <div className="mx-auto mt-12 grid max-w-2xl grid-cols-2 gap-x-12 gap-y-8 transition-all duration-500 group-hover:blur-xs group-hover:opacity-50 sm:grid-cols-4 sm:gap-x-16 sm:gap-y-14">
                {customerLogos.map((logo) => (
                  <div key={logo.alt} className="flex">
                    <img
                      className={cn("mx-auto w-fit dark:invert", logo.className)}
                      src={logo.src}
                      alt={logo.alt}
                      width="auto"
                      height="24"
                    />
                  </div>
                ))}
              </div>
            </div>
          </section>
        ) : null}
      </main>
    </>
  );
}

function HeroHeader({ menuItems }: { menuItems: NavItem[] }) {
  const [menuState, setMenuState] = React.useState(false);
  const [isScrolled, setIsScrolled] = React.useState(false);

  React.useEffect(() => {
    const handleScroll = () => {
      setIsScrolled(window.scrollY > 40);
    };

    window.addEventListener("scroll", handleScroll);

    return () => window.removeEventListener("scroll", handleScroll);
  }, []);

  return (
    <header>
      <nav data-state={menuState && "active"} className="group fixed z-20 w-full px-2">
        <div
          className={cn(
            "mx-auto mt-2 max-w-6xl px-6 transition-all duration-300 lg:px-12",
            isScrolled && "max-w-4xl rounded-[16px] border border-[var(--border)] bg-white/90 backdrop-blur-lg lg:px-5"
          )}
        >
          <div className="relative flex flex-wrap items-center justify-between gap-6 py-3 lg:gap-0 lg:py-4">
            <div className="flex w-full justify-between lg:w-auto">
              <Link href="/" aria-label="home" className="flex items-center space-x-2">
                <Logo />
              </Link>

              <button
                onClick={() => setMenuState(!menuState)}
                aria-label={menuState ? "Close Menu" : "Open Menu"}
                className="relative z-20 -m-2.5 -mr-4 block cursor-pointer p-2.5 lg:hidden"
              >
                <Menu className="m-auto size-6 duration-200 group-data-[state=active]:rotate-180 group-data-[state=active]:scale-0 group-data-[state=active]:opacity-0" />
                <X className="absolute inset-0 m-auto size-6 -rotate-180 scale-0 opacity-0 duration-200 group-data-[state=active]:rotate-0 group-data-[state=active]:scale-100 group-data-[state=active]:opacity-100" />
              </button>
            </div>

            <div className="absolute inset-0 m-auto hidden size-fit lg:block">
              <ul className="flex gap-8 text-sm">
                {menuItems.map((item) => (
                  <li key={item.name}>
                    <Link
                      href={item.href}
                      className="block text-[var(--text-secondary)] duration-150 hover:text-[var(--text-primary)]"
                    >
                      <span>{item.name}</span>
                    </Link>
                  </li>
                ))}
              </ul>
            </div>

            <div className="mb-6 hidden w-full flex-wrap items-center justify-end space-y-8 rounded-3xl border border-[var(--border)] bg-white p-6 shadow-[0_12px_30px_rgba(0,0,0,0.05)] group-data-[state=active]:block md:flex-nowrap lg:m-0 lg:flex lg:w-fit lg:gap-6 lg:space-y-0 lg:border-transparent lg:bg-transparent lg:p-0 lg:shadow-none">
              <div className="lg:hidden">
                <ul className="space-y-6 text-base">
                  {menuItems.map((item) => (
                    <li key={item.name}>
                      <Link
                        href={item.href}
                        className="block text-[var(--text-secondary)] duration-150 hover:text-[var(--text-primary)]"
                      >
                        <span>{item.name}</span>
                      </Link>
                    </li>
                  ))}
                </ul>
              </div>
              <div className="flex w-full flex-col space-y-3 sm:flex-row sm:gap-3 sm:space-y-0 md:w-fit">
                <Button asChild variant="outline" size="sm" className="border-[#E6E6E6] bg-white hover:bg-[#F8F8FB]">
                  <Link href="/register">
                    <span>Get Started</span>
                  </Link>
                </Button>
              </div>
            </div>
          </div>
        </div>
      </nav>
    </header>
  );
}

function Logo({ className }: { className?: string }) {
  return (
    <span
      className={cn(
        "text-[2rem] font-semibold tracking-[-0.06em] text-black",
        className
      )}
    >
      neyma
    </span>
  );
}
