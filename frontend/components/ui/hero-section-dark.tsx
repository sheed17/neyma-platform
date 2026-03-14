"use client";

import * as React from "react";
import { ChevronRight } from "lucide-react";

import { cn } from "@/lib/utils";

interface HeroSectionProps extends React.HTMLAttributes<HTMLDivElement> {
  title?: React.ReactNode;
  subtitle?: {
    regular: React.ReactNode;
    gradient: React.ReactNode;
  };
  description?: React.ReactNode;
  ctaText?: string;
  ctaHref?: string;
  secondaryCtaText?: string;
  secondaryCtaHref?: string;
  bottomImage?: {
    light: string;
    dark: string;
  };
  children?: React.ReactNode;
  gridOptions?: {
    angle?: number;
    cellSize?: number;
    opacity?: number;
    lightLineColor?: string;
    darkLineColor?: string;
  };
}

const RetroGrid = ({
  angle = 64,
  cellSize = 56,
  opacity = 0.22,
  lightLineColor = "rgba(255, 255, 255, 0.08)",
  darkLineColor = "rgba(255, 255, 255, 0.08)",
}: NonNullable<HeroSectionProps["gridOptions"]>) => {
  const gridStyles = {
    "--grid-angle": `${angle}deg`,
    "--cell-size": `${cellSize}px`,
    "--opacity": opacity,
    "--light-line": lightLineColor,
    "--dark-line": darkLineColor,
  } as React.CSSProperties;

  return (
    <div
      className={cn(
        "pointer-events-none absolute inset-0 overflow-hidden [perspective:200px]",
        "opacity-[var(--opacity)]"
      )}
      style={gridStyles}
    >
      <div className="absolute inset-0 [transform:rotateX(var(--grid-angle))]">
        <div className="animate-grid absolute inset-0 h-[300vh] w-[600vw] [margin-left:-200%] [transform-origin:100%_0_0] [background-image:linear-gradient(to_right,var(--light-line)_1px,transparent_0),linear-gradient(to_bottom,var(--light-line)_1px,transparent_0)] [background-repeat:repeat] [background-size:var(--cell-size)_var(--cell-size)] dark:[background-image:linear-gradient(to_right,var(--dark-line)_1px,transparent_0),linear-gradient(to_bottom,var(--dark-line)_1px,transparent_0)]" />
      </div>
      <div className="absolute inset-0 bg-[linear-gradient(180deg,rgba(10,10,10,0.08)_0%,rgba(10,10,10,0.42)_44%,#0a0a0a_100%)]" />
    </div>
  );
};

const HeroSection = React.forwardRef<HTMLDivElement, HeroSectionProps>(
  (
    {
      className,
      title = "AI Prospecting Intelligence",
      subtitle = {
        regular: "Let AI surface the practices ",
        gradient: "most worth pursuing.",
      },
      description = "Neyma scans a market, ranks the strongest opportunities with ML, narrows them with AI reasoning, and opens briefs only where deeper work is justified.",
      ctaText = "Run territory scan",
      ctaHref = "/register",
      secondaryCtaText,
      secondaryCtaHref,
      bottomImage,
      children,
      gridOptions,
      ...props
    },
    ref
  ) => {
    return (
      <div
        className={cn(
          "relative overflow-hidden rounded-[36px] border border-white/8 bg-[#0a0a0a] text-[#fafafa] shadow-[0_24px_80px_rgba(0,0,0,0.45)]",
          className
        )}
        ref={ref}
        {...props}
      >
        <div className="absolute inset-0 bg-[radial-gradient(ellipse_58%_70%_at_50%_-12%,rgba(255,255,255,0.08),rgba(10,10,10,0)_72%)]" />
        <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_right,rgba(255,255,255,0.04),rgba(10,10,10,0)_24%)]" />
        <section className="relative z-10 mx-auto max-w-full">
          <RetroGrid {...gridOptions} />
          <div className="mx-auto max-w-screen-xl px-5 py-22 md:px-8 lg:py-28">
            <div className="relative z-10 mx-auto max-w-3xl space-y-5 text-center">
              <div className="mx-auto inline-flex items-center rounded-full border border-white/10 bg-white/4 px-4 py-2 text-xs font-medium uppercase tracking-[0.16em] text-white/70 backdrop-blur-xl">
                {title}
                <ChevronRight className="ml-2 h-4 w-4 text-white/60" />
              </div>
              <h1 className="mx-auto max-w-[14ch] text-[2.9rem] font-semibold leading-[0.94] tracking-[-0.055em] text-white sm:text-[3.6rem] md:text-[4.6rem] lg:text-[5.2rem]">
                <span className="block text-balance">{subtitle.regular}</span>
                <span className="mt-2 block text-white">
                  {subtitle.gradient}
                </span>
              </h1>
              <div className="mx-auto max-w-2xl text-base leading-7 text-white/64 sm:text-lg">
                {description}
              </div>
              <div className="flex flex-col items-center justify-center gap-3 sm:flex-row sm:space-y-0">
                <span className="relative inline-flex overflow-hidden rounded-full p-[1.5px]">
                  <span className="absolute inset-[-1000%] animate-[spin_2.4s_linear_infinite] bg-[conic-gradient(from_90deg_at_50%_50%,#ffffff_0%,#3b3b3b_50%,#ffffff_100%)]" />
                  <a
                    href={ctaHref}
                    className="relative inline-flex h-12 items-center justify-center rounded-full bg-white px-6 text-sm font-semibold text-black transition hover:brightness-[0.97]"
                  >
                    {ctaText}
                  </a>
                </span>
                {secondaryCtaText && secondaryCtaHref ? (
                  <a
                    href={secondaryCtaHref}
                    className="inline-flex h-12 items-center justify-center rounded-full border border-white/10 bg-white/4 px-6 text-sm font-medium text-white/84 transition hover:bg-white/8"
                  >
                    {secondaryCtaText}
                  </a>
                ) : null}
              </div>
            </div>

            <div className="relative z-10 mt-16">
              {children ? (
                children
              ) : bottomImage ? (
                <div className="overflow-hidden rounded-[28px] border border-white/10 bg-[#111111] p-3 shadow-[0_30px_80px_rgba(0,0,0,0.38)]">
                  {/* eslint-disable-next-line @next/next/no-img-element */}
                  <img
                    src={bottomImage.light}
                    className="w-full rounded-[20px] border border-white/10 object-cover"
                    alt="Dashboard preview"
                  />
                </div>
              ) : null}
            </div>
          </div>
        </section>
      </div>
    );
  }
);

HeroSection.displayName = "HeroSection";

export { HeroSection };
