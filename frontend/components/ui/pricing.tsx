"use client";

import { buttonVariants } from "@/components/ui/button";
import { useMediaQuery } from "@/hooks/use-media-query";
import { cn } from "@/lib/utils";
import { motion } from "framer-motion";
import { ArrowRight, Check, Star } from "lucide-react";
import Link from "next/link";
import NumberFlow from "@number-flow/react";

interface PricingPlan {
  name: string;
  price?: number;
  priceLabel?: string;
  period?: string;
  features: string[];
  description: string;
  buttonText: string;
  href: string;
  isPopular: boolean;
  badge?: string;
  note?: string;
}

interface PricingProps {
  plans: PricingPlan[];
  title?: string;
  description?: string;
}

export function Pricing({
  plans,
  title = "Pricing that matches how Neyma works",
  description = "Start free, upgrade when the workflow is part of your weekly prospecting rhythm, or talk with us about enterprise rollout.",
}: PricingProps) {
  const isDesktop = useMediaQuery("(min-width: 768px)");

  return (
    <div className="container py-16 sm:py-20">
      <div className="mx-auto max-w-[760px] text-center space-y-4 mb-12">
        <p className="section-kicker">Pricing</p>
        <h2 className="text-4xl font-semibold tracking-tight text-[#0A0A0A] sm:text-5xl">
          {title}
        </h2>
        <p className="text-[17px] leading-7 text-[var(--text-secondary)]">
          {description}
        </p>
      </div>

      <div className="grid grid-cols-1 gap-5 md:grid-cols-3">
        {plans.map((plan, index) => (
          <motion.div
            key={plan.name}
            initial={{ y: 30, opacity: 0 }}
            whileInView={{
              y: isDesktop && plan.isPopular ? -8 : 0,
              opacity: 1,
              scale: isDesktop && !plan.isPopular ? 0.985 : 1,
            }}
            viewport={{ once: true, amount: 0.35 }}
            transition={{
              duration: 0.6,
              ease: "easeOut",
              delay: index * 0.06,
            }}
            className={cn(
              "relative flex flex-col overflow-hidden rounded-[28px] border p-6 shadow-[0_10px_30px_rgba(10,10,10,0.05)]",
              plan.isPopular
                ? "border-[#8B50D4] bg-[linear-gradient(180deg,#ffffff_0%,#fbf6ff_100%)] shadow-[0_18px_40px_rgba(139,80,212,0.16)]"
                : "border-[var(--border)] bg-white",
            )}
          >
            <div
              aria-hidden
              className={cn(
                "pointer-events-none absolute inset-x-0 top-0 h-24",
                plan.isPopular
                  ? "bg-[radial-gradient(circle_at_top,rgba(139,80,212,0.18)_0%,rgba(255,255,255,0)_75%)]"
                  : "bg-[radial-gradient(circle_at_top,rgba(139,80,212,0.08)_0%,rgba(255,255,255,0)_75%)]",
              )}
            />
            {plan.isPopular ? (
              <div className="absolute right-0 top-0 flex items-center rounded-bl-[18px] rounded-tr-[28px] bg-[#8B50D4] px-3 py-1.5 text-white">
                <Star className="h-4 w-4 fill-current" />
                <span className="ml-1 text-sm font-semibold">Popular</span>
              </div>
            ) : null}

            <div className="relative flex-1">
              <div className="flex items-start justify-between gap-4">
                <div>
                  <p className="text-[13px] font-semibold tracking-[0.16em] text-[var(--text-muted)] uppercase">
                    {plan.name}
                  </p>
                  {plan.badge ? (
                    <p className="mt-2 text-sm font-medium text-[#8B50D4]">{plan.badge}</p>
                  ) : null}
                </div>
              </div>

              <div className="mt-7 flex items-end gap-2">
                <span className="text-5xl font-semibold tracking-tight text-[#0A0A0A]">
                  {plan.priceLabel ? (
                    plan.priceLabel
                  ) : (
                    <>
                      <span className="mr-1">$</span>
                      <NumberFlow
                        value={plan.price ?? 0}
                        format={{
                          minimumFractionDigits: 0,
                          maximumFractionDigits: 0,
                        }}
                        transformTiming={{ duration: 400, easing: "ease-out" }}
                        willChange
                      />
                    </>
                  )}
                </span>
                {plan.period ? (
                  <span className="pb-1.5 text-sm font-semibold text-[var(--text-muted)]">
                    / {plan.period}
                  </span>
                ) : null}
              </div>

              <p className="mt-3 text-sm leading-6 text-[var(--text-secondary)]">
                {plan.description}
              </p>

              <ul className="mt-6 space-y-3">
                {plan.features.map((feature) => (
                  <li key={feature} className="flex items-start gap-3">
                    <span className="inline-flex h-6 w-6 shrink-0 items-center justify-center rounded-full bg-[#F5EEFC] text-[#8B50D4]">
                      <Check className="h-4 w-4" />
                    </span>
                    <span className="text-sm leading-6 text-[var(--text-primary)]">{feature}</span>
                  </li>
                ))}
              </ul>
            </div>

            <div className="relative mt-8">
              <Link
                href={plan.href}
                className={cn(
                  buttonVariants({ variant: plan.isPopular ? "default" : "outline", size: "lg" }),
                  "w-full justify-center rounded-[16px] text-base font-semibold transition-all duration-200",
                  plan.isPopular
                    ? "bg-[#8B50D4] text-white shadow-[0_14px_28px_rgba(139,80,212,0.28)] hover:-translate-y-0.5 hover:bg-[#7740c8] hover:shadow-[0_18px_34px_rgba(139,80,212,0.34)]"
                    : "border-[#E7D8FB] bg-[#F7F2FD] text-[#6F42C1] hover:bg-[#EFE4FD]",
                )}
              >
                <span>{plan.buttonText}</span>
                <ArrowRight className="ml-2 h-4 w-4" />
              </Link>
              {plan.note ? (
                <p className="mt-4 text-xs leading-5 text-[var(--text-muted)]">{plan.note}</p>
              ) : null}
            </div>
          </motion.div>
        ))}
      </div>
    </div>
  );
}
