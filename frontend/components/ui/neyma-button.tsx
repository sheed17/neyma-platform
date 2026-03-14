"use client";

import * as React from "react";

import { Button, type ButtonProps } from "@/components/ui/button";
import { cn } from "@/lib/utils";

type NeymaButtonVariant = "primary" | "secondary" | "ghost" | "icon" | "ai";

export interface NeymaButtonProps extends Omit<ButtonProps, "variant" | "size"> {
  variant?: NeymaButtonVariant;
}

const neymaVariantClasses: Record<NeymaButtonVariant, string> = {
  primary:
    "rounded-lg border border-transparent bg-gradient-to-r from-[#8B50D4] to-[#A873F3] px-6 py-3 text-white shadow-sm transition-all duration-200 hover:-translate-y-[1px] hover:shadow-md hover:brightness-[0.98]",
  secondary:
    "rounded-lg border border-[#E6E6E6] bg-white px-6 py-3 text-[#0A0A0A] shadow-sm transition-all duration-200 hover:bg-gray-50",
  ghost:
    "rounded-md border border-transparent bg-transparent px-4 py-2 text-[#0A0A0A] shadow-none transition-all duration-200 hover:bg-gray-100",
  icon:
    "flex h-9 w-9 items-center justify-center rounded-md border border-[#E6E6E6] bg-white p-0 text-[#0A0A0A] shadow-none transition-all duration-200 hover:bg-gray-100",
  ai:
    "group relative overflow-hidden rounded-lg border border-transparent bg-gradient-to-r from-[#8B50D4] to-[#A873F3] px-6 py-3 text-white shadow-sm transition-all duration-200 hover:-translate-y-[1px] hover:shadow-md hover:brightness-[0.98]",
};

export function NeymaButton({
  className,
  children,
  variant = "primary",
  asChild = false,
  ...props
}: NeymaButtonProps) {
  const size = variant === "icon" ? "icon" : undefined;
  const buttonChildren =
    variant === "ai" && !asChild ? (
      <>
        {children}
        <span
          aria-hidden="true"
          className="pointer-events-none absolute inset-0 opacity-0 group-hover:opacity-100 bg-gradient-to-r from-transparent via-white/30 to-transparent animate-[shine_1.5s_linear_infinite]"
        />
      </>
    ) : (
      children
    );

  return (
    <Button
      {...props}
      asChild={asChild}
      size={size}
      className={cn(
        "font-medium",
        neymaVariantClasses[variant],
        className
      )}
    >
      {buttonChildren}
    </Button>
  );
}
