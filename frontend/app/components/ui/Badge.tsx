import React from "react";

type Props = {
  children: React.ReactNode;
  tone?: "default" | "success" | "danger" | "muted";
};

const tones: Record<NonNullable<Props["tone"]>, string> = {
  default: "border border-[var(--border-default)] bg-white text-[var(--secondary-foreground)]",
  success: "border border-emerald-500/20 bg-emerald-500/12 text-emerald-300",
  danger: "border border-rose-500/20 bg-rose-500/12 text-rose-300",
  muted: "border border-[var(--border-default)] bg-[var(--surface)] text-[var(--muted-foreground)]",
};

export default function Badge({ children, tone = "muted" }: Props) {
  return <span className={`inline-flex rounded-full px-2.5 py-1 text-xs font-medium ${tones[tone]}`}>{children}</span>;
}
