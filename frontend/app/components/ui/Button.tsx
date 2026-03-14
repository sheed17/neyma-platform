import React from "react";

type Props = React.ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: "primary" | "secondary" | "ghost";
};

const variants: Record<NonNullable<Props["variant"]>, string> = {
  primary: "border border-transparent bg-[var(--primary)] text-[var(--primary-foreground)] hover:brightness-95",
  secondary: "border border-[var(--border-default)] bg-white text-[var(--text-primary)] hover:bg-[var(--surface)]",
  ghost: "border border-transparent bg-transparent text-[var(--text-primary)] hover:bg-[var(--surface)]",
};

export default function Button({ variant = "secondary", className = "", ...props }: Props) {
  return (
    <button
      {...props}
      className={`h-10 rounded-[8px] px-4 text-sm font-medium transition disabled:cursor-not-allowed disabled:opacity-60 ${variants[variant]} ${className}`}
    />
  );
}
