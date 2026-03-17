import React from "react";

export function Table({ children, className = "" }: { children: React.ReactNode; className?: string }) {
  return <div className={`app-surface overflow-x-auto ${className}`}><table className="min-w-full border-separate border-spacing-y-2 text-sm">{children}</table></div>;
}

export function THead({ children }: { children: React.ReactNode }) {
  return <thead className="text-left text-[11px] uppercase tracking-[0.14em] text-[var(--text-muted)]">{children}</thead>;
}

export function TH({ children, className = "" }: { children: React.ReactNode; className?: string }) {
  return <th className={`px-4 py-2 font-medium ${className}`}>{children}</th>;
}

export function TR({ children }: { children: React.ReactNode }) {
  return <tr className="rounded-[12px] bg-white shadow-[0_1px_3px_rgba(0,0,0,0.04)] transition hover:shadow-[0_12px_30px_rgba(0,0,0,0.05)]">{children}</tr>;
}

export function TD({ children, className = "" }: { children: React.ReactNode; className?: string }) {
  return <td className={`px-4 py-3 text-[var(--text-secondary)] first:rounded-l-[12px] last:rounded-r-[12px] ${className}`}>{children}</td>;
}
