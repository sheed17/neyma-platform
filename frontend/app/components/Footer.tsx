import Link from "next/link";

export default function Footer() {
  return (
    <footer className="border-t border-[var(--border-default)] bg-[linear-gradient(180deg,rgba(255,255,255,0.82)_0%,rgba(247,243,234,0.9)_100%)] px-4 py-5 sm:px-6">
      <div className="mx-auto flex max-w-[var(--max-content)] flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div className="min-w-0">
          <div className="flex items-center gap-3">
            <Link href="/dashboard" className="text-sm font-semibold text-[var(--text-primary)]">
              Neyma
            </Link>
            <span className="rounded-full border border-black/8 bg-white px-2.5 py-1 text-[11px] font-medium text-[var(--text-secondary)]">
              Territory-first workflow
            </span>
          </div>
          <p className="mt-2 text-xs text-[var(--text-muted)]">
            Scan the market, narrow the shortlist, open briefs only when they are worth the deeper work.
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-5 text-xs text-[var(--text-secondary)]">
          <Link href="/dashboard" className="transition hover:text-[var(--text-primary)]">Workspace</Link>
          <Link href="/territory/new" className="transition hover:text-[var(--text-primary)]">Territory Scan</Link>
          <Link href="/ask" className="transition hover:text-[var(--text-primary)]">Ask Neyma</Link>
          <Link href="/diagnostic/new" className="transition hover:text-[var(--text-primary)]">Build Brief</Link>
          <Link href="/lists" className="transition hover:text-[var(--text-primary)]">Lists</Link>
        </div>
      </div>
    </footer>
  );
}
