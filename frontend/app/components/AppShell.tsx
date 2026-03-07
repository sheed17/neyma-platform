"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useMemo, useState } from "react";
import { checkHealth } from "@/lib/api";
import { useAuth } from "@/lib/auth";
import Footer from "@/app/components/Footer";

const links = [
  { href: "/dashboard", label: "Workspace" },
  { href: "/ask", label: "Ask Neyma" },
  { href: "/territory/new", label: "Territory Scan" },
  { href: "/lists", label: "Lists" },
  { href: "/diagnostic/new", label: "Build Brief" },
  { href: "/settings", label: "Settings" },
];

function titleFromPath(pathname: string): string {
  if (pathname.startsWith("/ask")) return "Ask Neyma";
  if (pathname.startsWith("/territory/")) return "Territory Scan";
  if (pathname.startsWith("/lists/")) return "Lists";
  if (pathname.startsWith("/diagnostic/")) return "Brief";
  if (pathname.startsWith("/settings")) return "Settings";
  return "Workspace";
}

export default function AppShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const { user, logout } = useAuth();
  const [apiStatus, setApiStatus] = useState<"checking" | "up" | "down">("checking");
  const [mobileOpen, setMobileOpen] = useState(false);

  useEffect(() => {
    checkHealth().then(() => setApiStatus("up")).catch(() => setApiStatus("down"));
  }, []);

  const pageTitle = useMemo(() => titleFromPath(pathname || "/dashboard"), [pathname]);

  return (
    <div className="min-h-screen bg-[var(--bg-app)] text-[var(--text-primary)]">
      <div className="flex min-h-screen">
        <aside className="hidden min-h-screen w-64 shrink-0 self-stretch border-r border-[var(--border-default)] bg-gradient-to-b from-white to-[#f7fbff] lg:block">
          <div className="sticky top-0 px-3 py-4">
            <Link href="/dashboard" className="mb-4 block rounded-[var(--radius-md)] px-3 py-2">
              <p className="text-[10px] font-semibold uppercase tracking-[0.22em] text-[var(--text-muted)]">Workspace</p>
              <p className="mt-1 text-2xl font-semibold tracking-tight text-[var(--text-primary)]">Neyma</p>
            </Link>
            <nav className="space-y-1">
              {links.map((l) => {
                const active = pathname === l.href || pathname.startsWith(`${l.href}/`);
                return (
                  <Link
                    key={l.href}
                    href={l.href}
                    className={`block rounded-[var(--radius-md)] px-3 py-2.5 text-sm font-medium transition ${active ? "bg-[var(--accent-soft)] text-[var(--accent)] shadow-[inset_0_0_0_1px_rgba(13,148,136,0.15)]" : "text-[var(--text-secondary)] hover:bg-slate-100"}`}
                  >
                    {l.label}
                  </Link>
                );
              })}
            </nav>
          </div>
        </aside>

        <div className="flex min-w-0 flex-1 flex-col">
          <header className="sticky top-0 z-30 border-b border-[var(--border-default)] bg-[var(--bg-card)]/95 backdrop-blur">
            <div className="flex h-16 items-center justify-between px-4 sm:px-6">
              <div className="flex items-center gap-3">
                <button
                  aria-label="Toggle navigation"
                  className="rounded-[var(--radius-sm)] border border-[var(--border-default)] px-2 py-1 text-sm lg:hidden"
                  onClick={() => setMobileOpen((v) => !v)}
                >
                  ☰
                </button>
                <div>
                  <p className="text-xs font-medium text-[var(--text-muted)]">Workspace</p>
                  <h1 className="display-title text-lg font-bold">{pageTitle}</h1>
                </div>
              </div>
              <div className="flex items-center gap-4 text-sm">
                <div className="flex items-center gap-2 text-[var(--text-muted)]">
                  <span className={`inline-block h-2 w-2 rounded-full ${apiStatus === "up" ? "bg-emerald-500" : apiStatus === "down" ? "bg-rose-500" : "bg-amber-400 animate-pulse"}`} />
                  {apiStatus === "up" ? "Connected" : apiStatus === "down" ? "Offline" : "Checking"}
                </div>
                {user && (
                  <div className="flex items-center gap-3">
                    <span className="hidden text-[var(--text-secondary)] sm:block">{user.name}</span>
                    <button onClick={logout} className="rounded-full border border-[var(--border-default)] px-3 py-1.5 text-[var(--text-secondary)] hover:bg-slate-50">
                      Log out
                    </button>
                  </div>
                )}
              </div>
            </div>
          </header>

          {mobileOpen && (
            <div className="border-b border-[var(--border-default)] bg-[var(--bg-card)] px-4 py-3 lg:hidden">
              <nav className="space-y-1">
                {links.map((l) => {
                  const active = pathname === l.href || pathname.startsWith(`${l.href}/`);
                  return (
                    <Link
                      key={l.href}
                      href={l.href}
                      onClick={() => setMobileOpen(false)}
                      className={`block rounded-[var(--radius-md)] px-3 py-2 text-sm font-medium ${active ? "bg-[var(--accent-soft)] text-[var(--accent)]" : "text-[var(--text-secondary)] hover:bg-slate-100"}`}
                    >
                      {l.label}
                    </Link>
                  );
                })}
              </nav>
            </div>
          )}

          <main className="flex-1 px-4 py-6 sm:px-6">{children}</main>
          <Footer />
        </div>
      </div>
    </div>
  );
}
