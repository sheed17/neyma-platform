"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useMemo, useState, type ReactNode } from "react";
import {
  FileStack,
  LayoutGrid,
  LogIn,
  ListTodo,
  Map,
  MessageSquareText,
  PanelLeft,
  Settings,
  UserPlus,
} from "lucide-react";
import { isGuestEntryPath, useAuth } from "@/lib/auth";
import Footer from "@/app/components/Footer";

const links = [
  { href: "/dashboard", label: "Workspace", icon: LayoutGrid },
  { href: "/ask", label: "Ask Neyma", icon: MessageSquareText },
  { href: "/territory/new", label: "Territory Scan", icon: Map },
  { href: "/lists", label: "Lists", icon: ListTodo },
  { href: "/diagnostic/new", label: "Build Brief", icon: FileStack },
  { href: "/settings", label: "Settings", icon: Settings },
];

function pageMetaFromPath(pathname: string): { title: string; description: string } {
  if (pathname.startsWith("/ask")) {
    return {
      title: "Ask Neyma",
      description: "Describe the market or gap you want, then narrow the ranked output into the next brief.",
    };
  }
  if (pathname.startsWith("/territory/")) {
    return {
      title: "Territory Scan",
      description: "Start with the market, review the ranking, and open briefs only where the signal is strong.",
    };
  }
  if (pathname.startsWith("/lists/")) {
    return {
      title: "Lists",
      description: "Keep the best-fit accounts together so the next brief is always close at hand.",
    };
  }
  if (pathname.startsWith("/diagnostic/")) {
    return {
      title: "Brief",
      description: "Move from shortlist to pitch with the full account view in one place.",
    };
  }
  if (pathname.startsWith("/settings")) {
    return {
      title: "Settings",
      description: "Manage workspace defaults and the account details behind the workflow.",
    };
  }
  return {
    title: "Workspace",
    description: "A cleaner command center for scanning markets, narrowing lists, and building the next brief.",
  };
}

export default function AppShell({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const { user, access, logout } = useAuth();
  const [mobileOpen, setMobileOpen] = useState(false);
  const guestEntry = !user && isGuestEntryPath(pathname);
  const planLabel = access?.plan_tier ? `${String(access.plan_tier).charAt(0).toUpperCase()}${String(access.plan_tier).slice(1)} plan` : null;

  const pageMeta = useMemo(() => pageMetaFromPath(pathname || "/dashboard"), [pathname]);

  return (
    <div className="relative min-h-screen overflow-hidden bg-[linear-gradient(180deg,#fcf8ff_0%,#fffefe_24%,#f7f3fb_100%)] text-[var(--text-primary)]">
      <div
        aria-hidden
        className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_top_left,rgba(139,80,212,0.14),rgba(255,255,255,0)_34%)]"
      />
      <div
        aria-hidden
        className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_80%_10%,rgba(139,80,212,0.08),rgba(255,255,255,0)_26%)]"
      />

      <div className="relative z-10 flex min-h-screen">
        {!guestEntry ? (
        <aside className="hidden min-h-screen w-[248px] shrink-0 self-stretch border-r border-[var(--border-default)] bg-white/88 lg:block">
          <div className="sticky top-0 flex h-screen flex-col px-4 py-5">
            <Link href="/dashboard" className="rounded-[18px] px-2 py-1">
              <p className="text-[10px] font-medium uppercase tracking-[0.18em] text-[var(--text-muted)]">Neyma workspace</p>
              <p className="mt-1 text-[28px] font-medium tracking-[-0.05em] text-[var(--text-primary)]">Neyma</p>
            </Link>

            <nav className="mt-6 space-y-2">
              {links.map((link) => {
                const active = pathname === link.href || pathname.startsWith(`${link.href}/`);
                const Icon = link.icon;
                return (
                  <Link
                    key={link.href}
                    href={link.href}
                    className={`flex items-center gap-3 rounded-[16px] px-3 py-3 text-sm transition ${
                      active
                        ? "bg-white text-[var(--text-primary)] shadow-[0_12px_28px_rgba(10,10,10,0.08)]"
                        : "text-[var(--text-secondary)] hover:bg-white hover:text-[var(--text-primary)]"
                    }`}
                  >
                    <span
                      className={`inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-full border ${
                        active
                          ? "border-[#e9def7] bg-[#f5eefc] text-[var(--primary)]"
                          : "border-[var(--border-default)] bg-[var(--surface)] text-[var(--text-secondary)]"
                      }`}
                    >
                      <Icon className="h-4 w-4 shrink-0" />
                    </span>
                    <span className="font-medium">{link.label}</span>
                  </Link>
                );
              })}
            </nav>

            <div className="mt-auto rounded-[22px] border border-[var(--border-default)] bg-[var(--surface)] p-4">
              {user ? (
                <div>
                  <p className="truncate text-sm text-[var(--text-primary)]">{user.name}</p>
                  {planLabel ? <p className="mt-1 text-xs text-[var(--text-secondary)]">{planLabel}</p> : null}
                  <button
                    onClick={logout}
                    className="mt-3 inline-flex h-10 items-center rounded-full border border-[var(--border-default)] bg-white px-4 text-sm text-[var(--text-secondary)] transition hover:bg-white hover:text-[var(--text-primary)]"
                  >
                    Log out
                  </button>
                </div>
              ) : null}
            </div>
          </div>
        </aside>
        ) : null}

        <div className="flex min-w-0 flex-1 flex-col">
          <header className="sticky top-0 z-30 border-b border-black/6 bg-[rgba(255,255,255,0.78)] backdrop-blur-2xl">
            <div className="flex min-h-[72px] items-center justify-between gap-3 px-4 sm:min-h-[84px] sm:px-6 lg:px-8">
              <div className="flex min-w-0 items-center gap-3">
                {!guestEntry ? (
                  <button
                    aria-label="Toggle navigation"
                    className="rounded-full border border-[var(--border-default)] bg-white p-2 text-sm text-[var(--text-primary)] shadow-[0_10px_24px_rgba(10,10,10,0.06)] lg:hidden"
                    onClick={() => setMobileOpen((value) => !value)}
                  >
                    <PanelLeft className="h-4 w-4" />
                  </button>
                ) : null}
                <div className="min-w-0">
                  <p className="section-kicker">{guestEntry ? "Neyma" : "Neyma workspace"}</p>
                  <h1 className="page-title truncate text-black">{pageMeta.title}</h1>
                  <p className="mt-1 hidden max-w-[60ch] text-sm text-[var(--text-secondary)] sm:block">{pageMeta.description}</p>
                </div>
              </div>

              <div className="flex shrink-0 items-center gap-2 text-sm sm:gap-3">
                {guestEntry ? (
                  <>
                    <Link
                      href="/login"
                      className="inline-flex h-10 items-center rounded-full border border-[var(--border-default)] bg-white px-4 text-sm font-medium text-[var(--text-primary)] transition hover:bg-[var(--surface)]"
                    >
                      <LogIn className="mr-2 h-4 w-4" />
                      Log in
                    </Link>
                    <Link
                      href="/register"
                      className="inline-flex h-10 items-center rounded-full bg-[var(--primary)] px-4 text-sm font-medium text-white transition hover:brightness-95"
                    >
                      <UserPlus className="mr-2 h-4 w-4" />
                      Sign up
                    </Link>
                  </>
                ) : user ? (
                  <>
                    <div className="flex items-center gap-2 rounded-full border border-[var(--border-default)] bg-white px-2 py-2 shadow-[0_10px_20px_rgba(10,10,10,0.04)]">
                      <span className="inline-flex h-9 w-9 items-center justify-center rounded-full bg-[#f5eefc] text-sm font-medium text-[var(--primary)]">
                        {user.name?.slice(0, 1).toUpperCase() || "N"}
                      </span>
                      <div className="hidden pr-1 sm:block">
                        <p className="text-sm font-medium text-black/85">{user.name}</p>
                      </div>
                    </div>
                    <button
                      onClick={logout}
                      className="hidden h-10 items-center rounded-full border border-[var(--border-default)] bg-white px-4 text-sm font-medium text-[var(--text-primary)] transition hover:bg-[var(--surface)] sm:inline-flex"
                    >
                      Log out
                    </button>
                  </>
                ) : null}
              </div>
            </div>
          </header>

          {mobileOpen && !guestEntry ? (
            <div className="border-b border-black/6 bg-[rgba(255,255,255,0.92)] px-4 py-4 backdrop-blur-2xl lg:hidden">
              <div className="rounded-[24px] border border-[var(--border-default)] bg-white p-3 shadow-[0_24px_48px_rgba(10,10,10,0.08)]">
                <nav className="space-y-2">
                  {links.map((link) => {
                    const active = pathname === link.href || pathname.startsWith(`${link.href}/`);
                    const Icon = link.icon;
                    return (
                      <Link
                        key={link.href}
                        href={link.href}
                        onClick={() => setMobileOpen(false)}
                        className={`flex items-center gap-3 rounded-[16px] px-3 py-3 text-sm ${
                          active
                            ? "bg-[#f5eefc] text-[var(--text-primary)]"
                            : "text-[var(--text-secondary)] hover:bg-[var(--surface)] hover:text-[var(--text-primary)]"
                        }`}
                      >
                        <Icon className="h-4 w-4 shrink-0" />
                        {link.label}
                      </Link>
                    );
                  })}
                </nav>

                {user ? (
                  <button
                    onClick={() => {
                      setMobileOpen(false);
                      logout();
                    }}
                    className="mt-3 inline-flex h-10 items-center rounded-full border border-[var(--border-default)] px-4 text-sm text-[var(--text-secondary)] transition hover:bg-[var(--surface)] hover:text-[var(--text-primary)]"
                  >
                    Log out
                  </button>
                ) : null}
              </div>
            </div>
          ) : null}

          <main className={`relative z-10 flex-1 px-4 py-6 sm:px-6 ${guestEntry ? "lg:px-6" : "lg:px-8"}`}>{children}</main>
          <Footer />
        </div>
      </div>
    </div>
  );
}
