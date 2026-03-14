"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useMemo, useState } from "react";
import {
  LayoutGrid,
  MessageSquareText,
  Map,
  ListTodo,
  FileStack,
  Settings,
  PanelLeft,
} from "lucide-react";
import { checkHealth } from "@/lib/api";
import { useAuth } from "@/lib/auth";
import Footer from "@/app/components/Footer";

const links = [
  { href: "/dashboard", label: "Workspace", icon: LayoutGrid },
  { href: "/ask", label: "Ask Neyma", icon: MessageSquareText },
  { href: "/territory/new", label: "Territory Scan", icon: Map },
  { href: "/lists", label: "Lists", icon: ListTodo },
  { href: "/diagnostic/new", label: "Build Brief", icon: FileStack },
  { href: "/settings", label: "Settings", icon: Settings },
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
    <div className="relative min-h-screen overflow-hidden bg-[radial-gradient(circle_at_top,rgba(139,80,212,0.09)_0%,#ffffff_32%,#ffffff_100%)] text-[var(--text-primary)]">
      <div
        aria-hidden
        className="pointer-events-none absolute inset-0 bg-[radial-gradient(ellipse_60%_48%_at_50%_-10%,rgba(139,80,212,0.08),rgba(255,255,255,0)_72%)]"
      />
      <div
        aria-hidden
        className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_top_right,rgba(0,0,0,0.025),rgba(255,255,255,0)_24%)]"
      />
      <div className="flex min-h-screen">
        <aside className="hidden min-h-screen w-[248px] shrink-0 self-stretch border-r bg-[var(--surface)] lg:block" style={{ borderColor: "var(--border-default)" }}>
          <div className="sticky top-0 px-4 py-5">
            <Link href="/dashboard" className="mb-6 block rounded-[var(--radius)] px-2 py-1">
              <p className="text-[10px] font-medium uppercase tracking-[0.18em] text-[var(--text-muted)]">Neyma workspace</p>
              <p className="mt-1 text-[28px] font-medium tracking-[-0.04em] text-[var(--text-primary)]">Neyma</p>
            </Link>
            <nav className="space-y-1.5">
              {links.map((l) => {
                const active = pathname === l.href || pathname.startsWith(`${l.href}/`);
                const Icon = l.icon;
                return (
                  <Link
                    key={l.href}
                    href={l.href}
                    className={`flex items-center gap-3 rounded-[12px] px-3 py-2.5 text-sm font-normal transition ${active ? "bg-white text-[var(--text-primary)] shadow-soft" : "text-[var(--text-secondary)] hover:bg-white hover:text-[var(--text-primary)]"}`}
                    style={active ? { boxShadow: "inset 2px 0 0 var(--primary), 0 1px 3px rgba(0,0,0,0.05)" } : undefined}
                  >
                    <Icon className="h-4 w-4 shrink-0" />
                    {l.label}
                  </Link>
                );
              })}
            </nav>
          </div>
        </aside>

        <div className="flex min-w-0 flex-1 flex-col">
          <header className="sticky top-0 z-30 border-b bg-[rgba(255,255,255,0.88)] backdrop-blur-xl" style={{ borderColor: "var(--border-default)" }}>
            <div className="flex h-16 items-center justify-between px-4 sm:px-6">
              <div className="flex items-center gap-3">
                <button
                  aria-label="Toggle navigation"
                  className="rounded-[var(--radius)] border border-white/10 bg-white/[0.045] p-2 text-sm text-[var(--text-primary)] lg:hidden"
                  onClick={() => setMobileOpen((v) => !v)}
                >
                  <PanelLeft className="h-4 w-4" />
                </button>
                <div>
                  <p className="section-kicker">Workspace</p>
                  <h1 className="page-title text-black">{pageTitle}</h1>
                </div>
              </div>
              <div className="flex items-center gap-4 text-sm">
                <div className="flex items-center gap-2 text-black/55">
                  <span className={`inline-block h-2 w-2 rounded-full ${apiStatus === "up" ? "bg-emerald-500" : apiStatus === "down" ? "bg-rose-500" : "bg-amber-400 animate-pulse"}`} />
                  {apiStatus === "up" ? "Connected" : apiStatus === "down" ? "Offline" : "Checking"}
                </div>
                {user && (
                  <div className="flex items-center gap-3">
                    <span className="hidden text-black/85 sm:block">{user.name}</span>
                    <button onClick={logout} className="rounded-[8px] border border-[var(--border-default)] bg-white px-3 py-1.5 text-black/85 hover:bg-[var(--surface)]">
                      Log out
                    </button>
                  </div>
                )}
              </div>
            </div>
          </header>

          {mobileOpen && (
            <div className="border-b border-[var(--border-default)] bg-[rgba(255,255,255,0.96)] px-4 py-3 backdrop-blur-xl lg:hidden">
              <nav className="space-y-1">
                {links.map((l) => {
                  const active = pathname === l.href || pathname.startsWith(`${l.href}/`);
                  return (
                    <Link
                      key={l.href}
                      href={l.href}
                      onClick={() => setMobileOpen(false)}
                      className={`block rounded-[12px] px-3 py-2 text-sm font-normal ${active ? "bg-white text-[var(--text-primary)] shadow-soft" : "text-[var(--text-secondary)] hover:bg-[var(--surface)] hover:text-[var(--text-primary)]"}`}
                      style={active ? { boxShadow: "inset 2px 0 0 var(--primary), 0 1px 3px rgba(0,0,0,0.05)" } : undefined}
                    >
                      {l.label}
                    </Link>
                  );
                })}
              </nav>
            </div>
          )}

          <main className="relative z-10 flex-1 px-4 py-6 sm:px-6">{children}</main>
          <Footer />
        </div>
      </div>
    </div>
  );
}
