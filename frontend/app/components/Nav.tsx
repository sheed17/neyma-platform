"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useState } from "react";
import { checkHealth } from "@/lib/api";
import { useAuth } from "@/lib/auth";

const links = [
  { href: "/dashboard", label: "Workspace" },
  { href: "/ask", label: "Ask Neyma" },
  { href: "/territory/new", label: "Territory Scan" },
  { href: "/lists", label: "Lists" },
  { href: "/diagnostic/new", label: "Build Brief" },
  { href: "/settings", label: "Settings" },
];

export default function Nav() {
  const pathname = usePathname();
  const { user, logout } = useAuth();
  const [apiStatus, setApiStatus] = useState<"checking" | "up" | "down">("checking");
  const [mobileOpen, setMobileOpen] = useState(false);

  useEffect(() => {
    checkHealth()
      .then(() => setApiStatus("up"))
      .catch(() => setApiStatus("down"));
  }, []);

  return (
    <header className="border-b border-zinc-200 bg-white px-6 py-3">
      <div className="mx-auto flex max-w-5xl items-center justify-between">
        {/* Left: logo + nav links */}
        <div className="flex items-center gap-8">
          <Link href="/dashboard" className="text-lg font-bold tracking-tight text-zinc-900">
            Neyma
          </Link>
          <nav className="hidden items-center gap-1 md:flex">
            {links.map((l) => {
              const active = pathname === l.href || pathname.startsWith(l.href + "/");
              return (
                <Link
                  key={l.href}
                  href={l.href}
                  className={`rounded-md px-3 py-1.5 text-sm font-medium transition ${
                    active
                      ? "bg-zinc-100 text-zinc-900"
                      : "text-zinc-500 hover:bg-zinc-50 hover:text-zinc-700"
                  }`}
                >
                  {l.label}
                </Link>
              );
            })}
          </nav>
        </div>

        {/* Right: status + user + mobile toggle */}
        <div className="flex items-center gap-4">
          <div className="hidden items-center gap-2 text-sm sm:flex">
            <span
              className={`inline-block h-2 w-2 rounded-full ${
                apiStatus === "up" ? "bg-emerald-500" : apiStatus === "down" ? "bg-red-500" : "animate-pulse bg-amber-500"
              }`}
            />
            <span className="text-zinc-400">
              {apiStatus === "up" ? "Connected" : apiStatus === "down" ? "Offline" : "…"}
            </span>
          </div>

          {user && (
            <div className="hidden items-center gap-3 md:flex">
              <span className="text-sm text-zinc-600">{user.name}</span>
              <button
                onClick={logout}
                className="rounded-md px-3 py-1.5 text-sm font-medium text-zinc-500 transition hover:bg-zinc-50 hover:text-zinc-700"
              >
                Log out
              </button>
            </div>
          )}

          {/* Mobile hamburger */}
          <button
            className="flex h-8 w-8 items-center justify-center rounded-md text-zinc-600 hover:bg-zinc-100 md:hidden"
            onClick={() => setMobileOpen(!mobileOpen)}
            aria-label="Toggle menu"
          >
            {mobileOpen ? (
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M18 6L6 18M6 6l12 12" /></svg>
            ) : (
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M3 12h18M3 6h18M3 18h18" /></svg>
            )}
          </button>
        </div>
      </div>

      {/* Mobile menu */}
      {mobileOpen && (
        <div className="mt-3 border-t border-zinc-100 pt-3 md:hidden">
          <nav className="flex flex-col gap-1">
            {links.map((l) => {
              const active = pathname === l.href || pathname.startsWith(l.href + "/");
              return (
                <Link
                  key={l.href}
                  href={l.href}
                  onClick={() => setMobileOpen(false)}
                  className={`rounded-md px-3 py-2 text-sm font-medium ${
                    active ? "bg-zinc-100 text-zinc-900" : "text-zinc-600 hover:bg-zinc-50"
                  }`}
                >
                  {l.label}
                </Link>
              );
            })}
          </nav>
          {user && (
            <div className="mt-3 flex items-center justify-between border-t border-zinc-100 pt-3">
              <span className="text-sm text-zinc-600">{user.email}</span>
              <button onClick={logout} className="text-sm font-medium text-zinc-500 hover:text-zinc-700">
                Log out
              </button>
            </div>
          )}
        </div>
      )}
    </header>
  );
}
