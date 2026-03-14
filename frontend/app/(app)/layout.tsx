"use client";

import { AuthGuard } from "@/lib/auth";
import AppShell from "../components/AppShell";

export default function AppLayout({ children }: { children: React.ReactNode }) {
  return (
    <div className="theme-light workspace-theme">
      <AuthGuard>
        <AppShell>{children}</AppShell>
      </AuthGuard>
    </div>
  );
}
