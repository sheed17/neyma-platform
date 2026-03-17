"use client";

import { useAuth } from "@/lib/auth";
import { Card, CardBody, CardHeader } from "@/app/components/ui/Card";
import Badge from "@/app/components/ui/Badge";
import Input from "@/app/components/ui/Input";

export default function SettingsPage() {
  const { user, access } = useAuth();
  const usageRows = access ? [
    { label: "Territory scans", value: usageValue(access.remaining.territory_scan, access.limits.territory_scan) },
    { label: "Build briefs", value: usageValue(access.remaining.diagnostic, access.limits.diagnostic) },
    { label: "Ask Neyma", value: usageValue(access.remaining.ask, access.limits.ask) },
  ] : [];

  return (
    <div className="mx-auto max-w-4xl space-y-4">
      <div>
        <p className="section-kicker">Workspace</p>
        <h1 className="page-title">Settings</h1>
        <p className="mt-1 text-sm text-[var(--text-muted)]">Manage account and workspace defaults.</p>
      </div>

      <Card>
        <CardHeader title="Account" />
        <CardBody className="space-y-3">
          <Input label="Name" value={user?.name ?? ""} readOnly />
          <Input label="Email" value={user?.email ?? ""} readOnly />
        </CardBody>
      </Card>

      <Card>
        <CardHeader title="API Keys" subtitle="API key management will be available in a future update." />
        <CardBody>
          <div className="rounded-[var(--radius)] border border-dashed border-[var(--border-default)] bg-[var(--muted)] px-4 py-8 text-center text-sm text-[var(--text-muted)]">
            No API keys yet
          </div>
        </CardBody>
      </Card>

      <Card>
        <CardHeader title="Plan & Usage" />
        <CardBody className="space-y-4">
          <div className="flex items-center justify-between">
            <div>
              <p className="font-medium capitalize text-[var(--text-primary)]">{access?.plan_tier || "Free"} plan</p>
              <p className="text-sm text-[var(--text-muted)]">
                {String(access?.plan_tier) === "free"
                  ? "Monthly usage resets on the first day of each UTC month."
                  : "Workspace entitlements are active for this account."}
              </p>
            </div>
            <Badge tone="success">Active</Badge>
          </div>

          {usageRows.length ? (
            <div className="grid gap-3 sm:grid-cols-3">
              {usageRows.map((item) => (
                <div key={item.label} className="rounded-[var(--radius)] border border-[var(--border-default)] bg-[var(--surface)] p-3">
                  <p className="text-[11px] uppercase tracking-[0.1em] text-[var(--text-muted)]">{item.label}</p>
                  <p className="mt-2 text-sm font-medium text-[var(--text-primary)]">{item.value}</p>
                </div>
              ))}
            </div>
          ) : null}
        </CardBody>
      </Card>
    </div>
  );
}

function usageValue(remaining: number | null | undefined, limit: number | null | undefined) {
  if (limit == null) return "Unlimited";
  return `${remaining ?? 0} left of ${limit}`;
}
