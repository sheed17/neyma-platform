"use client";

import { useAuth } from "@/lib/auth";
import { Card, CardBody, CardHeader } from "@/app/components/ui/Card";
import Badge from "@/app/components/ui/Badge";
import Input from "@/app/components/ui/Input";

export default function SettingsPage() {
  const { user } = useAuth();

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
        <CardBody className="flex items-center justify-between">
          <div>
            <p className="font-medium text-[var(--text-primary)]">Free Plan</p>
            <p className="text-sm text-[var(--text-muted)]">Unlimited briefs during beta</p>
          </div>
          <Badge tone="success">Active</Badge>
        </CardBody>
      </Card>
    </div>
  );
}
