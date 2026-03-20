"use client";

import { useState } from "react";
import { useAuth } from "@/lib/auth";
import { Card, CardBody, CardHeader } from "@/app/components/ui/Card";
import Badge from "@/app/components/ui/Badge";
import Input from "@/app/components/ui/Input";
import Button from "@/app/components/ui/Button";
import {
  ApiError,
  createBillingCheckoutSession,
  createBillingPortalSession,
} from "@/lib/api";
import { clientFacingAppError } from "@/lib/present";

export default function SettingsPage() {
  const { user, access } = useAuth();
  const [billingBusy, setBillingBusy] = useState<"checkout" | "portal" | null>(null);
  const [billingMessage, setBillingMessage] = useState<string | null>(null);
  const usageRows = access ? [
    { label: "Territory scans", value: usageValue(access.remaining.territory_scan, access.limits.territory_scan) },
    { label: "Build briefs", value: usageValue(access.remaining.diagnostic, access.limits.diagnostic) },
    { label: "Ask Neyma", value: usageValue(access.remaining.ask, access.limits.ask) },
  ] : [];
  const isPro = String(access?.plan_tier || "").toLowerCase() === "pro";
  const billingStatus = String(access?.billing?.subscription_status || "").toLowerCase();

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
              <p className="font-semibold text-[var(--text-primary)]">{String(access?.plan_tier || "FREE").toUpperCase()} PLAN</p>
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

          <div className="rounded-[var(--radius)] border border-[var(--border-default)] bg-[var(--surface)] p-4">
            <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
              <div>
                <p className="font-semibold text-[var(--text-primary)]">
                  {isPro ? "Manage Neyma Pro" : "Upgrade to Neyma Pro"}
                </p>
                <p className="mt-1 text-sm text-[var(--text-muted)]">
                  {isPro
                    ? `Your billing status is ${billingStatus || "active"}. Manage payment details or cancel anytime in Stripe's billing portal.`
                    : "Start a 7-day free trial, then continue on Neyma Pro for $39/month with unlimited scans, briefs, and Ask Neyma."}
                </p>
              </div>

              {isPro ? (
                <Button
                  variant="primary"
                  disabled={billingBusy !== null}
                  onClick={() => void openBillingPortal(setBillingBusy, setBillingMessage)}
                >
                  {billingBusy === "portal" ? "Opening..." : "Manage Billing"}
                </Button>
              ) : (
                <Button
                  variant="primary"
                  disabled={billingBusy !== null}
                  onClick={() => void openBillingCheckout(setBillingBusy, setBillingMessage)}
                >
                  {billingBusy === "checkout" ? "Opening..." : "Start 7-day trial"}
                </Button>
              )}
            </div>

            {billingMessage ? (
              <p className="mt-3 text-sm text-[var(--text-muted)]">{billingMessage}</p>
            ) : null}
          </div>
        </CardBody>
      </Card>
    </div>
  );
}

function usageValue(remaining: number | null | undefined, limit: number | null | undefined) {
  if (limit == null) return "Unlimited";
  return `${remaining ?? 0} left of ${limit}`;
}

async function openBillingCheckout(
  setBillingBusy: (value: "checkout" | "portal" | null) => void,
  setBillingMessage: (value: string | null) => void,
) {
  setBillingBusy("checkout");
  setBillingMessage(null);
  try {
    const { url } = await createBillingCheckoutSession();
    window.location.assign(url);
  } catch (error) {
    if (error instanceof ApiError) {
      setBillingMessage(clientFacingAppError(error.message, "We couldn't open billing right now."));
    } else {
      setBillingMessage("We couldn't open billing right now.");
    }
  } finally {
    setBillingBusy(null);
  }
}

async function openBillingPortal(
  setBillingBusy: (value: "checkout" | "portal" | null) => void,
  setBillingMessage: (value: string | null) => void,
) {
  setBillingBusy("portal");
  setBillingMessage(null);
  try {
    const { url } = await createBillingPortalSession();
    window.location.assign(url);
  } catch (error) {
    if (error instanceof ApiError) {
      setBillingMessage(clientFacingAppError(error.message, "We couldn't open billing right now."));
    } else {
      setBillingMessage("We couldn't open billing right now.");
    }
  } finally {
    setBillingBusy(null);
  }
}
