"use client";

import { useState } from "react";
import { AlertTriangle, ArrowRight, Sparkles, Trash2 } from "lucide-react";
import { useAuth } from "@/lib/auth";
import { Card, CardBody, CardHeader } from "@/app/components/ui/Card";
import Badge from "@/app/components/ui/Badge";
import Input from "@/app/components/ui/Input";
import {
  ApiError,
  createBillingCheckoutSession,
  createBillingPortalSession,
  deleteCurrentAccount,
} from "@/lib/api";
import { clientFacingAppError } from "@/lib/present";
import { Button } from "@/components/ui/button";
import { useRouter } from "next/navigation";

export default function SettingsPage() {
  const router = useRouter();
  const { user, access, logout } = useAuth();
  const [billingBusy, setBillingBusy] = useState<"checkout" | "portal" | null>(null);
  const [billingMessage, setBillingMessage] = useState<string | null>(null);
  const [deleteBusy, setDeleteBusy] = useState(false);
  const [deleteConfirmation, setDeleteConfirmation] = useState("");
  const [deleteMessage, setDeleteMessage] = useState<string | null>(null);
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

          <div className="rounded-[24px] border border-[#E7D8FB] bg-[linear-gradient(135deg,#ffffff_0%,#fbf7ff_58%,#f5eefc_100%)] p-5 shadow-[0_14px_34px_rgba(139,80,212,0.08)]">
            <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
              <div>
                <div className="inline-flex items-center gap-2 rounded-full border border-[#E7D8FB] bg-white px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.16em] text-[#8B50D4]">
                  <Sparkles className="h-3.5 w-3.5" />
                  Billing
                </div>
                <p className="mt-3 text-lg font-semibold text-[var(--text-primary)]">
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
                  variant="default"
                  disabled={billingBusy !== null}
                  className="min-w-[176px] rounded-[16px] bg-[#8B50D4] px-5 text-[15px] font-semibold text-white shadow-[0_12px_28px_rgba(139,80,212,0.28)] hover:bg-[#7740C8]"
                  onClick={() => void openBillingPortal(setBillingBusy, setBillingMessage)}
                >
                  {billingBusy === "portal" ? "Opening..." : "Manage Billing"}
                  <ArrowRight className="ml-2 h-4 w-4" />
                </Button>
              ) : (
                <Button
                  variant="default"
                  disabled={billingBusy !== null}
                  className="min-w-[176px] rounded-[16px] bg-[#8B50D4] px-5 text-[15px] font-semibold text-white shadow-[0_12px_28px_rgba(139,80,212,0.28)] hover:bg-[#7740C8]"
                  onClick={() => void openBillingCheckout(setBillingBusy, setBillingMessage)}
                >
                  {billingBusy === "checkout" ? "Opening..." : "Start 7-day trial"}
                  <ArrowRight className="ml-2 h-4 w-4" />
                </Button>
              )}
            </div>

            {billingMessage ? (
              <p className="mt-3 text-sm text-[var(--text-muted)]">{billingMessage}</p>
            ) : null}
          </div>
        </CardBody>
      </Card>

      <Card className="border-red-200 bg-white">
        <CardHeader
          title="Danger Zone"
          subtitle="Delete your account, workspace, saved scans, lists, briefs, and billing state permanently."
        />
        <CardBody className="space-y-4">
          <div className="rounded-[18px] border border-red-200 bg-[#fff7f7] p-4">
            <div className="flex items-start gap-3">
              <div className="mt-0.5 rounded-full bg-red-50 p-2 text-red-600">
                <AlertTriangle className="h-4 w-4" />
              </div>
              <div>
                <p className="text-sm font-semibold text-[var(--text-primary)]">Delete this Neyma account permanently.</p>
                <p className="mt-1 text-sm leading-6 text-[var(--text-muted)]">
                  Enter the confirmation word below to remove your account, saved workspace data, scans, lists, and billing state. If you need help closing a larger workspace, email <a className="font-medium text-[#8B50D4] underline-offset-4 hover:underline" href="mailto:support@tryneyma.com">support@tryneyma.com</a>.
                </p>
              </div>
            </div>
          </div>

          <div className="grid gap-3 sm:grid-cols-[minmax(0,1fr)_auto] sm:items-end">
            <Input
              label="Type DELETE to confirm"
              value={deleteConfirmation}
              onChange={(event) => setDeleteConfirmation(event.target.value)}
              autoComplete="off"
              className="uppercase tracking-[0.08em]"
            />
            <Button
              variant="destructive"
              disabled={deleteBusy || deleteConfirmation.trim().toUpperCase() !== "DELETE"}
              className="h-10 rounded-[14px] px-5 text-sm font-semibold shadow-[0_10px_24px_rgba(185,28,28,0.18)]"
              onClick={() => void removeAccount({
                confirmation: deleteConfirmation,
                logout,
                router,
                setDeleteBusy,
                setDeleteMessage,
              })}
            >
              {deleteBusy ? "Deleting..." : "Delete Account"}
              <Trash2 className="ml-2 h-4 w-4" />
            </Button>
          </div>

          {deleteMessage ? (
            <p className="text-sm text-[var(--text-muted)]">{deleteMessage}</p>
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

async function removeAccount({
  confirmation,
  logout,
  router,
  setDeleteBusy,
  setDeleteMessage,
}: {
  confirmation: string;
  logout: () => Promise<void>;
  router: ReturnType<typeof useRouter>;
  setDeleteBusy: (value: boolean) => void;
  setDeleteMessage: (value: string | null) => void;
}) {
  setDeleteBusy(true);
  setDeleteMessage(null);
  try {
    const result = await deleteCurrentAccount(confirmation);
    if (!result.deleted) {
      setDeleteMessage("We couldn't finish deleting your account right now.");
      return;
    }
    await logout();
    router.replace("/");
  } catch (error) {
    if (error instanceof ApiError) {
      setDeleteMessage(clientFacingAppError(error.message, "We couldn't finish deleting your account right now."));
    } else {
      setDeleteMessage("We couldn't finish deleting your account right now.");
    }
  } finally {
    setDeleteBusy(false);
  }
}
