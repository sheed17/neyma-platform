"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";

import { useAuth } from "@/lib/auth";

function passwordStrengthMessage(password: string) {
  if (password.length < 8) return "Use at least 8 characters.";
  if (!/[A-Z]/.test(password) || !/[a-z]/.test(password) || !/\d/.test(password)) {
    return "Use upper, lower, and a number for a stronger password.";
  }
  return "Strong enough";
}

export default function ResetPasswordPage() {
  const router = useRouter();
  const { loading, updatePassword } = useAuth();
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  useEffect(() => {
    setError(null);
  }, [password, confirmPassword]);

  const validationError = useMemo(() => {
    if (!password) return "Enter a new password.";
    if (password.length < 8) return "Use at least 8 characters.";
    if (confirmPassword !== password) return "Passwords do not match.";
    return null;
  }, [password, confirmPassword]);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    setSuccess(null);
    if (validationError) {
      setError(validationError);
      return;
    }
    setSubmitting(true);
    try {
      await updatePassword(password);
      setSuccess("Password updated. Sign in with your new password.");
      setTimeout(() => {
        router.replace("/login");
      }, 1200);
    } catch (err) {
      setError(err instanceof Error ? err.message : "We couldn't update your password right now. Please try again.");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="theme-light relative flex min-h-screen flex-col overflow-hidden bg-[radial-gradient(circle_at_top,rgba(139,80,212,0.18)_0%,#ffffff_34%,#ffffff_100%)] text-foreground">
      <div aria-hidden className="pointer-events-none absolute inset-0 bg-[radial-gradient(ellipse_55%_70%_at_50%_-12%,rgba(139,80,212,0.14),rgba(255,255,255,0)_70%)]" />
      <header className="relative z-10 border-b border-black/10 px-6 py-4">
        <Link className="text-lg font-bold tracking-tight text-black" href="/">
          Neyma
        </Link>
      </header>

      <main className="relative z-10 flex flex-1 items-center justify-center px-6 py-12">
        <div className="w-full max-w-[520px] rounded-[28px] border border-[var(--border)] bg-white shadow-[0_18px_50px_rgba(0,0,0,0.06)]">
          <div className="rounded-[28px] bg-white p-6">
            <div className="mb-8 text-center">
              <p className="section-kicker">Password Recovery</p>
              <h1 className="mt-3 text-3xl font-semibold tracking-[-0.04em] text-[var(--text-primary)]">
                Set a new password
              </h1>
              <p className="mt-3 text-sm leading-7 text-[var(--text-secondary)]">
                Choose a new password for your Neyma account, then head back into the workspace.
              </p>
            </div>

            <form onSubmit={handleSubmit} className="space-y-4">
              <div>
                <label htmlFor="new-password" className="mb-[5px] block text-[11px] font-medium text-[var(--text-secondary)]">
                  New password
                </label>
                <input
                  id="new-password"
                  type="password"
                  autoComplete="new-password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  className="w-full rounded-xl border border-input bg-muted/50 px-4 py-3 text-[var(--text-primary)] placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/20"
                />
                {password ? (
                  <p className="mt-2 text-[11px] uppercase tracking-[0.06em] text-[var(--text-muted)]/80">
                    {passwordStrengthMessage(password)}
                  </p>
                ) : null}
              </div>

              <div>
                <label htmlFor="confirm-password" className="mb-[5px] block text-[11px] font-medium text-[var(--text-secondary)]">
                  Confirm password
                </label>
                <input
                  id="confirm-password"
                  type="password"
                  autoComplete="new-password"
                  value={confirmPassword}
                  onChange={(e) => setConfirmPassword(e.target.value)}
                  className="w-full rounded-xl border border-input bg-muted/50 px-4 py-3 text-[var(--text-primary)] placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/20"
                />
              </div>

              {error ? (
                <div className="rounded-[16px] border border-[rgba(220,38,38,0.16)] bg-[rgba(254,242,242,0.9)] px-4 py-3 text-sm text-rose-600">
                  {error}
                </div>
              ) : null}

              {success ? (
                <div className="rounded-[16px] border border-[rgba(83,74,183,0.14)] bg-[rgba(83,74,183,0.07)] px-4 py-3 text-sm font-medium text-[var(--text-primary)]">
                  {success}
                </div>
              ) : null}

              <button
                type="submit"
                disabled={loading || submitting}
                className="inline-flex h-11 w-full items-center justify-center rounded-[var(--radius)] bg-[var(--primary)] px-5 text-sm font-medium text-white transition hover:brightness-95 disabled:cursor-not-allowed disabled:opacity-60"
              >
                {submitting ? "Updating..." : "Update password"}
              </button>

              <Link
                href="/login"
                className="inline-flex h-11 w-full items-center justify-center rounded-[var(--radius)] border border-[var(--border-default)] bg-white px-5 text-sm font-medium text-[var(--text-primary)] transition hover:bg-[var(--surface)]"
              >
                Back to login
              </Link>
            </form>
          </div>
        </div>
      </main>
    </div>
  );
}
