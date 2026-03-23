"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";

import { AuthForm } from "@/components/ui/premium-auth";
import { useAuth } from "@/lib/auth";

export default function LoginPage() {
  const { user, loading, login, requestPasswordReset } = useAuth();
  const router = useRouter();

  useEffect(() => {
    if (!loading && user) {
      router.replace("/dashboard");
    }
  }, [loading, router, user]);

  return (
    <div className="grid w-full max-w-5xl gap-8 lg:grid-cols-[0.9fr_1.1fr] lg:items-center">
      <div className="hidden lg:block">
        <p className="section-kicker">
          Workspace Access
        </p>
        <h1 className="mt-3 text-5xl font-medium tracking-[-0.05em] text-[var(--text-primary)]">
          Return to the shortlist.
        </h1>
        <p className="mt-4 max-w-[34ch] text-base leading-8 text-[var(--text-secondary)]">
          Open Neyma to continue scoring markets, narrowing with Ask, and moving from ranked opportunities into briefs and lists.
        </p>
      </div>
      <div className="w-full rounded-[28px] border border-[var(--border)] bg-white shadow-[0_18px_50px_rgba(0,0,0,0.06)]">
        <div className="rounded-[28px] bg-white">
          <AuthForm
            initialMode="login"
            loginHref="/login"
            signupHref="/register"
            onLogin={async ({ email, password }) => {
              await login(email, password);
            }}
            onResetPassword={async ({ email }) => {
              await requestPasswordReset(email);
            }}
            onSuccess={() => {
              router.push("/dashboard");
            }}
          />
        </div>
      </div>
    </div>
  );
}
