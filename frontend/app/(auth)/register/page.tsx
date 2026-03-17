"use client";

import { useRouter } from "next/navigation";

import { AuthForm } from "@/components/ui/premium-auth";
import { useAuth } from "@/lib/auth";

export default function RegisterPage() {
  const { register, loginAsTestUser } = useAuth();
  const router = useRouter();

  return (
    <div className="grid w-full max-w-5xl gap-8 lg:grid-cols-[0.92fr_1.08fr] lg:items-center">
      <div className="hidden lg:block">
        <p className="section-kicker">
          New Workspace
        </p>
        <h1 className="mt-3 text-5xl font-medium tracking-[-0.05em] text-[var(--text-primary)]">
          Create your Neyma account.
        </h1>
        <p className="mt-4 max-w-[34ch] text-base leading-8 text-[var(--text-secondary)]">
          Start with territory scans, tighten the shortlist with Ask Neyma, and turn the strongest opportunities into briefs your team can act on.
        </p>
      </div>
      <div className="w-full rounded-[28px] border border-[var(--border)] bg-white shadow-[0_18px_50px_rgba(0,0,0,0.06)]">
        <div className="rounded-[28px] bg-white">
          <AuthForm
            initialMode="signup"
            onSignup={async ({ name, email, password }) => {
              await register(name, email, password);
            }}
            onUseTestAccount={async () => {
              await loginAsTestUser();
            }}
            testAccountLabel="Skip to test account"
            testAccountHint="Local shortcut for opening the workspace without verification."
            onSuccess={() => {
              router.push("/login");
            }}
            onClose={() => {
              router.push("/login");
            }}
          />
        </div>
      </div>
    </div>
  );
}
