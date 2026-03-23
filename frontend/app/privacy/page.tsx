import Link from "next/link";

export default function PrivacyPage() {
  return (
    <main className="mx-auto max-w-3xl px-6 py-16">
      <div className="rounded-[24px] border border-[var(--border)] bg-[var(--card)] p-6 shadow-[var(--shadow-card)] sm:p-8">
        <p className="section-kicker">Legal</p>
        <h1 className="mt-3 text-3xl font-medium tracking-[-0.04em] text-[var(--text-primary)]">
          Privacy Policy
        </h1>
        <p className="mt-4 text-sm leading-7 text-[var(--text-secondary)]">
          This is a temporary privacy placeholder while Neyma&apos;s full policy is being finalized. Neyma
          stores account and workflow data needed to operate the product and improve the experience, and it
          should not be used to submit sensitive personal information unless explicitly supported.
        </p>
        <p className="mt-4 text-sm leading-7 text-[var(--text-secondary)]">
          If you need the current privacy policy before signing up, contact{" "}
          <a className="text-[var(--primary)] hover:underline" href="mailto:support@tryneyma.com">
            support@tryneyma.com
          </a>.
        </p>
        <div className="mt-8">
          <Link href="/register" className="text-sm font-medium text-[var(--primary)] hover:underline">
            Back to sign up
          </Link>
        </div>
      </div>
    </main>
  );
}
