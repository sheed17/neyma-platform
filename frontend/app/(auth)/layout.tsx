import Link from "next/link";

export default function AuthLayout({ children }: { children: React.ReactNode }) {
  return (
    <div className="theme-light relative flex min-h-screen flex-col overflow-hidden bg-[radial-gradient(circle_at_top,rgba(139,80,212,0.18)_0%,#ffffff_34%,#ffffff_100%)] text-foreground">
      <div
        aria-hidden
        className="pointer-events-none absolute inset-0 bg-[radial-gradient(ellipse_55%_70%_at_50%_-12%,rgba(139,80,212,0.14),rgba(255,255,255,0)_70%)]"
      />
      <div
        aria-hidden
        className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_top_right,rgba(0,0,0,0.04),rgba(255,255,255,0)_22%)]"
      />
      <header className="relative z-10 border-b border-black/10 px-6 py-4">
        <Link href="/" className="text-lg font-bold tracking-tight text-black">
          Neyma
        </Link>
      </header>
      <main className="relative z-10 flex flex-1 items-center justify-center px-6 py-12">
        {children}
      </main>
    </div>
  );
}
