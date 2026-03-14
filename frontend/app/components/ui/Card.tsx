import React from "react";

type CardProps = React.HTMLAttributes<HTMLElement> & {
  children: React.ReactNode;
};

export function Card({ className = "", children, ...props }: CardProps) {
  return (
    <section className={`app-card ${className}`} {...props}>
      {children}
    </section>
  );
}

export function CardHeader({ title, subtitle, action }: { title: string; subtitle?: string; action?: React.ReactNode }) {
  return (
    <div className="flex items-start justify-between border-b border-[var(--border-default)] px-6 py-5">
      <div>
        <h2 className="section-kicker">{title}</h2>
        {subtitle && <p className="mt-2 text-sm text-[var(--text-secondary)]">{subtitle}</p>}
      </div>
      {action}
    </div>
  );
}

export function CardBody({ className = "", children }: { className?: string; children: React.ReactNode }) {
  return <div className={`px-6 py-6 ${className}`}>{children}</div>;
}
