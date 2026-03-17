import React from "react";

export default function Modal({
  open,
  title,
  onClose,
  children,
  footer,
}: {
  open: boolean;
  title: string;
  onClose: () => void;
  children: React.ReactNode;
  footer?: React.ReactNode;
}) {
  if (!open) return null;
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/40 px-4" onClick={onClose}>
      <div className="app-card app-elevated w-full max-w-md" onClick={(e) => e.stopPropagation()}>
        <div className="border-b border-[var(--border-default)] px-4 py-3">
          <h3 className="text-sm font-semibold text-[var(--text-primary)]">{title}</h3>
        </div>
        <div className="px-4 py-3">{children}</div>
        {footer && <div className="border-t border-[var(--border-default)] px-4 py-3">{footer}</div>}
      </div>
    </div>
  );
}
