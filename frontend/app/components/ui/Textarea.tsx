import React from "react";

type Props = React.TextareaHTMLAttributes<HTMLTextAreaElement> & {
  label?: string;
  error?: string;
};

export default function Textarea({ label, error, className = "", ...props }: Props) {
  return (
    <label className="block">
      {label && <span className="mb-1 block text-sm font-medium text-[var(--text-secondary)]">{label}</span>}
      <textarea
        {...props}
        className={`w-full rounded-[8px] border bg-white px-3 py-2 text-sm text-[var(--text-primary)] outline-none transition placeholder:text-[var(--text-muted)] ${error ? "border-red-400 focus:border-red-500" : "border-[var(--border-default)] focus:border-[var(--ring)]"} ${className}`}
      />
      {error && <span className="mt-1 block text-xs text-red-600">{error}</span>}
    </label>
  );
}
