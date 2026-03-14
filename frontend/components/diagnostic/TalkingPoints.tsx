"use client";

import Button from "@/app/components/ui/Button";

type Props = {
  bullets: string[];
  onCopy: () => void;
};

export default function TalkingPoints({ bullets, onCopy }: Props) {
  return (
    <section className="rounded-[var(--radius-md)] border border-[var(--border-default)] bg-[var(--bg-card)] p-4">
      <div className="mb-2 flex items-center justify-between gap-2">
        <h2 className="text-sm font-semibold">Verified Observations</h2>
        <Button onClick={onCopy}>Copy Notes</Button>
      </div>
      <ul className="list-disc space-y-1 pl-5 text-sm text-[var(--text-secondary)]">
        {bullets.map((bullet, i) => (
          <li key={i}>{bullet}</li>
        ))}
      </ul>
    </section>
  );
}
