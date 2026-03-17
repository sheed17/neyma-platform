"use client";

import { useMemo, useState } from "react";
import Modal from "@/app/components/ui/Modal";
import Button from "@/app/components/ui/Button";
import Input from "@/app/components/ui/Input";
import type { ProspectList } from "@/lib/types";

type Props = {
  open: boolean;
  title?: string;
  lists: ProspectList[];
  busy?: boolean;
  onClose: () => void;
  onConfirm: (payload: { listId?: number; newListName?: string }) => Promise<void> | void;
};

export default function ListPickerModal({
  open,
  title = "Add to list",
  lists,
  busy = false,
  onClose,
  onConfirm,
}: Props) {
  const hasLists = lists.length > 0;
  const [mode, setMode] = useState<"existing" | "new">(hasLists ? "existing" : "new");
  const [selectedListId, setSelectedListId] = useState<number | null>(hasLists && lists[0] ? lists[0].id : null);
  const [newListName, setNewListName] = useState("");
  const [error, setError] = useState<string | null>(null);
  const canSubmit = useMemo(() => {
    if (mode === "existing") return selectedListId != null;
    return newListName.trim().length > 0;
  }, [mode, newListName, selectedListId]);

  async function submit() {
    setError(null);
    try {
      if (mode === "existing") {
        if (selectedListId == null) {
          setError("Select a list.");
          return;
        }
        await onConfirm({ listId: selectedListId });
        return;
      }
      const trimmed = newListName.trim();
      if (!trimmed) {
        setError("List name is required.");
        return;
      }
      await onConfirm({ newListName: trimmed });
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to add to list");
    }
  }

  return (
    <Modal
      open={open}
      title={title}
      onClose={() => {
        if (!busy) onClose();
      }}
      footer={(
        <div className="flex items-center justify-end gap-2">
          <Button onClick={onClose} disabled={busy}>Cancel</Button>
          <Button variant="primary" onClick={() => void submit()} disabled={busy || !canSubmit}>
            {busy ? "Saving..." : "Add"}
          </Button>
        </div>
      )}
    >
      <div className="space-y-3">
        {hasLists && (
          <button
            type="button"
            onClick={() => setMode("existing")}
            disabled={busy}
            className={`flex w-full items-center justify-between rounded-[16px] border px-3 py-2.5 text-left transition ${
              mode === "existing"
                ? "border-[var(--primary)] bg-[var(--surface)]"
                : "border-[var(--border-default)] bg-[var(--bg-card)] hover:bg-[var(--surface)]"
            }`}
          >
            <div>
              <p className="text-sm font-medium text-[var(--text-primary)]">Add to existing list</p>
              <p className="mt-1 text-xs text-[var(--text-muted)]">Choose a saved lead list.</p>
            </div>
            <span
              className={`inline-flex h-4 w-4 rounded-full border ${
                mode === "existing"
                  ? "border-[var(--primary)] bg-[var(--primary)] shadow-[inset_0_0_0_3px_white]"
                  : "border-[var(--border-default)] bg-white"
              }`}
            />
          </button>
        )}
        {hasLists && mode === "existing" && (
          <div className="rounded-[16px] border border-[var(--border-default)] bg-[var(--surface)] p-2.5">
            <label className="mb-1.5 block text-[11px] font-medium uppercase tracking-[0.08em] text-[var(--text-muted)]">
              Saved lists
            </label>
            <div className="max-h-36 space-y-1.5 overflow-y-auto pr-1">
              {lists.map((list) => {
                const selected = selectedListId === list.id;
                return (
                  <button
                    key={list.id}
                    type="button"
                    onClick={() => setSelectedListId(list.id)}
                    disabled={busy}
                    className={`flex w-full items-center justify-between rounded-[12px] border px-3 py-2 text-sm transition ${
                      selected
                        ? "border-[var(--primary)] bg-white text-[var(--text-primary)]"
                        : "border-[var(--border-default)] bg-white text-[var(--text-secondary)] hover:border-[var(--ring)]"
                    }`}
                  >
                    <span className="truncate">{list.name}</span>
                    <span
                      className={`ml-3 inline-flex h-4 w-4 shrink-0 rounded-full border ${
                        selected
                          ? "border-[var(--primary)] bg-[var(--primary)] shadow-[inset_0_0_0_3px_white]"
                          : "border-[var(--border-default)] bg-white"
                      }`}
                    />
                  </button>
                );
              })}
            </div>
          </div>
        )}

        <button
          type="button"
          onClick={() => setMode("new")}
          disabled={busy}
          className={`flex w-full items-center justify-between rounded-[16px] border px-3 py-2.5 text-left transition ${
            mode === "new"
              ? "border-[var(--primary)] bg-[var(--surface)]"
              : "border-[var(--border-default)] bg-[var(--bg-card)] hover:bg-[var(--surface)]"
          }`}
        >
          <div>
            <p className="text-sm font-medium text-[var(--text-primary)]">Create new list</p>
            <p className="mt-1 text-xs text-[var(--text-muted)]">Name a new place to save this lead.</p>
          </div>
          <span
            className={`inline-flex h-4 w-4 rounded-full border ${
              mode === "new"
                ? "border-[var(--primary)] bg-[var(--primary)] shadow-[inset_0_0_0_3px_white]"
                : "border-[var(--border-default)] bg-white"
            }`}
          />
        </button>
        {mode === "new" && (
          <div className="rounded-[16px] border border-[var(--border-default)] bg-[var(--surface)] p-2.5">
            <Input
              value={newListName}
              onChange={(e) => setNewListName(e.target.value)}
              placeholder="List name"
              disabled={busy}
              className="h-9 rounded-[12px]"
            />
          </div>
        )}
        {error && <p className="text-sm text-red-600">{error}</p>}
      </div>
    </Modal>
  );
}
