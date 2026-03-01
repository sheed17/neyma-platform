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
          <label className="flex items-center gap-2 text-sm text-[var(--text-secondary)]">
            <input
              type="radio"
              checked={mode === "existing"}
              onChange={() => setMode("existing")}
              disabled={busy}
            />
            Add to existing list
          </label>
        )}
        {hasLists && mode === "existing" && (
          <select
            className="h-10 w-full rounded-[var(--radius-md)] border border-[var(--border-default)] px-3 text-sm"
            value={selectedListId ?? ""}
            onChange={(e) => setSelectedListId(Number(e.target.value))}
            disabled={busy}
          >
            {lists.map((list) => (
              <option key={list.id} value={list.id}>{list.name}</option>
            ))}
          </select>
        )}

        <label className="flex items-center gap-2 text-sm text-[var(--text-secondary)]">
          <input
            type="radio"
            checked={mode === "new"}
            onChange={() => setMode("new")}
            disabled={busy}
          />
          Create new list
        </label>
        {mode === "new" && (
          <Input
            value={newListName}
            onChange={(e) => setNewListName(e.target.value)}
            placeholder="List name"
            disabled={busy}
          />
        )}
        {error && <p className="text-sm text-red-600">{error}</p>}
      </div>
    </Modal>
  );
}
