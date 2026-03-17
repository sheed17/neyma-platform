"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { createProspectList, getProspectLists } from "@/lib/api";
import type { ProspectList } from "@/lib/types";
import Button from "@/app/components/ui/Button";
import { Card, CardHeader } from "@/app/components/ui/Card";
import { Table, THead, TH, TR, TD } from "@/app/components/ui/Table";
import EmptyState from "@/app/components/ui/EmptyState";
import Modal from "@/app/components/ui/Modal";
import Input from "@/app/components/ui/Input";
import { Skeleton } from "@/app/components/ui/Skeleton";

export default function ListsPage() {
  const [items, setItems] = useState<ProspectList[]>([]);
  const [loading, setLoading] = useState(true);
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");

  async function load() {
    setLoading(true);
    try {
      const data = await getProspectLists();
      setItems(data.items);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void load();
  }, []);

  async function createList() {
    if (!name.trim()) return;
    await createProspectList(name.trim());
    setName("");
    setOpen(false);
    await load();
  }

  return (
    <div className="mx-auto max-w-6xl">
      <div className="mb-5 flex flex-wrap items-end justify-between gap-3">
        <div>
          <p className="section-kicker">Workspace</p>
          <h1 className="page-title">Lists</h1>
          <p className="text-sm text-[var(--text-muted)]">Save leads worth revisiting, group them by market or focus, and reopen the strongest opportunities fast.</p>
        </div>
        <div className="flex w-full flex-col gap-2 sm:w-auto sm:flex-row">
          <Link href="/territory/new" className="w-full sm:w-auto"><Button className="w-full sm:w-auto">Run territory scan</Button></Link>
          <Button variant="primary" onClick={() => setOpen(true)} className="w-full sm:w-auto">New list</Button>
        </div>
      </div>

      {loading ? (
        <Skeleton className="h-64" />
      ) : items.length === 0 ? (
        <EmptyState
          title="No lists yet"
          description="Create a list to save leads from territory scans or Ask results."
          action={<Button variant="primary" onClick={() => setOpen(true)}>Create list</Button>}
        />
      ) : (
        <Card className="border border-[var(--border-default)] bg-[var(--bg-card)] shadow-[0_18px_40px_rgba(10,10,10,0.04)]">
          <CardHeader title="Saved Lead Lists" subtitle="Lead groups you can reopen, review, and refresh when the market changes." />
          <div className="space-y-3 p-4 sm:hidden">
            {items.map((list) => (
              <div key={list.id} className="rounded-[16px] border border-[var(--border-default)] bg-white p-4">
                <p className="text-[14px] font-medium text-[var(--text-primary)]">{list.name}</p>
                <p className="mt-1 text-[12px] text-[var(--text-muted)]">{list.members_count ?? 0} saved leads</p>
                <p className="mt-1 text-[12px] text-[var(--text-muted)]">
                  Created {list.created_at ? new Date(list.created_at).toLocaleDateString("en-US") : "-"}
                </p>
                <Link href={`/lists/${list.id}`} className="mt-3 inline-flex text-sm font-medium text-[var(--primary)] hover:underline">Open list</Link>
              </div>
            ))}
          </div>
          <div className="hidden sm:block">
            <Table>
              <THead><tr><TH>Name</TH><TH>Saved Leads</TH><TH>Created</TH><TH className="text-right">Open</TH></tr></THead>
              <tbody>
                {items.map((list) => (
                  <TR key={list.id}>
                    <TD className="font-medium text-[var(--text-primary)]">{list.name}</TD>
                    <TD>{list.members_count ?? 0}</TD>
                    <TD>{list.created_at ? new Date(list.created_at).toLocaleDateString("en-US") : "-"}</TD>
                    <TD className="text-right"><Link href={`/lists/${list.id}`} className="app-link font-medium">View</Link></TD>
                  </TR>
                ))}
              </tbody>
            </Table>
          </div>
        </Card>
      )}

      <Modal
        open={open}
        title="Create Saved Lead List"
        onClose={() => setOpen(false)}
        footer={
          <div className="flex flex-col justify-end gap-2 sm:flex-row">
            <Button onClick={() => setOpen(false)} className="w-full sm:w-auto">Cancel</Button>
            <Button variant="primary" onClick={() => void createList()} className="w-full sm:w-auto">Create</Button>
          </div>
        }
      >
        <Input label="List name" value={name} onChange={(e) => setName(e.target.value)} placeholder="Q2 Implant Prospects" />
      </Modal>
    </div>
  );
}
