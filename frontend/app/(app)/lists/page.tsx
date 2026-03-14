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
          <p className="text-sm text-[var(--text-muted)]">Save shortlisted prospects, track outcomes, and revisit priority accounts.</p>
        </div>
        <div className="flex gap-2">
          <Link href="/territory/new"><Button>Run territory scan</Button></Link>
          <Button variant="primary" onClick={() => setOpen(true)}>New list</Button>
        </div>
      </div>

      {loading ? (
        <Skeleton className="h-64" />
      ) : items.length === 0 ? (
        <EmptyState
          title="No lists yet"
          description="Create a list to organize prospects from territory scans or Ask results."
          action={<Button variant="primary" onClick={() => setOpen(true)}>Create list</Button>}
        />
      ) : (
        <Card className="border border-[var(--border-default)] bg-[var(--bg-card)] shadow-[0_18px_40px_rgba(10,10,10,0.04)]">
          <CardHeader title="Saved Prospect Lists" />
          <Table>
            <THead><tr><TH>Name</TH><TH>Members</TH><TH>Created</TH><TH className="text-right">Open</TH></tr></THead>
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
        </Card>
      )}

      <Modal
        open={open}
        title="Create Prospect List"
        onClose={() => setOpen(false)}
        footer={
          <div className="flex justify-end gap-2">
            <Button onClick={() => setOpen(false)}>Cancel</Button>
            <Button variant="primary" onClick={() => void createList()}>Create</Button>
          </div>
        }
      >
        <Input label="List name" value={name} onChange={(e) => setName(e.target.value)} placeholder="Q2 Implant Prospects" />
      </Modal>
    </div>
  );
}
