"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import { getPublicSharedBrief } from "@/lib/api";
import { clientFacingAppError } from "@/lib/present";
import type { DiagnosticResponse } from "@/lib/types";
import { generateOpportunityFocus, generateWhyNow } from "@/lib/pitch";
import { getModeledUpsideDisplay } from "@/lib/revenueDisplay";

export default function SharedBriefPage() {
  const params = useParams();
  const token = String(params.token || "");
  const [result, setResult] = useState<DiagnosticResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!token) return;
    getPublicSharedBrief(token)
      .then(setResult)
      .catch((e) => setError(clientFacingAppError(e instanceof Error ? e.message : "Failed to load shared brief", "We couldn't open this shared brief right now.")))
      .finally(() => setLoading(false));
  }, [token]);

  if (loading) {
    return <main className="mx-auto max-w-3xl px-6 py-16 text-sm text-zinc-500">Loading shared brief…</main>;
  }
  if (error || !result) {
    return <main className="mx-auto max-w-3xl px-6 py-16 text-sm text-red-600">{error || "Shared brief not found"}</main>;
  }

  const b = result.brief;
  const ed = b?.executive_diagnosis;
  const mp = b?.market_position;
  const modeledUpside = getModeledUpsideDisplay(result);
  const preferredFocus = modeledUpside.serviceContext && modeledUpside.serviceContext !== "Primary gap"
    ? modeledUpside.serviceContext
    : "";
  const focus = generateOpportunityFocus(result, preferredFocus);
  const whyNow = generateWhyNow(result, focus);

  return (
    <main className="mx-auto max-w-3xl px-6 py-10">
      <h1 className="text-xl font-semibold tracking-tight">Revenue Intelligence Brief</h1>
      <p className="mt-1 text-sm text-zinc-600">
        {result.business_name} · {result.city}{result.state ? `, ${result.state}` : ""}
      </p>

      <section className="mt-6 space-y-2 rounded-xl border border-zinc-200 bg-white p-5 text-sm">
        <h2 className="font-medium text-zinc-800">Lead Summary</h2>
        <p><strong>Signal:</strong> {typeof ed?.opportunity_profile === "string" ? ed.opportunity_profile : ed?.opportunity_profile?.label ?? result.opportunity_profile}</p>
        <p><strong>Focus:</strong> {focus}</p>
        <p><strong>Why now:</strong> {whyNow}</p>
        {modeledUpside.mode === "range" ? (
          <p><strong>Annual Upside:</strong> {modeledUpside.value} <span className="text-zinc-500">{modeledUpside.context}</span></p>
        ) : null}
        <p><strong>Constraint:</strong> {ed?.constraint ?? result.constraint}</p>
        <p><strong>Primary Leverage:</strong> {ed?.primary_leverage ?? result.primary_leverage}</p>
      </section>

      <section className="mt-4 space-y-2 rounded-xl border border-zinc-200 bg-white p-5 text-sm">
        <h2 className="font-medium text-zinc-800">Market Position</h2>
        {mp?.revenue_band && <p><strong>Revenue Band:</strong> {mp.revenue_band}</p>}
        {mp?.reviews && <p><strong>Reviews:</strong> {mp.reviews}</p>}
        {mp?.local_avg && <p><strong>Local Avg:</strong> {mp.local_avg}</p>}
        {mp?.market_density && <p><strong>Market Density:</strong> {mp.market_density}</p>}
      </section>

      {b?.intervention_plan?.length ? (
        <section className="mt-4 space-y-2 rounded-xl border border-zinc-200 bg-white p-5 text-sm">
          <h2 className="font-medium text-zinc-800">Intervention Plan</h2>
          <ul className="list-inside list-disc space-y-1">
            {b.intervention_plan.map((step, idx) => <li key={idx}>{step}</li>)}
          </ul>
        </section>
      ) : null}
    </main>
  );
}
