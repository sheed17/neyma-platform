import type {
  DiagnosticResponse,
  JobSubmitResponse,
  JobStatusResponse,
  DiagnosticListResponse,
  TerritoryScanRequest,
  TerritoryScanCreateResponse,
  TerritoryScanStatusResponse,
  TerritoryScanResultsResponse,
  TerritoryScansResponse,
  ProspectListsResponse,
  ProspectListMembersResponse,
  DiagnosticShareResponse,
  OutcomesSummaryResponse,
  OutcomesListItem,
  DeepBriefStartResponse,
  AskStartResponse,
  AskResultsResponse,
  AskEnsureBriefResponse,
} from "./types";

const getBaseUrl = () =>
  process.env.NEXT_PUBLIC_API_URL || "http://127.0.0.1:8000";

function timeoutSignal(ms: number): AbortSignal {
  const controller = new AbortController();
  setTimeout(() => controller.abort(), ms);
  return controller.signal;
}

export async function checkHealth(): Promise<{ status: string }> {
  const res = await fetch(`${getBaseUrl()}/health`, { cache: "no-store" });
  if (!res.ok) throw new Error("Health check failed");
  return res.json();
}

export async function submitDiagnostic(body: {
  business_name: string;
  city: string;
  state: string;
  website?: string;
  deep_audit?: boolean;
  source_diagnostic_id?: number;
}): Promise<JobSubmitResponse> {
  const res = await fetch(`${getBaseUrl()}/diagnostic`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}

export async function getJobStatus(jobId: string): Promise<JobStatusResponse> {
  const res = await fetch(`${getBaseUrl()}/jobs/${jobId}`, { cache: "no-store" });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}

export async function pollUntilDone(
  jobId: string,
  onStatus?: (s: JobStatusResponse) => void,
  intervalMs = 2000,
  maxAttempts = 150,
): Promise<JobStatusResponse> {
  for (let i = 0; i < maxAttempts; i++) {
    const status = await getJobStatus(jobId);
    onStatus?.(status);
    if (status.status === "completed" || status.status === "failed") return status;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new Error("Job timed out");
}

export async function listDiagnostics(
  limit = 50,
  offset = 0,
): Promise<DiagnosticListResponse> {
  const res = await fetch(
    `${getBaseUrl()}/diagnostics?limit=${limit}&offset=${offset}`,
    { cache: "no-store" },
  );
  if (!res.ok) throw new Error("Failed to load diagnostics");
  return res.json();
}

export async function getDiagnostic(id: number): Promise<DiagnosticResponse> {
  const res = await fetch(`${getBaseUrl()}/diagnostics/${id}`, { cache: "no-store" });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}

export async function getPublicSharedBrief(token: string): Promise<DiagnosticResponse> {
  const res = await fetch(`${getBaseUrl()}/brief/s/${token}`, { cache: "no-store" });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}

export async function createDiagnosticShareLink(id: number): Promise<DiagnosticShareResponse> {
  const res = await fetch(`${getBaseUrl()}/diagnostics/${id}/share`, { method: "POST" });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}

export function getDiagnosticBriefPdfUrl(id: number): string {
  return `${getBaseUrl()}/diagnostics/${id}/brief.pdf`;
}

export async function deleteDiagnostic(id: number): Promise<void> {
  const res = await fetch(`${getBaseUrl()}/diagnostics/${id}`, { method: "DELETE" });
  if (!res.ok) throw new Error("Failed to delete diagnostic");
}

export async function recordOutcome(body: {
  diagnostic_id: number;
  outcome_type: string;
  outcome_data: Record<string, unknown>;
}): Promise<{ success: boolean; message: string }> {
  const res = await fetch(`${getBaseUrl()}/outcomes`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}

export async function getCalibrationStats(): Promise<Record<string, unknown>> {
  const res = await fetch(`${getBaseUrl()}/outcomes/calibration`, { cache: "no-store" });
  if (!res.ok) throw new Error("Failed to fetch calibration stats");
  return res.json();
}

export async function getOutcomes(diagnosticId: number): Promise<Array<Record<string, unknown>>> {
  const res = await fetch(`${getBaseUrl()}/outcomes/${diagnosticId}`, { cache: "no-store" });
  if (!res.ok) throw new Error("Failed to fetch outcomes");
  return res.json();
}

export async function createTerritoryScan(
  body: TerritoryScanRequest,
): Promise<TerritoryScanCreateResponse> {
  const res = await fetch(`${getBaseUrl()}/territory`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}

export async function getTerritoryScanStatus(scanId: string): Promise<TerritoryScanStatusResponse> {
  const res = await fetch(`${getBaseUrl()}/territory/${scanId}`, { cache: "no-store" });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}

export async function getTerritoryScanResults(scanId: string): Promise<TerritoryScanResultsResponse> {
  let lastError: Error | null = null;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await fetch(`${getBaseUrl()}/territory/${scanId}/results`, {
        cache: "no-store",
        signal: timeoutSignal(12000),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({ detail: res.statusText }));
        throw new Error(err.detail || res.statusText);
      }
      return res.json();
    } catch (err) {
      lastError = err instanceof Error ? err : new Error("Failed to fetch territory results");
      if (attempt < 2) {
        await new Promise((r) => setTimeout(r, 1000 * (attempt + 1)));
      }
    }
  }
  throw lastError || new Error("Failed to fetch territory results");
}

export async function createProspectList(name: string): Promise<{ id: number; name: string }> {
  const res = await fetch(`${getBaseUrl()}/lists`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}

export async function getProspectLists(): Promise<ProspectListsResponse> {
  const res = await fetch(`${getBaseUrl()}/lists`, { cache: "no-store" });
  if (!res.ok) throw new Error("Failed to fetch lists");
  return res.json();
}

export async function addProspectsToList(listId: number, diagnosticIds: number[]): Promise<{ added: number }> {
  const res = await fetch(`${getBaseUrl()}/lists/${listId}/members`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ diagnostic_ids: diagnosticIds }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}

export async function getListMembers(listId: number): Promise<ProspectListMembersResponse> {
  const res = await fetch(`${getBaseUrl()}/lists/${listId}/members`, { cache: "no-store" });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}

export async function removeListMember(listId: number, diagnosticId: number): Promise<void> {
  const res = await fetch(`${getBaseUrl()}/lists/${listId}/members/${diagnosticId}`, { method: "DELETE" });
  if (!res.ok) throw new Error("Failed to remove list member");
}

export async function rescanList(listId: number): Promise<{ scan_id: string; status: string; message: string }> {
  const res = await fetch(`${getBaseUrl()}/lists/${listId}/rescan`, { method: "POST" });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}

export async function markProspectOutcome(body: {
  diagnostic_id: number;
  status: "contacted" | "closed_won" | "closed_lost";
  note?: string;
}): Promise<{ diagnostic_id: number; status: string; note?: string | null }> {
  const res = await fetch(`${getBaseUrl()}/diagnostics/${body.diagnostic_id}/outcome`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ status: body.status, note: body.note }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}

export async function ensureTerritoryProspectBrief(
  prospectId: number,
): Promise<{ prospect_id: number; status: "ready" | "building"; diagnostic_id?: number; job_id?: string }> {
  const res = await fetch(`${getBaseUrl()}/territory/prospects/${prospectId}/ensure-brief`, {
    method: "POST",
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}

export async function startTerritoryDeepScan(
  scanId: string,
  body?: { max_prospects?: number; concurrency?: number },
): Promise<DeepBriefStartResponse> {
  const res = await fetch(`${getBaseUrl()}/territory/${scanId}/deep-scan`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      max_prospects: body?.max_prospects ?? 25,
      concurrency: body?.concurrency ?? 3,
    }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}

export async function startListDeepBriefs(
  listId: number,
  body?: { max_prospects?: number; concurrency?: number },
): Promise<DeepBriefStartResponse> {
  const res = await fetch(`${getBaseUrl()}/lists/${listId}/deep-briefs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      max_prospects: body?.max_prospects ?? 25,
      concurrency: body?.concurrency ?? 3,
    }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}

export async function getRecentTerritoryScans(limit = 20): Promise<TerritoryScansResponse> {
  const res = await fetch(`${getBaseUrl()}/territory/scans?limit=${limit}`, { cache: "no-store" });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}

export async function getOutcomesSummary(): Promise<OutcomesSummaryResponse> {
  const res = await fetch(`${getBaseUrl()}/diagnostics/outcomes/summary`, { cache: "no-store" });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}

export async function getOutcomesList(limit = 200): Promise<{ items: OutcomesListItem[] }> {
  const res = await fetch(`${getBaseUrl()}/diagnostics/outcomes?limit=${limit}`, { cache: "no-store" });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}

export async function runAskQuery(
  query: string,
  confirmedLowConfidence = false,
): Promise<AskStartResponse> {
  const res = await fetch(`${getBaseUrl()}/ask`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query, confirmed_low_confidence: confirmedLowConfidence }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}

export async function getAskResults(jobId: string): Promise<AskResultsResponse> {
  const res = await fetch(`${getBaseUrl()}/ask/jobs/${jobId}/results`, { cache: "no-store" });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}

export async function ensureAskProspectBrief(body: {
  place_id?: string | null;
  business_name: string;
  city: string;
  state?: string | null;
  website?: string | null;
}): Promise<AskEnsureBriefResponse> {
  const res = await fetch(`${getBaseUrl()}/ask/prospects/ensure-brief`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}
