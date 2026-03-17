import type {
  AccessState,
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

export const getBaseUrl = () =>
  process.env.NEXT_PUBLIC_API_URL || "http://127.0.0.1:8000";

const STORAGE_KEY = "neyma_user";
const ACCESS_TOKEN_KEY = "neyma_access_token";

type StoredUser = {
  email: string;
  name: string;
};

export class ApiError extends Error {
  status: number;
  code?: string;
  recommendedCta?: string | null;
  access?: AccessState | null;

  constructor(
    message: string,
    options?: {
      status?: number;
      code?: string;
      recommendedCta?: string | null;
      access?: AccessState | null;
    },
  ) {
    super(message);
    this.name = "ApiError";
    this.status = options?.status ?? 500;
    this.code = options?.code;
    this.recommendedCta = options?.recommendedCta ?? null;
    this.access = options?.access ?? null;
  }
}

function getStoredUser(): StoredUser | null {
  if (typeof window === "undefined") return null;
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    return raw ? (JSON.parse(raw) as StoredUser) : null;
  } catch {
    return null;
  }
}

function authHeaders(headers?: HeadersInit): Headers {
  const next = new Headers(headers || {});
  const token = typeof window !== "undefined" ? window.localStorage.getItem(ACCESS_TOKEN_KEY) : null;
  if (token) {
    next.set("Authorization", `Bearer ${token}`);
  }
  const user = getStoredUser();
  if (user?.email && user.email.endsWith("@neyma.local")) {
    next.set("X-Neyma-User-Email", user.email);
    next.set("X-Neyma-User-Name", user.name || user.email.split("@")[0]);
  }
  return next;
}

async function parseError(res: Response): Promise<ApiError> {
  const err = await res.json().catch(() => ({ detail: res.statusText }));
  const detail = err?.detail;
  if (detail && typeof detail === "object") {
    return new ApiError(detail.message || res.statusText, {
      status: res.status,
      code: detail.code,
      recommendedCta: detail.recommended_cta,
      access: detail.access || null,
    });
  }
  return new ApiError(detail || res.statusText, { status: res.status });
}

async function apiFetch(input: string, init?: RequestInit): Promise<Response> {
  return globalThis.fetch(input, {
    credentials: "include",
    ...init,
    headers: authHeaders(init?.headers),
  });
}

function timeoutSignal(ms: number): AbortSignal {
  const controller = new AbortController();
  setTimeout(() => controller.abort(), ms);
  return controller.signal;
}

export async function checkHealth(): Promise<{ status: string }> {
  const res = await apiFetch(`${getBaseUrl()}/health`, { cache: "no-store" });
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
  const res = await apiFetch(`${getBaseUrl()}/diagnostic`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    throw await parseError(res);
  }
  return res.json();
}

export async function getJobStatus(jobId: string): Promise<JobStatusResponse> {
  const res = await apiFetch(`${getBaseUrl()}/jobs/${jobId}`, { cache: "no-store" });
  if (!res.ok) {
    throw await parseError(res);
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
  let res: Response;
  try {
    res = await apiFetch(
      `${getBaseUrl()}/diagnostics?limit=${limit}&offset=${offset}`,
      { cache: "no-store" },
    );
  } catch {
    throw new Error(`Unable to reach the API at ${getBaseUrl()}. Start the backend or set NEXT_PUBLIC_API_URL.`);
  }
  if (!res.ok) throw await parseError(res);
  return res.json();
}

export async function getDiagnostic(id: number): Promise<DiagnosticResponse> {
  const res = await apiFetch(`${getBaseUrl()}/diagnostics/${id}`, { cache: "no-store" });
  if (!res.ok) {
    throw await parseError(res);
  }
  return res.json();
}

export async function getPublicSharedBrief(token: string): Promise<DiagnosticResponse> {
  const res = await apiFetch(`${getBaseUrl()}/brief/s/${token}`, { cache: "no-store" });
  if (!res.ok) {
    throw await parseError(res);
  }
  return res.json();
}

export async function createDiagnosticShareLink(id: number): Promise<DiagnosticShareResponse> {
  const res = await apiFetch(`${getBaseUrl()}/diagnostics/${id}/share`, { method: "POST" });
  if (!res.ok) {
    throw await parseError(res);
  }
  return res.json();
}

export function getDiagnosticBriefPdfUrl(id: number): string {
  return `${getBaseUrl()}/diagnostics/${id}/brief.pdf`;
}

export async function deleteDiagnostic(id: number): Promise<void> {
  const res = await apiFetch(`${getBaseUrl()}/diagnostics/${id}`, { method: "DELETE" });
  if (!res.ok) throw await parseError(res);
}

export async function recordOutcome(body: {
  diagnostic_id: number;
  outcome_type: string;
  outcome_data: Record<string, unknown>;
}): Promise<{ success: boolean; message: string }> {
  const res = await apiFetch(`${getBaseUrl()}/outcomes`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    throw await parseError(res);
  }
  return res.json();
}

export async function getCalibrationStats(): Promise<Record<string, unknown>> {
  const res = await apiFetch(`${getBaseUrl()}/outcomes/calibration`, { cache: "no-store" });
  if (!res.ok) throw await parseError(res);
  return res.json();
}

export async function getOutcomes(diagnosticId: number): Promise<Array<Record<string, unknown>>> {
  const res = await apiFetch(`${getBaseUrl()}/outcomes/${diagnosticId}`, { cache: "no-store" });
  if (!res.ok) throw await parseError(res);
  return res.json();
}

export async function createTerritoryScan(
  body: TerritoryScanRequest,
): Promise<TerritoryScanCreateResponse> {
  const res = await apiFetch(`${getBaseUrl()}/territory`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    throw await parseError(res);
  }
  return res.json();
}

export async function getTerritoryScanStatus(scanId: string): Promise<TerritoryScanStatusResponse> {
  const res = await apiFetch(`${getBaseUrl()}/territory/${scanId}`, { cache: "no-store" });
  if (!res.ok) {
    throw await parseError(res);
  }
  return res.json();
}

export async function getTerritoryScanResults(scanId: string): Promise<TerritoryScanResultsResponse> {
  let lastError: Error | null = null;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await apiFetch(`${getBaseUrl()}/territory/${scanId}/results`, {
        cache: "no-store",
        signal: timeoutSignal(12000),
      });
      if (!res.ok) {
        throw await parseError(res);
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
  const res = await apiFetch(`${getBaseUrl()}/lists`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name }),
  });
  if (!res.ok) {
    throw await parseError(res);
  }
  return res.json();
}

export async function getProspectLists(): Promise<ProspectListsResponse> {
  const res = await apiFetch(`${getBaseUrl()}/lists`, { cache: "no-store" });
  if (!res.ok) throw await parseError(res);
  return res.json();
}

export async function addProspectsToList(listId: number, diagnosticIds: number[]): Promise<{ added: number }> {
  const res = await apiFetch(`${getBaseUrl()}/lists/${listId}/members`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ diagnostic_ids: diagnosticIds }),
  });
  if (!res.ok) {
    throw await parseError(res);
  }
  return res.json();
}

export async function getListMembers(listId: number): Promise<ProspectListMembersResponse> {
  const res = await apiFetch(`${getBaseUrl()}/lists/${listId}/members`, { cache: "no-store" });
  if (!res.ok) {
    throw await parseError(res);
  }
  return res.json();
}

export async function removeListMember(listId: number, diagnosticId: number): Promise<void> {
  const res = await apiFetch(`${getBaseUrl()}/lists/${listId}/members/${diagnosticId}`, { method: "DELETE" });
  if (!res.ok) throw await parseError(res);
}

export async function rescanList(listId: number): Promise<{ scan_id: string; status: string; message: string }> {
  const res = await apiFetch(`${getBaseUrl()}/lists/${listId}/rescan`, { method: "POST" });
  if (!res.ok) {
    throw await parseError(res);
  }
  return res.json();
}

export async function markProspectOutcome(body: {
  diagnostic_id: number;
  status: "contacted" | "closed_won" | "closed_lost";
  note?: string;
}): Promise<{ diagnostic_id: number; status: string; note?: string | null }> {
  const res = await apiFetch(`${getBaseUrl()}/diagnostics/${body.diagnostic_id}/outcome`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ status: body.status, note: body.note }),
  });
  if (!res.ok) {
    throw await parseError(res);
  }
  return res.json();
}

export async function ensureTerritoryProspectBrief(
  prospectId: number,
): Promise<{ prospect_id: number; status: "ready" | "building"; diagnostic_id?: number; job_id?: string }> {
  const res = await apiFetch(`${getBaseUrl()}/territory/prospects/${prospectId}/ensure-brief`, {
    method: "POST",
  });
  if (!res.ok) {
    throw await parseError(res);
  }
  return res.json();
}

export async function startTerritoryDeepScan(
  scanId: string,
  body?: { max_prospects?: number; concurrency?: number },
): Promise<DeepBriefStartResponse> {
  const res = await apiFetch(`${getBaseUrl()}/territory/${scanId}/deep-scan`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      max_prospects: body?.max_prospects ?? 25,
      concurrency: body?.concurrency ?? 3,
    }),
  });
  if (!res.ok) {
    throw await parseError(res);
  }
  return res.json();
}

export async function startListDeepBriefs(
  listId: number,
  body?: { max_prospects?: number; concurrency?: number },
): Promise<DeepBriefStartResponse> {
  const res = await apiFetch(`${getBaseUrl()}/lists/${listId}/deep-briefs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      max_prospects: body?.max_prospects ?? 25,
      concurrency: body?.concurrency ?? 3,
    }),
  });
  if (!res.ok) {
    throw await parseError(res);
  }
  return res.json();
}

export async function getRecentTerritoryScans(limit = 20): Promise<TerritoryScansResponse> {
  const res = await apiFetch(`${getBaseUrl()}/territory/scans?limit=${limit}`, { cache: "no-store" });
  if (!res.ok) {
    throw await parseError(res);
  }
  return res.json();
}

export async function getAccessState(): Promise<AccessState> {
  const res = await apiFetch(`${getBaseUrl()}/access/me`, { cache: "no-store" });
  if (!res.ok) {
    throw await parseError(res);
  }
  return res.json();
}

export async function bootstrapGuestSession(): Promise<AccessState> {
  const res = await apiFetch(`${getBaseUrl()}/access/guest-session`, { method: "POST" });
  if (!res.ok) {
    throw await parseError(res);
  }
  return res.json();
}

export async function getWorkspaceMembers(): Promise<{ items: Array<Record<string, unknown>> }> {
  const res = await apiFetch(`${getBaseUrl()}/access/workspace/members`, { cache: "no-store" });
  if (!res.ok) {
    throw await parseError(res);
  }
  return res.json();
}

export async function inviteWorkspaceMember(body: {
  email: string;
  name?: string;
  role?: string;
}): Promise<{ member: Record<string, unknown> }> {
  const res = await apiFetch(`${getBaseUrl()}/access/workspace/members`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    throw await parseError(res);
  }
  return res.json();
}

export async function removeWorkspaceMember(userId: number): Promise<{ removed: boolean }> {
  const res = await apiFetch(`${getBaseUrl()}/access/workspace/members/${userId}`, {
    method: "DELETE",
  });
  if (!res.ok) {
    throw await parseError(res);
  }
  return res.json();
}

export async function getOutcomesSummary(): Promise<OutcomesSummaryResponse> {
  const res = await apiFetch(`${getBaseUrl()}/diagnostics/outcomes/summary`, { cache: "no-store" });
  if (!res.ok) {
    throw await parseError(res);
  }
  return res.json();
}

export async function getOutcomesList(limit = 200): Promise<{ items: OutcomesListItem[] }> {
  const res = await apiFetch(`${getBaseUrl()}/diagnostics/outcomes?limit=${limit}`, { cache: "no-store" });
  if (!res.ok) {
    throw await parseError(res);
  }
  return res.json();
}

export async function runAskQuery(
  query: string,
  confirmedLowConfidence = false,
): Promise<AskStartResponse> {
  const res = await apiFetch(`${getBaseUrl()}/ask`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query, confirmed_low_confidence: confirmedLowConfidence }),
  });
  if (!res.ok) {
    throw await parseError(res);
  }
  return res.json();
}

export async function getAskResults(jobId: string): Promise<AskResultsResponse> {
  const res = await apiFetch(`${getBaseUrl()}/ask/jobs/${jobId}/results`, { cache: "no-store" });
  if (!res.ok) {
    throw await parseError(res);
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
  const res = await apiFetch(`${getBaseUrl()}/ask/prospects/ensure-brief`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    throw await parseError(res);
  }
  return res.json();
}
