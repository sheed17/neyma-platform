# Neyma Platform

Neyma is an AI-assisted prospect intelligence platform for dental go-to-market workflows.

It helps teams start from a market, rank the best opportunities, generate full diagnostic briefs, save prospects into working lists, track outcomes, and ask for leads in plain English.

The repo name used to describe just the lead-scoring engine. The product has grown beyond that. This codebase now contains the broader `neyma-platform` application: backend APIs, frontend workspace, ranking logic, brief generation, access control, and billing hooks.

---

## What Neyma Does

### 1. Territory Scan
- Start with a city, state, and dental vertical
- Discover and rank local practices
- Return a shortlist of the strongest opportunities before running full deep-dive analysis

### 2. Build Brief
- Generate a full diagnostic brief for a known business
- Combine place data, website signals, market context, service coverage, and decision logic
- Produce a sales-ready intelligence view rather than a bare score

### 3. Ask Neyma
- Turn a plain-English query into a ranked prospect search
- Support criteria like review gaps, website weakness, or missing service pages
- Let users build full briefs on-demand from Ask results

### 4. Lists and Workspace
- Save prospects into reusable lists
- Reopen scans and diagnostics from the dashboard
- Re-scan and manage lead groups over time

### 5. Outcome Tracking
- Record statuses like contacted, won, or lost
- Store embeddings and historical outcomes for retrieval and calibration loops
- Feed future intelligence with structural similarity and prior result patterns

### 6. Access and Billing
- Guest, free, and paid workflow limits
- Supabase-backed auth and account management
- Stripe checkout, customer portal, and webhook support for Neyma Pro

---

## Product Surface In This Repo

### Frontend
- Marketing landing page
- Auth flows: login, register, reset password
- Dashboard workspace
- Territory Scan flow
- Build Brief flow
- Ask Neyma flow
- Saved lists
- Settings, billing, and account deletion
- Shared/public brief pages

### Backend API
- Async diagnostic jobs
- Territory scan creation and polling
- Prospect shortlist retrieval
- Ask query resolution and results
- Saved lists and list membership
- Outcome tracking
- Access state and workspace membership
- Billing endpoints

### Intelligence Layer
- Google Places extraction and enrichment
- Website and conversion-path signal capture
- Objective decision logic
- Revenue and service-gap analysis
- Competitor and market context
- Hybrid retrieval over lead documents, embeddings, and outcomes

---

## How The Workflow Fits Together

1. Run a territory scan to rank a market quickly.
2. Open a shortlisted prospect and ensure a full brief only when it is worth deeper work.
3. Save strong prospects into lists.
4. Use Ask Neyma when you want to describe the target in natural language instead of starting from a market scan.
5. Track outreach outcomes so Neyma can learn from similar lead patterns over time.

---

## Tech Stack

- Backend: FastAPI, Python
- Frontend: Next.js 16, React 19, TypeScript
- ML/runtime logic: scikit-learn, NumPy, deterministic + LLM-assisted decision layers
- Data/auth: SQLite and Postgres-compatible access patterns, Supabase auth
- Payments: Stripe
- Browser/site inspection: Playwright

---

## Local Setup

### Backend

Install Python dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install
```

The backend loads env vars from the project-root `.env`.

Core env vars used by the platform:
- `GOOGLE_PLACES_API_KEY`
- `OPENAI_API_KEY`
- `SUPABASE_URL`
- `SUPABASE_ANON_KEY` or `SUPABASE_PUBLISHABLE_KEY`
- `SUPABASE_SERVICE_ROLE_KEY` for account deletion/admin flows
- `ACCESS_DATABASE_URL` or `SUPABASE_DB_URL` if using Postgres-backed access data
- `STRIPE_SECRET_KEY`, `STRIPE_PRO_PRICE_ID`, `STRIPE_WEBHOOK_SECRET`, `APP_BASE_URL` for billing
- `CORS_ORIGINS` and `RUN_EMBEDDED_WORKER` as optional backend settings

Run the API:

```bash
uvicorn backend.main:app --reload
```

Health check:

```bash
curl http://127.0.0.1:8000/health
```

### Frontend

Install frontend dependencies:

```bash
cd frontend
npm install
```

Create `frontend/.env.local` and set at least:

```bash
NEXT_PUBLIC_API_URL=http://127.0.0.1:8000
NEXT_PUBLIC_SUPABASE_URL=your_supabase_url
NEXT_PUBLIC_SUPABASE_ANON_KEY=your_supabase_anon_key
```

Run the app:

```bash
npm run dev
```

Open `http://localhost:3000`.

---

## Key API Paths

- `GET /health`
- `POST /diagnostic`
- `GET /jobs/{job_id}`
- `GET /diagnostics`
- `GET /diagnostics/{id}`
- `POST /territory`
- `GET /territory/scans`
- `GET /territory/{scan_id}`
- `GET /territory/{scan_id}/results`
- `POST /territory/prospects/{prospect_id}/ensure-brief`
- `POST /ask`
- `GET /ask/jobs/{job_id}/results`
- `POST /ask/prospects/ensure-brief`
- `GET /access/me`
- `POST /billing/checkout`
- `POST /billing/customer-portal`
- `POST /billing/webhook`

---

## Repo Structure

```text
neyma-platform/
├── backend/      # FastAPI app, routes, auth/access, workers, services
├── frontend/     # Next.js app, landing page, workspace, auth, billing UI
├── pipeline/     # Enrichment, scoring, retrieval, reasoning, exports
├── scripts/      # Operational scripts for scans, enrichment, training, exports, migrations
├── tests/        # Backend and pipeline tests
├── docs/         # Internal product and architecture notes
└── README.md
```

---

## Current Product Focus

Neyma is currently optimized for dental. The platform structure is broader than the original scoring engine, but the strongest workflow, ranking assumptions, and product language are all centered on dental practices today.

---

## License

MIT
