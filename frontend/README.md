# Lead Scoring Engine — Frontend

Minimal Next.js UI to run the diagnostic API and view results.

## Setup

```bash
npm install
cp .env.local.example .env.local   # optional: set NEXT_PUBLIC_API_URL (default http://127.0.0.1:8000)
```

## Run

Start the **backend** first (from project root):

```bash
uvicorn backend.main:app --reload
```

Then start the frontend:

```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000).

## What it does

- **API status** — Shows whether the backend is reachable (green/red dot).
- **Diagnostic form** — Business name (required), city (required), website (optional). Submits to `POST /diagnostic`.
- **Result** — Displays lead_id, opportunity profile, constraint, primary leverage, market density, review position, paid status, and intervention plan.
