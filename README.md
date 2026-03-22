# Lead Scoring Engine

An intelligence platform that extracts, enriches, and analyzes business leads from Google Places—built for the dental vertical and production deployment.

We decide which businesses are worth pursuing and explain why. The system delivers **context over scores**: reasoning, dimensions, outreach angles, and evidence so sales teams can act with confidence.

---

## What We Built

### End-to-end pipeline
- **Extraction** — Geographic tiling, keyword expansion, and pagination to collect leads from Google Places Nearby Search
- **Enrichment** — Place Details, website signals, Meta Ads detection, and competitor sampling
- **Intelligence** — Objective decision layer, revenue modeling, and deterministic context for each lead

### Dental vertical
The system is specialized for dental practices. For each lead we produce:

- **Objective Intelligence** — Root constraint, primary growth vector, service gaps (high-ticket detected vs missing landing pages), competitive profile (market density, review tier, nearest competitors)
- **Revenue Intelligence Brief** — Executive diagnosis, market position, demand signals (Google Ads, Meta Ads), high-value service gaps, modeled revenue upside, strategic gap analysis, conversion infrastructure, risk flags, and intervention plan
- **Opportunity Profile** — Deterministic label (High-Leverage | Moderate | Low-Leverage) with short parenthetical reasoning

### Signals and context
- Website: SSL, mobile-friendly, contact forms, booking widgets, schema markup
- Reviews: Count, rating, recency, velocity
- Paid demand: Google Ads and Meta Ads presence (no spend estimates; factual status only)
- Service depth: High-ticket procedures detected, missing dedicated pages, schema coverage

### Outcome loop
- **Embeddings** — Structural snapshot per lead (objective state) stored in SQLite for similarity search
- **Lead outcomes** — Contacted, proposal sent, closed, close value, service sold
- **Similarity stats** — Conversion rates and top service sold across similar historical profiles (used for UI and future analytics)

### Hybrid RAG (cohort + similarity + outcomes)
- **Typed lead docs (`lead_docs_v1`)**: `signal_profile`, `service_coverage`, `market_context`, `conversion_path`, `llm_brief_summary`
- **Cohort retrieval**: SQL-filtered peers by vertical/city/review-gap bucket/market-density
- **Vector retrieval**: semantic nearest docs from other leads (when embeddings available)
- **Outcome patterns**: observed constraint/leverage/outreach patterns weighted by booked/closed outcomes
- **Guardrails**: dentist LLM receives only retrieved context and must emit strict JSON with evidence references

---

## Architecture

| Component | Description |
|-----------|-------------|
| **Extraction** | `run_pipeline.py` — Grid-based Nearby Search with keyword expansion |
| **Enrichment** | `run_enrichment.py` — Place Details, signals, Meta Ads, competitors, embeddings |
| **Upload** | `run_upload.py` — Enrich uploaded leads (CSV/JSON) through the same pipeline |
| **Export** | `export_leads.py` — Context-first or legacy export from DB |
| **Briefs** | `render_brief.py` — Revenue Intelligence Brief HTML per lead |
| **Outcomes** | `update_outcome.py` — Create/update outcome records for the loop |
| **Hybrid RAG** | `pipeline/doc_builder.py` + `pipeline/rag/hybrid_retriever.py` |
| **API** | FastAPI backend — `POST /diagnostic` for single-lead enrichment |

### Lightweight sourcing

For branch-local prospect sourcing without the full app stack, run:

```bash
python scripts/source_local_leads.py --region "San Jose, CA"
```

This uses Google Geocoding + Nearby Search + Place Details, keeps dentist practice filtering, and exports only lightweight outreach fields to `output/sourcing/`:
- `name`
- `address`
- `rating`
- `review_count`
- `phone`
- `website`

### Run the API server

From the project root:

```bash
uvicorn backend.main:app --reload
```

- Health check: `GET /health`
- Diagnostic: `POST /diagnostic` with body `{"business_name": "Example Dental", "city": "San Jose"}` (website optional)

### Territory + lists workflow

New workflow path (single-diagnostic remains unchanged):

- `POST /territory` — start async Tier 1 territory scan `{ city, state?, vertical, limit?, filters? }`
- `GET /territory/{scan_id}` — poll scan status/progress
- `GET /territory/{scan_id}/results` — Tier 1 ranked prospect rows (Places + Place Details + single-homepage checks)
- `POST /territory/prospects/{prospect_id}/ensure-brief` — run/attach Tier 2 full diagnostic on demand
- `POST /lists`, `GET /lists` — create/list saved prospect lists
- `POST /lists/{id}/members`, `GET /lists/{id}/members`, `DELETE /lists/{id}/members/{diagnostic_id}` — list membership
- `POST /lists/{id}/rescan` — async re-scan current list members
- `POST /diagnostics/{id}/outcome` — mark `contacted | closed_won | closed_lost`

Tier 1 ranking is deterministic and lightweight: review count/rating plus basic website infrastructure checks (SSL, contact form, phone, viewport/schema).
Tier 2 remains the existing full diagnostic pipeline and runs only when a user requests a brief or adds a prospect to a list.

### Run the frontend (Next.js)

From the project root, start the API first, then:

```bash
cd frontend
cp .env.local.example .env.local   # optional: edit NEXT_PUBLIC_API_URL if backend runs elsewhere
npm run dev
```

Open [http://localhost:3000](http://localhost:3000). The UI shows API status and a diagnostic form (business name, city, optional website) and displays the structured result.

### Measure Capture Accuracy (gold-set benchmark)

Use this to measure real precision/recall/F1 for brief-critical capture signals:
- online booking
- contact form
- phone prominence
- phone clickable
- CTA presence
- form structure accuracy

1. Prepare input CSV with at least:
`business_name,city,state,website`

2. Generate model predictions:

```bash
python3 scripts/generate_capture_benchmark.py \
  --csv data/capture_benchmark_input.csv \
  --output output/capture_benchmark_predictions.csv
```

3. Human-label the same CSV by filling:
`label_online_booking,label_contact_form,label_phone_prominent,label_phone_clickable,label_has_cta,label_form_structure`
using:
- bool labels: `true` or `false`
- form structure: `single_step`, `multi_step`, `none`, or `unknown`

4. Evaluate:

```bash
python3 scripts/evaluate_capture_benchmark.py \
  --csv output/capture_benchmark_predictions.csv \
  --json-out output/capture_benchmark_metrics.json
```

The evaluator prints per-signal precision/recall/F1/accuracy and summary macro/micro F1.

### Database
- SQLite: runs, leads, signals, decisions, embeddings (versioned), outcomes
- Tables: `runs`, `leads`, `lead_signals`, `decisions`, `lead_embeddings_v2`, `lead_outcomes`, `lead_intel_v1`, `lead_docs_v1`

### Key modules
- **Objective intelligence** — Root bottleneck, growth vector, service intel, competitive profile
- **Revenue brief renderer** — Deterministic HTML and view model (no LLM in brief)
- **Embedding snapshot** — Structural text for embeddings (objective state)
- **Outcome stats** — Similarity-based conversion metrics

---

## Revenue Intelligence Brief

Each dental lead gets a Revenue Intelligence Brief that includes:

- **Executive Diagnosis** — Constraint, primary leverage, opportunity profile, modeled revenue upside
- **Market Position** — Revenue band, reviews, local avg, market density
- **Competitive Context** — Dentists sampled, lead vs market, nearest competitors
- **Demand Signals** — Google Ads status (Search campaigns detected / Not detected), Meta Ads (Active / Not detected), estimated traffic, last review, review velocity
- **Local SEO & High-Value Service Pages** — Detected services, missing pages, schema
- **Modeled Revenue Upside** — Primary service capture gap (conservative bands, 30% cap vs revenue band)
- **Strategic Gap** — Nearest competitor, capture gap narrative
- **Conversion Infrastructure** — Online booking, contact form, phone, mobile
- **Risk Flags** — Cost leakage and agency-fit risks
- **Intervention Plan** — 3-step plan; Step 3 dynamically calibrated by paid demand status

---

## Production Readiness

The core logic is complete and stable. The system:

- Uses deterministic classification (no invented numbers, no probabilities in briefs)
- Stores embeddings for similarity and outcome analytics
- Supports outcome tracking and similar-lead conversion stats
- Handles missing data gracefully (omits sections when data is absent)
- Works for dental leads; non-dental paths remain intact

Next phase: backend API, UI, and production database deployment.

---

## Project Structure

```
lead-scoring-engine/
├── pipeline/
│   ├── db.py                    # Persistence (runs, leads, signals, embeddings, outcomes)
│   ├── revenue_brief_renderer.py
│   ├── objective_intelligence.py
│   ├── objective_decision_layer.py
│   ├── revenue_intelligence.py
│   ├── embedding_snapshot.py
│   ├── competitor_sampling.py
│   ├── dentist_profile.py
│   ├── fetch.py, enrich.py, signals.py
│   └── ...
├── scripts/
│   ├── run_pipeline.py
│   ├── run_enrichment.py
│   ├── run_upload.py
│   ├── export_leads.py
│   ├── render_brief.py
│   ├── update_outcome.py
│   ├── list_runs.py
│   └── test_small.py
├── data/                        # SQLite DB
└── output/                      # Leads, enriched JSON, briefs
```

---

## License

MIT
