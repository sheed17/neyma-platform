# Core App Postgres Migration Plan

This documents the remaining Neyma data still backed by the local app database layer and the safest order to move it into shared Postgres.

## Current split

Already on Postgres/Supabase:

- `workspaces`
- `users`
- `workspace_members`
- `guest_sessions`
- `access_entitlements`
- `usage_counters`
- `usage_events`

Still on the local app database:

- scans
- prospects
- diagnostics / briefs
- jobs / worker state
- lists and saved leads
- outcomes / sharing
- embeddings and retrieval docs
- caches
- ML support tables

## Live SQLite table counts

Snapshot from local inventory:

- `runs`: 438
- `leads`: 487
- `lead_signals`: 487
- `decisions`: 478
- `lead_embeddings_v2`: 1863
- `lead_docs_v1`: 1470
- `lead_intel_v1`: 294
- `jobs`: 326
- `diagnostics`: 356
- `territory_scans`: 55
- `territory_prospects`: 918
- `territory_scan_diagnostics`: 19
- `diagnostic_predictions`: 410
- `diagnostic_outcomes`: 19
- `brief_share_tokens`: 1
- `prospect_lists`: 2
- `prospect_list_members`: 2
- `ask_places_cache`: 15
- `ask_lightweight_cache`: 424
- `territory_tier1_cache`: 1321
- `review_snapshots`: 403
- `ml_feature_snapshots`: 748
- `ml_predictions`: 748
- `ml_training_runs`: 12
- `ml_dataset_registry`: 20

## Embeddings

The system does store embeddings today.

### What is stored

Primary embedding store:

- `lead_embeddings_v2`

Fields stored there:

- `lead_id`
- `embedding_json`
- `text_snapshot`
- `embedding_version`
- `embedding_type`
- `created_at`

The main structural lead embedding currently uses:

- `embedding_version = v1_structural`
- `embedding_type = objective_state`

The embedded text is not raw page HTML. It is a compact structural snapshot built from:

- objective intelligence root constraint
- primary growth vector
- competitive profile
- review tier
- missing service pages
- schema presence / absence
- paid ads state

That snapshot is built in [embedding_snapshot.py](/Users/sammyfammy/Downloads/lead-scoring-engine/pipeline/embedding_snapshot.py).

The actual vector is generated through OpenAI embeddings in [embeddings.py](/Users/sammyfammy/Downloads/lead-scoring-engine/pipeline/embeddings.py), currently defaulting to `text-embedding-3-small`.

There is also typed retrieval-doc support:

- `lead_docs_v1`

Document types include:

- `signal_profile`
- `service_coverage`
- `market_context`
- `conversion_path`
- `llm_brief_summary`

When those docs are embedded, the vector still lands in `lead_embeddings_v2`, keyed by:

- `embedding_version = v1_doc`
- `embedding_type = doc_<doc_type>`

### What is not stored

- no pgvector table yet
- no external vector DB
- no raw website crawl dumps as embeddings

Embeddings are currently stored as JSON arrays in SQLite.

## Recommended migration order

### Phase 1: Core runtime data

Move first because the product depends on these for daily operation:

- `jobs`
- `territory_scans`
- `territory_prospects`
- `territory_scan_diagnostics`
- `diagnostics`
- `diagnostic_predictions`
- `diagnostic_outcomes`
- `brief_share_tokens`
- `prospect_lists`
- `prospect_list_members`

Why first:

- these power the live workspace
- both API and worker need them
- they should not live on local disk in production

### Phase 2: Lead intelligence foundation

- `runs`
- `leads`
- `lead_signals`
- `decisions`
- `lead_intel_v1`
- `review_snapshots`

Why second:

- these are the data spine for enrichment and brief building
- they are large enough to matter, but less urgent than active runtime state

### Phase 3: Embeddings and retrieval docs

- `lead_embeddings_v2`
- `lead_docs_v1`

Why third:

- important for similarity / RAG quality
- not required to get the core runtime safely onto shared Postgres
- may later be redesigned around pgvector instead of JSON blobs

### Phase 4: Caches and ML support

- `ask_places_cache`
- `ask_lightweight_cache`
- `territory_tier1_cache`
- `ml_feature_snapshots`
- `ml_predictions`
- `ml_training_runs`
- `ml_dataset_registry`
- `qa_signal_checks`

Why last:

- operationally useful, but not the first production blocker
- some of these may be better retained / pruned differently in Postgres

## Recommended technical approach

1. Add a second runtime path in `pipeline/db.py` for core app Postgres, similar to the access-store split.
2. Start with Phase 1 tables only.
3. Migrate data table by table with shape-normalizing scripts, not naive `SELECT *` copies.
4. Switch API + worker reads/writes for Phase 1 tables to Postgres.
5. Only then move into Phase 2 and embeddings.

## Immediate next implementation task

Implement Postgres runtime support for:

- `jobs`
- `territory_scans`
- `territory_prospects`
- `territory_scan_diagnostics`
- `diagnostics`
- `prospect_lists`
- `prospect_list_members`

That is the minimum slice that makes the live app and worker substantially more production-safe.
