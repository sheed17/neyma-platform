#!/usr/bin/env python3
"""Run a scan-to-train batch for a set of territory markets."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Sequence
from uuid import uuid4

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from backend.ml.training_utils import load_market_specs
from backend.services.job_worker import start_worker, stop_worker
from pipeline.db import (
    create_job,
    create_territory_scan,
    get_job,
    get_territory_scan,
    init_db,
    list_territory_prospects,
)
from scripts.build_lead_quality_dataset import build_dataset_snapshot
from scripts.train_lead_quality import train_model


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _timestamp() -> str:
    return _now().strftime("%Y%m%dT%H%M%SZ")


def _log(message: str) -> None:
    print(f"[{_now().strftime('%H:%M:%S')}] {message}", flush=True)


def _write_json(path: str, payload: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)


def _evenly_spaced_indices(total: int, count: int) -> list[int]:
    if total <= 0 or count <= 0:
        return []
    if count >= total:
        return list(range(total))
    if count == 1:
        return [0]

    chosen: list[int] = []
    used: set[int] = set()
    for slot in range(count):
        candidate = round(slot * (total - 1) / (count - 1))
        idx = candidate
        while idx in used and idx < total - 1:
            idx += 1
        if idx in used:
            idx = candidate
            while idx in used and idx > 0:
                idx -= 1
        if idx in used:
            continue
        used.add(idx)
        chosen.append(idx)

    if len(chosen) < count:
        for idx in range(total):
            if idx in used:
                continue
            used.add(idx)
            chosen.append(idx)
            if len(chosen) >= count:
                break
    return sorted(chosen)


def select_prospect_ids(rows: Sequence[Dict[str, Any]], desired_count: int, strategy: str) -> list[int]:
    ordered = [row for row in rows if row.get("id")]
    if desired_count <= 0 or not ordered:
        return []
    if desired_count >= len(ordered):
        return [int(row["id"]) for row in ordered]
    if strategy == "top":
        return [int(row["id"]) for row in ordered[:desired_count]]
    indices = _evenly_spaced_indices(len(ordered), desired_count)
    return [int(ordered[idx]["id"]) for idx in indices]


def _enqueue_scan_job(*, user_id: int, market: Dict[str, str], vertical: str, scan_limit: int) -> Dict[str, Any]:
    scan_id = str(uuid4())
    job_id = create_job(
        user_id=user_id,
        job_type="territory_scan",
        input_data={
            "scan_id": scan_id,
            "city": market["city"],
            "state": market["state"],
            "vertical": vertical,
            "limit": scan_limit,
            "filters": {},
        },
    )
    create_territory_scan(
        scan_id=scan_id,
        user_id=user_id,
        job_id=job_id,
        city=market["city"],
        state=market["state"],
        vertical=vertical,
        limit_count=scan_limit,
        filters={},
        scan_type="territory",
    )
    return {
        "market": market["raw"],
        "market_key": market["market_key"],
        "scan_id": scan_id,
        "scan_job_id": job_id,
        "scan_status": "pending",
    }


def _enqueue_deep_brief_job(
    *,
    user_id: int,
    scan_id: str,
    prospect_ids: Sequence[int],
    concurrency: int,
) -> Dict[str, Any]:
    job_id = create_job(
        user_id=user_id,
        job_type="territory_deep_scan",
        input_data={
            "scan_id": scan_id,
            "max_prospects": len(prospect_ids),
            "concurrency": concurrency,
            "deep_audit": True,
            "prospect_ids": list(prospect_ids),
        },
    )
    return {
        "job_id": job_id,
        "scan_id": scan_id,
        "status": "pending",
    }


def _update_scan_record(entry: Dict[str, Any], user_id: int) -> None:
    job = get_job(str(entry["scan_job_id"])) or {}
    scan = get_territory_scan(str(entry["scan_id"]), user_id) or {}
    entry["scan_job_status"] = job.get("status")
    entry["scan_status"] = scan.get("status") or job.get("status") or "unknown"
    entry["scan_summary"] = scan.get("summary") or {}
    entry["scan_error"] = scan.get("error") or job.get("error")
    entry["scan_completed_at"] = scan.get("completed_at") or job.get("completed_at")


def _update_deep_job_record(entry: Dict[str, Any]) -> None:
    job = get_job(str(entry["job_id"])) or {}
    entry["status"] = job.get("status") or "unknown"
    entry["result"] = job.get("result") or {}
    entry["error"] = job.get("error")
    entry["completed_at"] = job.get("completed_at")


def _wait_for_scan_jobs(
    entries: Sequence[Dict[str, Any]],
    *,
    user_id: int,
    timeout_seconds: int,
    poll_interval_seconds: int,
) -> None:
    deadline = time.time() + timeout_seconds
    pending = {str(entry["scan_job_id"]): entry for entry in entries}
    while pending:
        if time.time() > deadline:
            raise TimeoutError("Timed out waiting for territory scan jobs to finish.")
        completed_ids: list[str] = []
        for job_id, entry in pending.items():
            _update_scan_record(entry, user_id)
            if entry.get("scan_status") in {"completed", "failed"} or entry.get("scan_job_status") in {"completed", "failed"}:
                completed_ids.append(job_id)
        if completed_ids:
            for job_id in completed_ids:
                entry = pending.pop(job_id)
                _log(f"Scan {entry['market']} finished with status {entry['scan_status']}.")
        if pending:
            time.sleep(poll_interval_seconds)


def _wait_for_deep_jobs(
    entries: Sequence[Dict[str, Any]],
    *,
    timeout_seconds: int,
    poll_interval_seconds: int,
) -> None:
    deadline = time.time() + timeout_seconds
    pending = {str(entry["job_id"]): entry for entry in entries if entry.get("job_id")}
    while pending:
        if time.time() > deadline:
            raise TimeoutError("Timed out waiting for deep-brief jobs to finish.")
        completed_ids: list[str] = []
        for job_id, entry in pending.items():
            _update_deep_job_record(entry)
            if entry.get("status") in {"completed", "failed"}:
                completed_ids.append(job_id)
        if completed_ids:
            for job_id in completed_ids:
                entry = pending.pop(job_id)
                _log(f"Deep brief job for {entry['market']} finished with status {entry['status']}.")
        if pending:
            time.sleep(poll_interval_seconds)


def _coerce_scan_entries(
    scan_entries: List[Dict[str, Any]],
    *,
    user_id: int,
    diagnostics_per_market: int,
    selection_strategy: str,
    deep_brief_concurrency: int,
) -> list[Dict[str, Any]]:
    deep_jobs: list[Dict[str, Any]] = []
    for entry in scan_entries:
        rows = list_territory_prospects(str(entry["scan_id"]), user_id)
        entry["prospect_count"] = len(rows)
        selected_ids = select_prospect_ids(rows, diagnostics_per_market, selection_strategy)
        selected_set = set(selected_ids)
        ready_ids = [int(row["id"]) for row in rows if int(row.get("id") or 0) in selected_set and row.get("full_brief_ready")]
        build_ids = [prospect_id for prospect_id in selected_ids if prospect_id not in set(ready_ids)]
        entry["selected_prospect_ids"] = selected_ids
        entry["selected_ready_ids"] = ready_ids
        entry["selected_build_ids"] = build_ids
        if build_ids:
            deep_job = _enqueue_deep_brief_job(
                user_id=user_id,
                scan_id=str(entry["scan_id"]),
                prospect_ids=build_ids,
                concurrency=deep_brief_concurrency,
            )
            deep_job["market"] = entry["market"]
            entry["deep_brief_job_id"] = deep_job["job_id"]
            deep_jobs.append(deep_job)
        else:
            entry["deep_brief_status"] = "not_needed"
    return deep_jobs


def run_market_training_batch(
    *,
    market_values: Sequence[str] | None,
    market_file: str | None,
    vertical: str,
    scan_limit: int,
    diagnostics_per_market: int,
    selection_strategy: str,
    user_id: int,
    deep_brief_concurrency: int,
    scan_timeout_minutes: int,
    brief_timeout_minutes: int,
    poll_interval_seconds: int,
    dataset_output_dir: str,
    batch_output_dir: str,
    group_by: str,
    holdout_market_values: Sequence[str] | None,
    holdout_market_file: str | None,
    model_version: str | None,
    promote_latest: bool,
    start_local_worker: bool,
    allow_partial: bool,
    dry_run: bool,
) -> Dict[str, Any]:
    market_specs = load_market_specs(market_values, market_file)
    if not market_specs:
        raise SystemExit("At least one market is required.")
    if scan_limit < diagnostics_per_market:
        raise SystemExit("--scan-limit must be greater than or equal to --diagnostics-per-market.")

    batch_id = _timestamp()
    batch_dir = os.path.join(batch_output_dir, batch_id)
    manifest_path = os.path.join(batch_dir, "manifest.json")
    output: Dict[str, Any] = {
        "batch_id": batch_id,
        "created_at": _now().isoformat(),
        "vertical": vertical,
        "user_id": user_id,
        "scan_limit": scan_limit,
        "diagnostics_per_market": diagnostics_per_market,
        "selection_strategy": selection_strategy,
        "group_by": group_by,
        "holdout_markets": [item["raw"] for item in load_market_specs(holdout_market_values, holdout_market_file)],
        "promote_latest": bool(promote_latest),
        "markets": [item["raw"] for item in market_specs],
        "scans": [],
        "deep_brief_jobs": [],
        "dataset": None,
        "training": {},
    }
    _write_json(manifest_path, output)

    if dry_run:
        output["dry_run"] = True
        _write_json(manifest_path, output)
        return output

    init_db()
    worker_started = False
    try:
        if start_local_worker:
            start_worker()
            worker_started = True

        for market in market_specs:
            entry = _enqueue_scan_job(
                user_id=user_id,
                market=market,
                vertical=vertical,
                scan_limit=scan_limit,
            )
            output["scans"].append(entry)
        _write_json(manifest_path, output)
        _log(f"Queued {len(output['scans'])} territory scans.")

        _wait_for_scan_jobs(
            output["scans"],
            user_id=user_id,
            timeout_seconds=max(1, scan_timeout_minutes) * 60,
            poll_interval_seconds=max(1, poll_interval_seconds),
        )
        _write_json(manifest_path, output)

        failed_scans = [entry for entry in output["scans"] if entry.get("scan_status") != "completed"]
        if failed_scans and not allow_partial:
            raise SystemExit(f"{len(failed_scans)} scan jobs failed. See {manifest_path}.")

        completed_scans = [entry for entry in output["scans"] if entry.get("scan_status") == "completed"]
        deep_jobs = _coerce_scan_entries(
            completed_scans,
            user_id=user_id,
            diagnostics_per_market=diagnostics_per_market,
            selection_strategy=selection_strategy,
            deep_brief_concurrency=deep_brief_concurrency,
        )
        output["deep_brief_jobs"] = deep_jobs
        _write_json(manifest_path, output)

        if deep_jobs:
            _log(f"Queued {len(deep_jobs)} deep-brief jobs.")
            _wait_for_deep_jobs(
                deep_jobs,
                timeout_seconds=max(1, brief_timeout_minutes) * 60,
                poll_interval_seconds=max(1, poll_interval_seconds),
            )
            _write_json(manifest_path, output)

        failed_deep_jobs = [entry for entry in deep_jobs if entry.get("status") != "completed"]
        if failed_deep_jobs and not allow_partial:
            raise SystemExit(f"{len(failed_deep_jobs)} deep-brief jobs failed. See {manifest_path}.")

        dataset_manifest = build_dataset_snapshot(
            output_dir=dataset_output_dir,
            market_values=[item["raw"] for item in market_specs],
        )
        output["dataset"] = dataset_manifest
        _write_json(manifest_path, output)
        _log(f"Built dataset snapshot {dataset_manifest['built_at']}.")

        version = model_version or f"v{_now().strftime('%Y.%m.%d.%H%M')}"
        territory_result = train_model(
            dataset_csv=str(dataset_manifest["territory"]["path"]),
            task_name="territory_lead_quality",
            dataset_version=str(dataset_manifest["territory"]["dataset_version"]),
            model_version=version,
            promote_latest=promote_latest,
            group_by=group_by,
            holdout_market_values=holdout_market_values,
            holdout_market_file=holdout_market_file,
        )
        diagnostic_result = train_model(
            dataset_csv=str(dataset_manifest["diagnostic"]["path"]),
            task_name="diagnostic_lead_quality",
            dataset_version=str(dataset_manifest["diagnostic"]["dataset_version"]),
            model_version=version,
            promote_latest=promote_latest,
            group_by=group_by,
            holdout_market_values=holdout_market_values,
            holdout_market_file=holdout_market_file,
        )
        output["training"] = {
            "model_version": version,
            "territory": territory_result,
            "diagnostic": diagnostic_result,
        }
        _write_json(manifest_path, output)
        _log(f"Training finished for model version {version}.")
        return output
    finally:
        if worker_started:
            stop_worker()


def main() -> None:
    parser = argparse.ArgumentParser(description="Automate a territory scan-to-train batch.")
    parser.add_argument("--market", action="append", default=[], help="Repeatable market in 'City, ST' format")
    parser.add_argument("--market-file", default=None, help="Optional newline-delimited market file")
    parser.add_argument("--vertical", default="dentist")
    parser.add_argument("--scan-limit", type=int, default=20, help="How many ranked prospects to save per market")
    parser.add_argument("--diagnostics-per-market", type=int, default=10, help="How many prospects per market to include in the training batch")
    parser.add_argument("--selection-strategy", choices=["stratified_rank", "top"], default="stratified_rank")
    parser.add_argument("--deep-brief-concurrency", type=int, default=3)
    parser.add_argument("--user-id", type=int, default=1)
    parser.add_argument("--scan-timeout-minutes", type=int, default=180)
    parser.add_argument("--brief-timeout-minutes", type=int, default=480)
    parser.add_argument("--poll-interval-seconds", type=int, default=10)
    parser.add_argument("--dataset-output-dir", default=os.path.join("output", "ml_datasets"))
    parser.add_argument("--batch-output-dir", default=os.path.join("output", "training_batches"))
    parser.add_argument("--group-by", choices=["place_id", "market"], default="market")
    parser.add_argument("--holdout-market", action="append", default=[], help="Repeatable holdout market in 'City, ST' format")
    parser.add_argument("--holdout-market-file", default=None, help="Optional newline-delimited holdout market file")
    parser.add_argument("--model-version", default=None, help="Optional explicit model version shared by both tasks")
    parser.add_argument("--promote-latest", action="store_true")
    parser.add_argument("--no-start-local-worker", action="store_true", help="Assume another process is already running the job worker")
    parser.add_argument("--allow-partial", action="store_true", help="Continue to dataset/train even if some markets fail")
    parser.add_argument("--dry-run", action="store_true", help="Validate inputs and write a batch manifest without creating jobs")
    args = parser.parse_args()

    result = run_market_training_batch(
        market_values=args.market,
        market_file=args.market_file,
        vertical=args.vertical,
        scan_limit=args.scan_limit,
        diagnostics_per_market=args.diagnostics_per_market,
        selection_strategy=args.selection_strategy,
        user_id=args.user_id,
        deep_brief_concurrency=args.deep_brief_concurrency,
        scan_timeout_minutes=args.scan_timeout_minutes,
        brief_timeout_minutes=args.brief_timeout_minutes,
        poll_interval_seconds=args.poll_interval_seconds,
        dataset_output_dir=args.dataset_output_dir,
        batch_output_dir=args.batch_output_dir,
        group_by=args.group_by,
        holdout_market_values=args.holdout_market,
        holdout_market_file=args.holdout_market_file,
        model_version=args.model_version,
        promote_latest=args.promote_latest,
        start_local_worker=not args.no_start_local_worker,
        allow_partial=args.allow_partial,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
