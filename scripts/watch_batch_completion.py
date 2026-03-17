#!/usr/bin/env python3
"""Watch a running batch and send a macOS notification on completion."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.macos_notify import notify


def _process_running(pattern: str) -> bool:
    return subprocess.run(["pgrep", "-f", pattern], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0


def _manifest_training_ready(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    training = payload.get("training")
    return isinstance(training, dict) and bool(training)


def _log_contains_success(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        return "Training finished for model version" in path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return False


def watch_batch(*, manifest_path: Path, log_path: Path, process_pattern: str, poll_seconds: int) -> int:
    seen_running = False
    while True:
        running = _process_running(process_pattern)
        seen_running = seen_running or running

        if _manifest_training_ready(manifest_path) or _log_contains_success(log_path):
            notify(
                title="Neyma batch complete",
                message="The current market batch finished successfully.",
            )
            return 0

        if seen_running and not running:
            notify(
                title="Neyma batch stopped",
                message=f"The current market batch ended. Check {log_path.name}.",
            )
            return 1

        time.sleep(max(5, poll_seconds))


def main() -> None:
    parser = argparse.ArgumentParser(description="Watch a batch and notify on completion.")
    parser.add_argument("--manifest-path", required=True)
    parser.add_argument("--log-path", required=True)
    parser.add_argument("--process-pattern", required=True)
    parser.add_argument("--poll-seconds", type=int, default=30)
    args = parser.parse_args()

    raise SystemExit(
        watch_batch(
            manifest_path=Path(args.manifest_path),
            log_path=Path(args.log_path),
            process_pattern=args.process_pattern,
            poll_seconds=args.poll_seconds,
        )
    )


if __name__ == "__main__":
    main()
