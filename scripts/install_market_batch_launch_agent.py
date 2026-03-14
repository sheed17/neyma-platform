#!/usr/bin/env python3
"""Install a launchd agent for scheduled Neyma market batch runs."""

from __future__ import annotations

import argparse
import json
import os
import plistlib
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LABEL = "com.neyma.market-training-batch"


def _launch_agent_path(label: str) -> Path:
    return Path.home() / "Library" / "LaunchAgents" / f"{label}.plist"


def install_launch_agent(*, label: str, weekday: int, hour: int, minute: int) -> dict:
    wrapper_path = PROJECT_ROOT / "scripts" / "run_scheduled_market_batch.sh"
    if not wrapper_path.exists():
        raise SystemExit(f"Missing wrapper script: {wrapper_path}")

    launch_agent_path = _launch_agent_path(label)
    launch_agent_path.parent.mkdir(parents=True, exist_ok=True)

    log_dir = PROJECT_ROOT / "output" / "launchd"
    log_dir.mkdir(parents=True, exist_ok=True)

    plist_payload = {
        "Label": label,
        "ProgramArguments": [
            "/usr/bin/open",
            "-a",
            "Terminal.app",
            str(wrapper_path),
        ],
        "RunAtLoad": False,
        "StartCalendarInterval": {
            "Weekday": int(weekday),
            "Hour": int(hour),
            "Minute": int(minute),
        },
        "StandardOutPath": str(log_dir / "launchd.stdout.log"),
        "StandardErrorPath": str(log_dir / "launchd.stderr.log"),
    }

    with open(launch_agent_path, "wb") as fh:
        plistlib.dump(plist_payload, fh)

    domain = f"gui/{os.getuid()}"
    subprocess.run(["launchctl", "bootout", domain, str(launch_agent_path)], check=False)
    subprocess.run(["launchctl", "bootstrap", domain, str(launch_agent_path)], check=True)
    subprocess.run(["launchctl", "enable", f"{domain}/{label}"], check=False)

    return {
        "label": label,
        "launch_agent_path": str(launch_agent_path),
        "wrapper_path": str(wrapper_path),
        "schedule": {
            "weekday": int(weekday),
            "hour": int(hour),
            "minute": int(minute),
        },
        "launcher": "Terminal.app",
        "stdout_log": str(log_dir / "launchd.stdout.log"),
        "stderr_log": str(log_dir / "launchd.stderr.log"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Install the scheduled market-batch LaunchAgent.")
    parser.add_argument("--label", default=DEFAULT_LABEL)
    parser.add_argument("--weekday", type=int, default=1, help="launchd weekday (1=Monday, 7=Sunday)")
    parser.add_argument("--hour", type=int, default=2)
    parser.add_argument("--minute", type=int, default=15)
    args = parser.parse_args()

    result = install_launch_agent(
        label=args.label,
        weekday=args.weekday,
        hour=args.hour,
        minute=args.minute,
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
