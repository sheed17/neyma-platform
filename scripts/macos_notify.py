#!/usr/bin/env python3
"""Send a native macOS notification."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys


def notify(*, title: str, message: str, subtitle: str | None = None, sound: str | None = None) -> None:
    if sys.platform != "darwin":
        return

    script = f"display notification {json.dumps(message)} with title {json.dumps(title)}"
    if subtitle:
        script += f" subtitle {json.dumps(subtitle)}"
    if sound:
        script += f" sound name {json.dumps(sound)}"
    subprocess.run(["osascript", "-e", script], check=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Send a native macOS notification.")
    parser.add_argument("--title", required=True)
    parser.add_argument("--message", required=True)
    parser.add_argument("--subtitle", default=None)
    parser.add_argument("--sound", default=None)
    args = parser.parse_args()
    notify(title=args.title, message=args.message, subtitle=args.subtitle, sound=args.sound)


if __name__ == "__main__":
    main()
