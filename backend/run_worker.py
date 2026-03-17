"""
Standalone job worker entrypoint for production deployments.
"""

import logging
import os
import signal
import sys
import time
from pathlib import Path

try:
    from dotenv import load_dotenv

    _project_root = Path(__file__).resolve().parent.parent
    load_dotenv(_project_root / ".env")
except ImportError:
    pass

from backend.services.job_worker import start_worker, stop_worker
from pipeline.db import init_db


def _configure_logging() -> None:
    level_name = (os.getenv("LOG_LEVEL") or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def main() -> int:
    _configure_logging()
    init_db()
    start_worker()

    def _shutdown(*_args) -> None:
        stop_worker()
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        while True:
            time.sleep(1)
    finally:
        stop_worker()


if __name__ == "__main__":
    sys.exit(main())
