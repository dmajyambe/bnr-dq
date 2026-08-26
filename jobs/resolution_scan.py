#resolution job
from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from issues.resolution import run_resolution_scan

SCRIPT_DIR = Path(__file__).resolve().parent.parent

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("jobs.resolution_scan")


if __name__ == "__main__":
    load_dotenv(SCRIPT_DIR / ".env")

    from storage.postgres.connection import build_connection_string, get_engine

    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", default=os.environ.get("DQ_SCHEMA", "dqp"))
    args = parser.parse_args()

    engine = get_engine(build_connection_string())
    run_id = datetime.now().strftime("RES-%Y%m%d-%H%M%S")

    run_resolution_scan(engine, args.schema, run_id)
