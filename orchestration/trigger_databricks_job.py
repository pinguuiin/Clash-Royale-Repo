"""Entrypoint: trigger the Databricks notebook job and wait for it to finish.

Triggers the bronze->silver->quality->gold notebook chain via the Databricks Jobs
API (``run_now``) once the raw JSON is on the volume, then blocks until the run
completes. A non-successful run raises, so the GitHub Actions step — and the
whole daily workflow — goes red on a pipeline failure.

Auth is the standard Databricks SDK chain: ``DATABRICKS_HOST`` and
``DATABRICKS_TOKEN`` in the environment. The job id comes from ``--job-id`` or
``$DATABRICKS_JOB_ID``.

Usage:
    python -m orchestration.trigger_databricks_job     # uses $DATABRICKS_JOB_ID
    python -m orchestration.trigger_databricks_job --job-id 123456789
"""

from __future__ import annotations

import argparse
import logging
import os

from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

# Load DATABRICKS_HOST/TOKEN/JOB_ID from .env so that the script can be run locally
# without extra setup
load_dotenv()

logger = logging.getLogger(__name__)

def trigger_job(job_id: int, client: WorkspaceClient | None = None) -> None:
    """Run the job now and block until it finishes; raise if it does not succeed."""
    client = client or WorkspaceClient()
    logger.info("triggering Databricks job %d", job_id)
    # .result() polls to completion and raises on a failed/cancelled terminal state.
    run = client.jobs.run_now(job_id=job_id).result()
    logger.info("job %d finished: run_id=%s, state=%s", job_id, run.run_id, run.state)

def main(argv: list[str] | None = None) -> int:
    """Parse arguments, set logging, trigger the job and wait for completion."""
    parser = argparse.ArgumentParser(description="Trigger a Databricks job and wait.")
    parser.add_argument(
        "--job-id", type=int, default=None,
        help="Databricks job id (default: $DATABRICKS_JOB_ID).",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    raw_job_id = args.job_id or os.environ.get("DATABRICKS_JOB_ID")
    if not raw_job_id:
        parser.error("no job id — pass --job-id or set $DATABRICKS_JOB_ID")

    trigger_job(int(raw_job_id))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
