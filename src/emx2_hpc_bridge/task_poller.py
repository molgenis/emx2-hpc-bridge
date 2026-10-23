from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Callable
from urllib.parse import urljoin

import requests

from .emx2_status import Emx2JobStatus
from .job_store import JobStore
from .job_workspace import JobPaths, JobWorkspace
from .slurm_monitor import SlurmMonitor

logger = logging.getLogger(__name__)

JOBS_QUERY = """
query Jobs($filter: JobsFilter, $orderby: [Jobsorderby]) {
  Jobs(filter: $filter, limit: 1, offset: 0, orderby: $orderby) {
    id
    input {
      id
      size
      filename
      extension
      url
    }
    status {
      label
    }
    mg_insertedBy
    mg_insertedOn
  }
  Jobs_agg(filter: $filter) {
    count
  }
}
"""

CREATED_JOBS_VARIABLES = {
    "filter": {"status": {"equals": [{"label": Emx2JobStatus.CREATED.value}]}},
    "orderby": [{"mg_insertedOn": "ASC"}],
}

# The input file is gone for good; retrying will not help
MISSING_INPUT_HTTP_CODES = (404, 410)


class InputUnavailableError(Exception):
    """The job's input can never be downloaded, so the job cannot run."""


class GraphQLError(Exception):
    """The server answered a GraphQL request with errors."""


class TaskPoller:
    """Polls an EMX2 schema for jobs with status CREATED."""

    def __init__(
        self,
        server: str,
        schema: str,
        token: str,
        poll_interval: int = 30,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
        job_store: JobStore | None = None,
        slurm_monitor: SlurmMonitor | None = None,
        workspace: JobWorkspace | None = None,
    ):
        self.server = server.rstrip("/")
        self.url = f"{self.server}/{schema}/graphql"
        self.poll_interval = poll_interval
        self.session = session or requests.Session()
        self.session.headers["x-molgenis-token"] = token
        self._sleep = sleep
        self.job_store = job_store
        self.slurm_monitor = slurm_monitor
        self.workspace = workspace

    def fetch_next_job(self) -> dict | None:
        """Return the oldest CREATED job, or None if there is none."""
        response = self.session.post(
            self.url,
            json={"query": JOBS_QUERY, "variables": CREATED_JOBS_VARIABLES},
            timeout=30,
        )
        response.raise_for_status()
        if not response.content:
            raise ValueError("Empty response from server when fetching jobs")
        # EMX2 omits the "Jobs" key entirely when no rows match the filter
        data = response.json().get("data") or {}
        jobs = data.get("Jobs") or []
        return jobs[0] if jobs else None

    def set_status(self, job_id: str, status: Emx2JobStatus) -> None:
        """Set the status of a job on the server."""
        query = """
        mutation update($value:[JobsInput]){update(Jobs:$value){message}}
        """
        label = Emx2JobStatus(status).value
        variables = {"value": [{"id": job_id, "status": {"label": label}}]}
        response = self.session.post(
            self.url,
            json={"query": query, "variables": variables},
            timeout=30,
        )
        response.raise_for_status()
        # EMX2 reports a rejected mutation as HTTP 200 with an "errors" field
        errors = response.json().get("errors")
        if errors:
            messages = "; ".join(str(e.get("message", e)) for e in errors)
            raise GraphQLError(
                f"Setting job {job_id} to {label} was rejected: {messages}"
            )

    def claim_job(self, job_id: str) -> str:
        """Claim a job by setting its status to PENDING; returns the job id."""
        self.set_status(job_id, Emx2JobStatus.PENDING)
        return job_id

    def fail_job(self, job_id: str) -> None:
        """Mark a job FAILED, so it leaves the CREATED queue."""
        self.set_status(job_id, Emx2JobStatus.FAILED)

    def download_input(self, job: dict, paths: JobPaths) -> Path:
        """Download the job's input file into `paths.input_dir`, under the
        filename it was uploaded with (sanitized); returns the stored file's path.

        Written to a temporary file first, so an interrupted download never
        leaves a partial file behind that looks complete.

        Raises InputUnavailableError when the input is missing for good; any
        other error is assumed transient and the download is retried later.
        """
        file_info = job.get("input") or {}
        if not file_info.get("url"):
            raise InputUnavailableError(f"Job {job['id']} has no input file")
        input_file = paths.input_file(
            file_info.get("filename"), file_info.get("extension")
        )
        # EMX2 returns file urls relative to the server
        url = urljoin(f"{self.server}/", file_info["url"])
        partial = input_file.with_name(input_file.name + ".part")
        try:
            with self.session.get(url, stream=True, timeout=60) as response:
                if response.status_code in MISSING_INPUT_HTTP_CODES:
                    raise InputUnavailableError(
                        f"Input of job {job['id']} not found "
                        f"(HTTP {response.status_code})"
                    )
                response.raise_for_status()
                with open(partial, "wb") as f:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)
            os.replace(partial, input_file)
        finally:
            # Never leave a stray partial file next to the inputs on a retry
            partial.unlink(missing_ok=True)
        return input_file

    def run(self, max_polls: int | None = None) -> None:
        """Poll forever, or `max_polls` times if given.

        Errors during a poll are logged and never stop the loop.
        """
        polls = 0
        while max_polls is None or polls < max_polls:
            print(f"Polling for jobs (poll {polls + 1})...")
            try:
                self._poll_once()
            except Exception:
                logger.exception("Poll %d failed", polls + 1)
            polls += 1
            self._sleep(self.poll_interval)

    def check_cluster(self) -> bool | None:
        """Return whether an extraction job is running on the cluster.

        Returns None when no monitor is configured or the check failed; a failing
        check is logged and does not prevent claiming new jobs.
        """
        if self.slurm_monitor is None:
            return None
        try:
            running = self.slurm_monitor.running_jobs()
        except Exception:
            logger.exception("Checking Slurm queue failed")
            return None
        if running:
            for job in running:
                print(f"Extraction job {job.job_id} running on cluster ({job.elapsed})")
        else:
            print("No extraction job running on cluster")
        return bool(running)

    def _poll_once(self) -> None:
        self.check_cluster()
        job = self.fetch_next_job()
        if job:
            print(f"Found job {job['id']}")
            if self.workspace:
                try:
                    paths = self.workspace.prepare(job["id"])
                except ValueError:
                    logger.exception("Failing job %s: invalid workspace id", job["id"])
                    self.fail_job(job["id"])
                    return
                try:
                    input_file = self.download_input(job, paths)
                except InputUnavailableError:
                    # Without failing it, this job would stay the oldest CREATED
                    # job and block every job behind it
                    logger.exception("Failing job %s", job["id"])
                    self.fail_job(job["id"])
                    return
                print(f"Stored input of job {job['id']} in {input_file}")
            # Record locally before claiming on the server: if the server claim
            # fails, the job stays CREATED and is retried on the next poll.
            if self.job_store:
                self.job_store.add_claimed(job["id"])
            claim_job = self.claim_job(job["id"])
            print(f"Claimed job {claim_job}")
