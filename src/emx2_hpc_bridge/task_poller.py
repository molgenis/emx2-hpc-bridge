from __future__ import annotations

import json
import logging
import os
import time
from typing import Callable
from urllib.parse import urljoin

import requests

from .emx2_status import Emx2JobStatus
from .job_store import JobStatus, JobStore, StoredJob
from .job_workspace import OUTPUT_FILENAME, JobPaths, JobWorkspace
from .slurm_monitor import SlurmMonitor
from .slurm_submitter import SlurmSubmitter

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

JOB_STATUS_QUERY = """
query Job($filter: JobsFilter) {
  Jobs(filter: $filter) {
    id
    status {
      label
    }
  }
}
"""

# Slurm job state -> server status
SLURM_STATES = {
    "PENDING": Emx2JobStatus.PENDING,
    "CONFIGURING": Emx2JobStatus.PENDING,
    "REQUEUED": Emx2JobStatus.PENDING,
    "REQUEUE_HOLD": Emx2JobStatus.PENDING,
    "RESIZING": Emx2JobStatus.PENDING,
    "SUSPENDED": Emx2JobStatus.PENDING,
    "RUNNING": Emx2JobStatus.RUNNING,
    "COMPLETING": Emx2JobStatus.RUNNING,
    "STAGE_OUT": Emx2JobStatus.RUNNING,
    "COMPLETED": Emx2JobStatus.COMPLETED,
    "CANCELLED": Emx2JobStatus.STOPPED,
    "FAILED": Emx2JobStatus.FAILED,
    "TIMEOUT": Emx2JobStatus.FAILED,
    "OUT_OF_MEMORY": Emx2JobStatus.FAILED,
    "NODE_FAIL": Emx2JobStatus.FAILED,
    "BOOT_FAIL": Emx2JobStatus.FAILED,
    "DEADLINE": Emx2JobStatus.FAILED,
    "PREEMPTED": Emx2JobStatus.FAILED,
}

# Server statuses of a job that is no longer pending or running
FINAL_SERVER_STATUSES = {
    Emx2JobStatus.COMPLETED,
    Emx2JobStatus.STOPPED,
    Emx2JobStatus.FAILED,
}

# The input file is gone for good; retrying will not help
MISSING_INPUT_HTTP_CODES = (404, 410)

UPDATE_JOBS_MUTATION = """
mutation update($value:[JobsInput]){update(Jobs:$value){message}}
"""

# Multipart part holding the result; EMX2 swaps a variable whose value equals
# a part's name for that part
RESULT_PART = "resultFile"
XLSX_CONTENT_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


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
        submitter: SlurmSubmitter | None = None,
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
        self.submitter = submitter
        # Last status set on the server per submitted job, so an unchanged
        # Slurm state is not pushed every poll; after a restart it is pushed once
        self._reported: dict[str, Emx2JobStatus] = {}

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

    def _graphql(
        self,
        query: str,
        variables: dict,
        description: str,
        files: dict | None = None,
    ) -> dict:
        """Post a GraphQL request and return its "data".

        With `files`, the request is sent as multipart form data so the files
        can be referenced from `variables` by their part name.
        """
        if files:
            response = self.session.post(
                self.url,
                data={"query": query, "variables": json.dumps(variables)},
                files=files,
                timeout=300,
            )
        else:
            response = self.session.post(
                self.url,
                json={"query": query, "variables": variables},
                timeout=30,
            )
        response.raise_for_status()
        # EMX2 reports a rejected request as HTTP 200 with an "errors" field
        body = response.json()
        errors = body.get("errors")
        if errors:
            messages = "; ".join(str(e.get("message", e)) for e in errors)
            raise GraphQLError(f"{description} was rejected: {messages}")
        return body.get("data") or {}

    def set_status(self, job_id: str, status: Emx2JobStatus) -> None:
        """Set the status of a job on the server."""
        label = Emx2JobStatus(status).value
        variables = {"value": [{"id": job_id, "status": {"label": label}}]}
        self._graphql(
            UPDATE_JOBS_MUTATION, variables, f"Setting job {job_id} to {label}"
        )

    def post_result(self, job_id: str, paths: JobPaths) -> None:
        """Upload the job's result to its output column and mark it COMPLETED.

        Both in one mutation, so the server never has a COMPLETED job without
        its result.
        """
        row = {
            "id": job_id,
            "status": {"label": Emx2JobStatus.COMPLETED.value},
            "output": RESULT_PART,
        }
        with open(paths.output_file, "rb") as f:
            self._graphql(
                UPDATE_JOBS_MUTATION,
                {"value": [row]},
                f"Posting result of job {job_id}",
                files={RESULT_PART: (OUTPUT_FILENAME, f, XLSX_CONTENT_TYPE)},
            )

    def fetch_job_status(self, job_id: str) -> Emx2JobStatus | None:
        """Return the job's status on the server, or None if the job is gone."""
        variables = {"filter": {"id": {"equals": [job_id]}}}
        data = self._graphql(
            JOB_STATUS_QUERY, variables, f"Fetching status of job {job_id}"
        )
        jobs = data.get("Jobs") or []
        if not jobs:
            return None
        return Emx2JobStatus((jobs[0].get("status") or {}).get("label"))

    def claim_job(self, job_id: str) -> str:
        """Claim a job by setting its status to PENDING; returns the job id."""
        self.set_status(job_id, Emx2JobStatus.PENDING)
        return job_id

    def fail_job(self, job_id: str) -> None:
        """Mark a job FAILED, so it leaves the CREATED queue."""
        self.set_status(job_id, Emx2JobStatus.FAILED)

    def download_input(self, job: dict, paths: JobPaths) -> None:
        """Download the job's input file to `paths.input_file`.

        Written to a temporary file first, so an interrupted download never
        leaves a partial PDF behind that looks complete.

        Raises InputUnavailableError when the input is missing for good; any
        other error is assumed transient and the download is retried later.
        """
        file_info = job.get("input") or {}
        if not file_info.get("url"):
            raise InputUnavailableError(f"Job {job['id']} has no input file")
        # EMX2 returns file urls relative to the server
        url = urljoin(f"{self.server}/", file_info["url"])
        partial = paths.input_file.with_name(paths.input_file.name + ".part")
        with self.session.get(url, stream=True, timeout=60) as response:
            if response.status_code in MISSING_INPUT_HTTP_CODES:
                raise InputUnavailableError(
                    f"Input of job {job['id']} not found (HTTP {response.status_code})"
                )
            response.raise_for_status()
            with open(partial, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
        os.replace(partial, paths.input_file)

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

    def extraction_active(self) -> bool:
        """Whether an extraction job is submitted, pending or running.

        Checks Slurm too, so a job not submitted by the bridge also counts.
        """
        if self.job_store.list_by_status(JobStatus.SUBMITTED):
            return True
        if self.slurm_monitor is None:
            return False
        return bool(self.slurm_monitor.list_jobs())

    def submit_claimed_jobs(self) -> None:
        """Submit the oldest claimed job, unless an extraction job is active.

        Only one extraction job may run at a time. A job that fails to submit
        is logged and retried on the next poll; the next claimed job is tried
        instead.
        """
        if not (self.job_store and self.workspace and self.submitter):
            return
        try:
            if self.extraction_active():
                return
        except Exception:
            # Submitting blindly could start a second job
            logger.exception("Checking for active extraction failed; not submitting")
            return
        for stored in self.job_store.list_by_status(JobStatus.CLAIMED):
            try:
                if self.submit_job(stored.emx2_job_id):
                    return
            except Exception:
                logger.exception("Submitting job %s failed", stored.emx2_job_id)

    def submit_job(self, job_id: str) -> str | None:
        """Submit a claimed job to Slurm; returns the Slurm job id.

        Only submits when the job is claimed on the server and its workspace is
        complete; returns None otherwise.
        """
        # Jobs are stored locally before the server claim, so the claim may
        # have failed; such a job is still CREATED and gets claimed again later
        server_status = self.fetch_job_status(job_id)
        if server_status is not Emx2JobStatus.PENDING:
            if server_status is None or server_status in FINAL_SERVER_STATUSES:
                print(f"Not submitting job {job_id}: {server_status} on server")
                self.job_store.update_status(job_id, JobStatus.COMPLETED)
            return None
        paths = self.workspace.paths(job_id)
        if not paths.is_ready():
            logger.error("Failing job %s: workspace %s incomplete", job_id, paths.root)
            self.fail_job(job_id)
            self.job_store.update_status(job_id, JobStatus.COMPLETED)
            return None
        slurm_job_id = self.submitter.submit(paths)
        self.job_store.set_submitted(job_id, slurm_job_id)
        print(f"Submitted job {job_id} as Slurm job {slurm_job_id}")
        return slurm_job_id

    def update_job_statuses(self) -> None:
        """Sync the status of submitted jobs from Slurm to the store and server."""
        if not (self.job_store and self.slurm_monitor):
            return
        for stored in self.job_store.list_by_status(JobStatus.SUBMITTED):
            try:
                self.update_job_status(stored)
            except Exception:
                logger.exception("Updating status of job %s failed", stored.emx2_job_id)

    def update_job_status(self, stored: StoredJob) -> None:
        state = self.slurm_monitor.job_state(stored.slurm_job_id)
        server_status = SLURM_STATES.get(state)
        if server_status is None:
            logger.warning(
                "Job %s: unknown Slurm state %r of Slurm job %s",
                stored.emx2_job_id,
                state,
                stored.slurm_job_id,
            )
            return
        if server_status is Emx2JobStatus.COMPLETED and self.workspace:
            self.complete_job(stored)
        elif self._reported.get(stored.emx2_job_id) is not server_status:
            self.set_status(stored.emx2_job_id, server_status)
            self._reported[stored.emx2_job_id] = server_status
            print(
                f"Job {stored.emx2_job_id} (Slurm job {stored.slurm_job_id}) is {state}"
            )
        # Completed only once the server knows the outcome: if setting it fails,
        # the job stays SUBMITTED and the update is retried on the next poll
        if server_status in FINAL_SERVER_STATUSES:
            self.job_store.update_status(stored.emx2_job_id, JobStatus.COMPLETED)
            self._reported.pop(stored.emx2_job_id, None)

    def complete_job(self, stored: StoredJob) -> None:
        """Post the result of a job that Slurm completed.

        A job that completed without writing its result is set to FAILED.
        """
        paths = self.workspace.paths(stored.emx2_job_id)
        output = paths.output_file
        if output.is_file() and output.stat().st_size > 0:
            self.post_result(stored.emx2_job_id, paths)
            print(f"Posted result of job {stored.emx2_job_id} from {output}")
        else:
            logger.error(
                "Failing job %s: Slurm job %s completed without writing %s",
                stored.emx2_job_id,
                stored.slurm_job_id,
                output,
            )
            self.fail_job(stored.emx2_job_id)

    def _poll_once(self) -> None:
        self.check_cluster()
        try:
            self._claim_next_job()
        finally:
            # Also when claiming failed: already claimed jobs must still run.
            # Statuses first, so a job that just finished frees the cluster
            self.update_job_statuses()
            self.submit_claimed_jobs()

    def _claim_next_job(self) -> None:
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
                    self.download_input(job, paths)
                except InputUnavailableError:
                    # Without failing it, this job would stay the oldest CREATED
                    # job and block every job behind it
                    logger.exception("Failing job %s", job["id"])
                    self.fail_job(job["id"])
                    return
                print(f"Stored input of job {job['id']} in {paths.input_file}")
            # Record locally before claiming on the server: if the server claim
            # fails, the job stays CREATED and is retried on the next poll.
            if self.job_store:
                self.job_store.add_claimed(job["id"])
            claim_job = self.claim_job(job["id"])
            print(f"Claimed job {claim_job}")
