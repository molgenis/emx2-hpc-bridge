from __future__ import annotations

import logging
import time
from typing import Callable

import requests

from .job_store import JobStatus, JobStore
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
    "filter": {"status": {"equals": [{"label": "CREATED"}]}},
    "orderby": [{"mg_insertedOn": "ASC"}],
}


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
        sbatch_script: str | None = None,
    ):
        self.url = f"{server.rstrip('/')}/{schema}/graphql"
        self.poll_interval = poll_interval
        self.session = session or requests.Session()
        self.session.headers["x-molgenis-token"] = token
        self._sleep = sleep
        self.job_store = job_store
        self.slurm_monitor = slurm_monitor
        self.sbatch_script = sbatch_script

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

    def claim_job(self, job_id: str) -> str:
        """Claim a job by setting its status to PENDING; returns the job id."""
        claim_query = """
        mutation update($value:[JobsInput]){update(Jobs:$value){message}}
        """
        variables = {"value": [{"id": job_id, "status": {"label": "PENDING"}}]}
        response = self.session.post(
            self.url,
            json={"query": claim_query, "variables": variables},
            timeout=30,
        )
        response.raise_for_status()
        return job_id

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
        """Return whether an extraction job is queued or running on the cluster.

        Queued (e.g. PENDING) jobs count as busy, so a job submitted on a previous
        poll is not submitted twice before Slurm starts it.

        Returns None when no monitor is configured or the check failed; a failing
        check is logged and no jobs are claimed or submitted in that poll.
        """
        if self.slurm_monitor is None:
            return None
        try:
            jobs = self.slurm_monitor.list_jobs()
        except Exception:
            logger.exception("Checking Slurm queue failed")
            return None
        if jobs:
            for job in jobs:
                print(
                    f"Extraction job {job.job_id} {job.state} on cluster "
                    f"({job.elapsed})"
                )
        else:
            print("No extraction job running on cluster")
        return bool(jobs)

    def submit_job(self, job_id: str) -> str:
        """Start the extraction for `job_id` on the cluster.

        Returns the Slurm job id.
        """
        # slurm_job_id = self.slurm_monitor.submit_job(self.sbatch_script, job_id)
        if self.job_store:
            self.job_store.update_status(job_id, JobStatus.SUBMITTED)
        return slurm_job_id

    def _poll_once(self) -> None:
        # Only claim jobs that can be submitted right away: the cluster must be
        # known to be free and there must be a script to submit.
        if not self.sbatch_script or self.check_cluster() is not False:
            return
        if self._submit_claimed_job():
            return
        job = self.fetch_next_job()
        if not job:
            return
        print(f"Found job {job['id']}")
        # Record locally before claiming on the server: if the server claim
        # fails, the job is submitted from the store on the next poll.
        if self.job_store:
            self.job_store.add_claimed(job["id"])
        claim_job = self.claim_job(job["id"])
        print(f"Claimed job {claim_job}")
        slurm_job_id = self.submit_job(claim_job)
        print(f"Submitted job {claim_job} to cluster as Slurm job {slurm_job_id}")

    def _submit_claimed_job(self) -> bool:
        """Submit the oldest job that was claimed but never submitted.

        Returns whether a job was submitted; the cluster runs one job at a time,
        so new jobs are only claimed once no claimed jobs are left.
        """
        if not self.job_store:
            return False
        claimed = self.job_store.list_by_status(JobStatus.CLAIMED)
        if not claimed:
            return False
        job_id = claimed[0].job_id
        print(f"Found claimed job {job_id} that was not submitted")
        # The earlier server claim may have failed; claiming again is idempotent
        self.claim_job(job_id)
        slurm_job_id = self.submit_job(job_id)
        print(f"Submitted job {job_id} to cluster as Slurm job {slurm_job_id}")
        return True
