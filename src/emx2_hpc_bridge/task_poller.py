import logging
import time
from typing import Callable

import requests

from .job_store import JobStore

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
    ):
        self.url = f"{server.rstrip('/')}/{schema}/graphql"
        self.poll_interval = poll_interval
        self.session = session or requests.Session()
        self.session.headers["x-molgenis-token"] = token
        self._sleep = sleep
        self.job_store = job_store

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

    def _poll_once(self) -> None:
        job = self.fetch_next_job()
        if job:
            print(f"Found job {job['id']}")
            claim_job = self.claim_job(job["id"])
            print(f"Claimed job {claim_job}")
            if self.job_store:
                self.job_store.add_claimed(claim_job)
