import time
from typing import Callable

import requests

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
    "orderby": [{"mg_insertedOn": "DESC"}],
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
    ):
        self.url = f"{server.rstrip('/')}/{schema}/graphql"
        self.poll_interval = poll_interval
        self.session = session or requests.Session()
        self.session.headers["x-molgenis-token"] = token
        self._sleep = sleep

    def fetch_next_job(self) -> dict | None:
        """Return the newest CREATED job, or None if there is none."""
        response = self.session.post(
            self.url,
            json={"query": JOBS_QUERY, "variables": CREATED_JOBS_VARIABLES},
        )
        response.raise_for_status()
        jobs = response.json()["data"]["Jobs"] or []
        return jobs[0] if jobs else None

    def run(self, max_polls: int | None = None) -> None:
        """Poll forever, or `max_polls` times if given."""
        polls = 0
        while max_polls is None or polls < max_polls:
            job = self.fetch_next_job()
            if job:
                print(f"Found job {job['id']}")
            polls += 1
            self._sleep(self.poll_interval)
