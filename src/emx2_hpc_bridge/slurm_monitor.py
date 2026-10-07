from __future__ import annotations

import getpass
import subprocess
from dataclasses import dataclass
from typing import Callable

# Job name set by `#SBATCH --job-name` in paper-extracting's run_cluster_cohort.sbatch
DEFAULT_JOB_NAME = "cohort_extract"

# squeue output: job id | job name | state | elapsed time
SQUEUE_FORMAT = "%i|%j|%T|%M"


@dataclass
class SlurmJob:
    job_id: str
    name: str
    state: str
    elapsed: str


class SlurmMonitor:
    """Checks the Slurm queue for paper-extraction jobs."""

    def __init__(
        self,
        job_name: str = DEFAULT_JOB_NAME,
        user: str | None = None,
        run: Callable[..., subprocess.CompletedProcess] = subprocess.run,
    ):
        self.job_name = job_name
        self.user = user or getpass.getuser()
        self._run = run

    def list_jobs(self) -> list[SlurmJob]:
        """Return all queued extraction jobs of the user (any state)."""
        result = self._run(
            [
                "squeue",
                "--noheader",
                f"--user={self.user}",
                f"--name={self.job_name}",
                f"--format={SQUEUE_FORMAT}",
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        )
        return [
            SlurmJob(*line.split("|", 3))
            for line in result.stdout.splitlines()
            if line.strip()
        ]

    def running_jobs(self) -> list[SlurmJob]:
        """Return the extraction jobs that are currently running."""
        return [job for job in self.list_jobs() if job.state == "RUNNING"]

    def is_job_running(self) -> bool:
        return bool(self.running_jobs())
