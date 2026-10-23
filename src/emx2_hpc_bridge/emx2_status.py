from __future__ import annotations

from enum import Enum


class Emx2JobStatus(str, Enum):
    """Status labels of the Jobs table on the EMX2 server (the source of truth).

    Not to be confused with `job_store.JobStatus`, which tracks the bridge's
    local progress.
    """

    CREATED = "CREATED"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    STOPPED = "STOPPED"
    FAILED = "FAILED"
