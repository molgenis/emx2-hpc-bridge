from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum


class JobStatus(str, Enum):
    """The bridge's local progress of a job.

    CLAIMED: claimed on the server, not yet submitted to Slurm.
    SUBMITTED: submitted to Slurm; pending or running there.
    COMPLETED: no longer pending or running, whether it succeeded or failed.
    The outcome is kept on the server (see `emx2_status.Emx2JobStatus`).
    """

    CLAIMED = "claimed"
    SUBMITTED = "submitted"
    COMPLETED = "completed"


@dataclass
class StoredJob:
    emx2_job_id: str
    claimed_at: datetime
    status: JobStatus
    slurm_job_id: str | None = None


_COLUMNS = "emx2_job_id, claimed_at, status, slurm_job_id"


class JobStore:
    """Persists jobs claimed by the poller in a local sqlite3 database."""

    def __init__(self, path: str = "jobs.db"):
        self._conn = sqlite3.connect(path)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                emx2_job_id TEXT PRIMARY KEY,
                claimed_at TEXT NOT NULL,
                status TEXT NOT NULL
                    CHECK (status IN ('claimed', 'submitted', 'completed')),
                slurm_job_id TEXT
            )
            """
        )
        self._conn.commit()

    def add_claimed(self, emx2_job_id: str) -> StoredJob:
        """Record a claimed job with status CLAIMED.

        Idempotent: if the job is already stored, the existing record is returned
        unchanged, so a claim can safely be retried.
        """
        claimed_at = datetime.now(timezone.utc).isoformat()
        with self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO jobs (emx2_job_id, claimed_at, status) "
                "VALUES (?, ?, ?)",
                (emx2_job_id, claimed_at, JobStatus.CLAIMED.value),
            )
        return self.get(emx2_job_id)

    def update_status(self, emx2_job_id: str, status: JobStatus) -> None:
        with self._conn:
            cursor = self._conn.execute(
                "UPDATE jobs SET status = ? WHERE emx2_job_id = ?",
                (JobStatus(status).value, emx2_job_id),
            )
        if cursor.rowcount == 0:
            raise KeyError(emx2_job_id)

    def set_submitted(self, emx2_job_id: str, slurm_job_id: str) -> None:
        """Record that a job was submitted to Slurm as `slurm_job_id`."""
        with self._conn:
            cursor = self._conn.execute(
                "UPDATE jobs SET status = ?, slurm_job_id = ? WHERE emx2_job_id = ?",
                (JobStatus.SUBMITTED.value, slurm_job_id, emx2_job_id),
            )
        if cursor.rowcount == 0:
            raise KeyError(emx2_job_id)

    def get(self, emx2_job_id: str) -> StoredJob | None:
        row = self._conn.execute(
            f"SELECT {_COLUMNS} FROM jobs WHERE emx2_job_id = ?",
            (emx2_job_id,),
        ).fetchone()
        return self._to_job(row) if row else None

    def list_by_status(self, *statuses: JobStatus) -> list[StoredJob]:
        values = [JobStatus(status).value for status in statuses]
        placeholders = ", ".join("?" for _ in values)
        rows = self._conn.execute(
            f"SELECT {_COLUMNS} FROM jobs WHERE status IN ({placeholders}) "
            "ORDER BY claimed_at",
            values,
        ).fetchall()
        return [self._to_job(row) for row in rows]

    def close(self) -> None:
        self._conn.close()

    @staticmethod
    def _to_job(row: tuple) -> StoredJob:
        emx2_job_id, claimed_at, status, slurm_job_id = row
        return StoredJob(
            emx2_job_id,
            datetime.fromisoformat(claimed_at),
            JobStatus(status),
            slurm_job_id,
        )
