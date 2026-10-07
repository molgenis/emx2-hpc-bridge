from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum


class JobStatus(str, Enum):
    CLAIMED = "claimed"
    SUBMITTED = "submitted"
    COMPLETED = "completed"


@dataclass
class StoredJob:
    job_id: str
    claimed_at: datetime
    status: JobStatus


class JobStore:
    """Persists jobs claimed by the poller in a local sqlite3 database."""

    def __init__(self, path: str = "jobs.db"):
        self._conn = sqlite3.connect(path)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                claimed_at TEXT NOT NULL,
                status TEXT NOT NULL
                    CHECK (status IN ('claimed', 'submitted', 'completed'))
            )
            """)
        self._conn.commit()

    def add_claimed(self, job_id: str) -> StoredJob:
        """Record a claimed job with status CLAIMED.

        Idempotent: if the job is already stored, the existing record is returned
        unchanged, so a claim can safely be retried.
        """
        claimed_at = datetime.now(timezone.utc).isoformat()
        with self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO jobs (job_id, claimed_at, status) "
                "VALUES (?, ?, ?)",
                (job_id, claimed_at, JobStatus.CLAIMED.value),
            )
        return self.get(job_id)

    def update_status(self, job_id: str, status: JobStatus) -> None:
        with self._conn:
            cursor = self._conn.execute(
                "UPDATE jobs SET status = ? WHERE job_id = ?",
                (JobStatus(status).value, job_id),
            )
        if cursor.rowcount == 0:
            raise KeyError(job_id)

    def get(self, job_id: str) -> StoredJob | None:
        row = self._conn.execute(
            "SELECT job_id, claimed_at, status FROM jobs WHERE job_id = ?",
            (job_id,),
        ).fetchone()
        return self._to_job(row) if row else None

    def list_by_status(self, status: JobStatus) -> list[StoredJob]:
        rows = self._conn.execute(
            "SELECT job_id, claimed_at, status FROM jobs WHERE status = ? "
            "ORDER BY claimed_at",
            (JobStatus(status).value,),
        ).fetchall()
        return [self._to_job(row) for row in rows]

    def close(self) -> None:
        self._conn.close()

    @staticmethod
    def _to_job(row: tuple) -> StoredJob:
        job_id, claimed_at, status = row
        return StoredJob(job_id, datetime.fromisoformat(claimed_at), JobStatus(status))
