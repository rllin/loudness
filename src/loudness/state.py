"""SQLite-backed state manager for job persistence and crash recovery."""

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import aiosqlite

from .models import (
    BusinessResult,
    Checkpoint,
    Job,
    JobStatus,
    UrlStatus,
    UrlTask,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    total_urls INTEGER NOT NULL DEFAULT 0,
    completed INTEGER NOT NULL DEFAULT 0,
    failed INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS urls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    alias TEXT NOT NULL,
    url TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    result TEXT,
    updated_at TEXT,
    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS checkpoints (
    job_id TEXT PRIMARY KEY,
    last_url_id INTEGER,
    state TEXT NOT NULL DEFAULT '{}',
    saved_at TEXT NOT NULL,
    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_urls_job_id ON urls(job_id);
CREATE INDEX IF NOT EXISTS idx_urls_status ON urls(status);
CREATE INDEX IF NOT EXISTS idx_urls_job_status ON urls(job_id, status);
"""


def extract_alias_from_url(url: str) -> str:
    """Extract business alias from Yelp URL."""
    parsed = urlparse(url)
    path = parsed.path
    if path.startswith("/biz/"):
        alias = path[5:].split("?")[0].split("/")[0]
        return alias
    return url


class StateManager:
    """Manages job state persistence in SQLite."""

    def __init__(self, db_path: Path | str = "loudness.db"):
        self.db_path = Path(db_path)
        self._connection: aiosqlite.Connection | None = None

    async def __aenter__(self) -> "StateManager":
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()

    async def connect(self) -> None:
        """Connect to database and initialize schema."""
        self._connection = await aiosqlite.connect(self.db_path)
        self._connection.row_factory = aiosqlite.Row
        await self._connection.executescript(SCHEMA)
        await self._connection.commit()

    async def close(self) -> None:
        """Close database connection."""
        if self._connection:
            await self._connection.close()
            self._connection = None

    @property
    def conn(self) -> aiosqlite.Connection:
        """Get current connection, raising if not connected."""
        if not self._connection:
            raise RuntimeError("Not connected to database")
        return self._connection

    async def create_job(self, urls: list[str]) -> str:
        """Create a new job with the given URLs."""
        job_id = str(uuid.uuid4())[:8]
        now = datetime.utcnow().isoformat()

        await self.conn.execute(
            """
            INSERT INTO jobs (id, created_at, status, total_urls, completed, failed)
            VALUES (?, ?, ?, ?, 0, 0)
            """,
            (job_id, now, JobStatus.PENDING.value, len(urls)),
        )

        for url in urls:
            alias = extract_alias_from_url(url)
            await self.conn.execute(
                """
                INSERT INTO urls (job_id, alias, url, status, attempts, updated_at)
                VALUES (?, ?, ?, ?, 0, ?)
                """,
                (job_id, alias, url, UrlStatus.PENDING.value, now),
            )

        await self.conn.commit()
        return job_id

    async def get_job(self, job_id: str) -> Job | None:
        """Get job by ID."""
        async with self.conn.execute(
            "SELECT * FROM jobs WHERE id = ?", (job_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            return Job(
                id=row["id"],
                created_at=datetime.fromisoformat(row["created_at"]),
                status=JobStatus(row["status"]),
                total_urls=row["total_urls"],
                completed=row["completed"],
                failed=row["failed"],
            )

    async def get_all_jobs(self) -> list[Job]:
        """Get all jobs."""
        async with self.conn.execute(
            "SELECT * FROM jobs ORDER BY created_at DESC"
        ) as cursor:
            rows = await cursor.fetchall()
            return [
                Job(
                    id=row["id"],
                    created_at=datetime.fromisoformat(row["created_at"]),
                    status=JobStatus(row["status"]),
                    total_urls=row["total_urls"],
                    completed=row["completed"],
                    failed=row["failed"],
                )
                for row in rows
            ]

    async def update_job_status(self, job_id: str, status: JobStatus) -> None:
        """Update job status."""
        await self.conn.execute(
            "UPDATE jobs SET status = ? WHERE id = ?",
            (status.value, job_id),
        )
        await self.conn.commit()

    async def get_pending_urls(
        self, job_id: str, limit: int = 100, max_attempts: int = 3
    ) -> list[UrlTask]:
        """Get pending URLs for a job, including failed ones under max attempts."""
        async with self.conn.execute(
            """
            SELECT * FROM urls
            WHERE job_id = ?
              AND (status = ? OR (status = ? AND attempts < ?))
            ORDER BY id
            LIMIT ?
            """,
            (
                job_id,
                UrlStatus.PENDING.value,
                UrlStatus.FAILED.value,
                max_attempts,
                limit,
            ),
        ) as cursor:
            rows = await cursor.fetchall()
            return [self._row_to_url_task(row) for row in rows]

    async def get_url_task(self, url_id: int) -> UrlTask | None:
        """Get a URL task by ID."""
        async with self.conn.execute(
            "SELECT * FROM urls WHERE id = ?", (url_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            return self._row_to_url_task(row)

    def _row_to_url_task(self, row: aiosqlite.Row) -> UrlTask:
        """Convert database row to UrlTask."""
        result = None
        if row["result"]:
            try:
                result_data = json.loads(row["result"])
                result = BusinessResult(**result_data)
            except (json.JSONDecodeError, ValueError):
                pass

        return UrlTask(
            id=row["id"],
            job_id=row["job_id"],
            alias=row["alias"],
            url=row["url"],
            status=UrlStatus(row["status"]),
            attempts=row["attempts"],
            last_error=row["last_error"],
            result=result,
            updated_at=(
                datetime.fromisoformat(row["updated_at"]) if row["updated_at"] else None
            ),
        )

    async def mark_in_progress(self, url_id: int) -> None:
        """Mark a URL as in progress."""
        now = datetime.utcnow().isoformat()
        await self.conn.execute(
            """
            UPDATE urls
            SET status = ?, updated_at = ?
            WHERE id = ?
            """,
            (UrlStatus.IN_PROGRESS.value, now, url_id),
        )
        await self.conn.commit()

    async def mark_completed(self, url_id: int, result: BusinessResult) -> None:
        """Mark a URL as completed with result."""
        now = datetime.utcnow().isoformat()
        result_json = result.model_dump_json()

        async with self.conn.execute(
            "SELECT job_id FROM urls WHERE id = ?", (url_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return
            job_id = row["job_id"]

        await self.conn.execute(
            """
            UPDATE urls
            SET status = ?, result = ?, updated_at = ?, attempts = attempts + 1
            WHERE id = ?
            """,
            (UrlStatus.COMPLETED.value, result_json, now, url_id),
        )

        await self.conn.execute(
            "UPDATE jobs SET completed = completed + 1 WHERE id = ?",
            (job_id,),
        )
        await self.conn.commit()

    async def mark_failed(
        self, url_id: int, error: str, increment_attempts: bool = True
    ) -> None:
        """Mark a URL as failed with error."""
        now = datetime.utcnow().isoformat()

        async with self.conn.execute(
            "SELECT job_id FROM urls WHERE id = ?", (url_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return
            job_id = row["job_id"]

        if increment_attempts:
            await self.conn.execute(
                """
                UPDATE urls
                SET status = ?, last_error = ?, updated_at = ?, attempts = attempts + 1
                WHERE id = ?
                """,
                (UrlStatus.FAILED.value, error, now, url_id),
            )
        else:
            await self.conn.execute(
                """
                UPDATE urls
                SET status = ?, last_error = ?, updated_at = ?
                WHERE id = ?
                """,
                (UrlStatus.FAILED.value, error, now, url_id),
            )

        await self.conn.execute(
            "UPDATE jobs SET failed = failed + 1 WHERE id = ?",
            (job_id,),
        )
        await self.conn.commit()

    async def mark_skipped(self, url_id: int, reason: str) -> None:
        """Mark a URL as skipped."""
        now = datetime.utcnow().isoformat()
        await self.conn.execute(
            """
            UPDATE urls
            SET status = ?, last_error = ?, updated_at = ?
            WHERE id = ?
            """,
            (UrlStatus.SKIPPED.value, reason, now, url_id),
        )
        await self.conn.commit()

    async def save_checkpoint(self, job_id: str, state: dict[str, Any]) -> None:
        """Save a checkpoint for the job."""
        now = datetime.utcnow().isoformat()
        state_json = json.dumps(state)

        async with self.conn.execute(
            "SELECT last_url_id FROM checkpoints WHERE job_id = ?", (job_id,)
        ) as cursor:
            row = await cursor.fetchone()

        async with self.conn.execute(
            """
            SELECT MAX(id) as last_id FROM urls
            WHERE job_id = ? AND status IN (?, ?)
            """,
            (job_id, UrlStatus.COMPLETED.value, UrlStatus.FAILED.value),
        ) as cursor:
            last_row = await cursor.fetchone()
            last_url_id = last_row["last_id"] if last_row else None

        if row:
            await self.conn.execute(
                """
                UPDATE checkpoints
                SET last_url_id = ?, state = ?, saved_at = ?
                WHERE job_id = ?
                """,
                (last_url_id, state_json, now, job_id),
            )
        else:
            await self.conn.execute(
                """
                INSERT INTO checkpoints (job_id, last_url_id, state, saved_at)
                VALUES (?, ?, ?, ?)
                """,
                (job_id, last_url_id, state_json, now),
            )
        await self.conn.commit()

    async def load_checkpoint(self, job_id: str) -> Checkpoint | None:
        """Load checkpoint for a job."""
        async with self.conn.execute(
            "SELECT * FROM checkpoints WHERE job_id = ?", (job_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            return Checkpoint(
                job_id=row["job_id"],
                last_url_id=row["last_url_id"],
                state=json.loads(row["state"]),
                saved_at=datetime.fromisoformat(row["saved_at"]),
            )

    async def get_job_stats(self, job_id: str) -> dict[str, int]:
        """Get statistics for a job."""
        stats: dict[str, int] = {}

        async with self.conn.execute(
            """
            SELECT status, COUNT(*) as count
            FROM urls WHERE job_id = ?
            GROUP BY status
            """,
            (job_id,),
        ) as cursor:
            rows = await cursor.fetchall()
            for row in rows:
                stats[row["status"]] = row["count"]

        return stats

    async def get_results(self, job_id: str) -> list[BusinessResult]:
        """Get all completed results for a job."""
        async with self.conn.execute(
            """
            SELECT result FROM urls
            WHERE job_id = ? AND status = ? AND result IS NOT NULL
            ORDER BY id
            """,
            (job_id, UrlStatus.COMPLETED.value),
        ) as cursor:
            rows = await cursor.fetchall()
            results = []
            for row in rows:
                try:
                    data = json.loads(row["result"])
                    results.append(BusinessResult(**data))
                except (json.JSONDecodeError, ValueError):
                    continue
            return results

    async def reset_in_progress(self, job_id: str) -> int:
        """Reset in-progress URLs back to pending (for recovery)."""
        result = await self.conn.execute(
            """
            UPDATE urls
            SET status = ?
            WHERE job_id = ? AND status = ?
            """,
            (UrlStatus.PENDING.value, job_id, UrlStatus.IN_PROGRESS.value),
        )
        await self.conn.commit()
        return result.rowcount
