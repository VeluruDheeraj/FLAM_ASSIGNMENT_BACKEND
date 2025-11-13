"""Persistent job storage using SQLite."""
import sqlite3
import json
import os
from pathlib import Path
from typing import List, Optional
from datetime import datetime
from .models import Job, JobState


class JobStorage:
    """SQLite-based persistent job storage."""
    
    DB_PATH = Path.home() / ".queuectl" / "jobs.db"
    CONFIG_PATH = Path.home() / ".queuectl" / "config.json"
    
    def __init__(self):
        """Initialize storage and create database if needed."""
        self.db_path = self.DB_PATH
        self.config_path = self.CONFIG_PATH
        self._ensure_db()
        self._ensure_config()
    
    def _ensure_db(self):
        """Create database and tables if they don't exist."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        conn = sqlite3.connect(str(self.db_path), timeout=10.0)
        cursor = conn.cursor()
        
        # Jobs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                command TEXT NOT NULL,
                state TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_retries INTEGER NOT NULL DEFAULT 3,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                next_retry_at TEXT,
                error_message TEXT,
                locked_until REAL DEFAULT 0
            )
        """)
        
        # DLQ table (dead letter queue)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dlq (
                id TEXT PRIMARY KEY,
                job_data TEXT NOT NULL,
                moved_at TEXT NOT NULL,
                reason TEXT
            )
        """)
        
        conn.commit()
        conn.close()
    
    def _ensure_config(self):
        """Create config file if it doesn't exist."""
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        
        if not self.config_path.exists():
            default_config = {
                "max_retries": 3,
                "backoff_base": 2,
                "backoff_max_seconds": 600,
            }
            with open(self.config_path, "w") as f:
                json.dump(default_config, f, indent=2)
    
    def add_job(self, job: Job) -> None:
        """Add a new job to the queue."""
        job.updated_at = datetime.utcnow().isoformat() + "Z"
        
        conn = sqlite3.connect(str(self.db_path), timeout=10.0)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT OR REPLACE INTO jobs 
            (id, command, state, attempts, max_retries, created_at, updated_at, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            job.id,
            job.command,
            job.state,
            job.attempts,
            job.max_retries,
            job.created_at,
            job.updated_at,
            job.error_message,
        ))
        
        conn.commit()
        conn.close()
    
    def get_job(self, job_id: str) -> Optional[Job]:
        """Retrieve a job by ID."""
        conn = sqlite3.connect(str(self.db_path), timeout=10.0)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            return None
        
        return Job(
            id=row["id"],
            command=row["command"],
            state=row["state"],
            attempts=row["attempts"],
            max_retries=row["max_retries"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            next_retry_at=row["next_retry_at"],
            error_message=row["error_message"],
        )
    
    def get_jobs_by_state(self, state: str) -> List[Job]:
        """Get all jobs with a specific state."""
        conn = sqlite3.connect(str(self.db_path), timeout=10.0)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM jobs WHERE state = ? ORDER BY created_at", (state,))
        rows = cursor.fetchall()
        conn.close()
        
        jobs = []
        for row in rows:
            jobs.append(Job(
                id=row["id"],
                command=row["command"],
                state=row["state"],
                attempts=row["attempts"],
                max_retries=row["max_retries"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                next_retry_at=row["next_retry_at"],
                error_message=row["error_message"],
            ))
        
        return jobs

    def get_ready_jobs(self) -> List[Job]:
        """Return jobs that are ready to run.

        This includes jobs in `pending` state as well as jobs in `failed`
        state whose `next_retry_at` timestamp has passed.
        """
        conn = sqlite3.connect(str(self.db_path), timeout=10.0)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Select pending jobs
        cursor.execute("SELECT * FROM jobs WHERE state = ? ORDER BY created_at", (JobState.PENDING.value,))
        pending_rows = cursor.fetchall()

        # Select failed jobs where next_retry_at is not null and <= now
        cursor.execute("SELECT * FROM jobs WHERE state = ? AND next_retry_at IS NOT NULL", (JobState.FAILED.value,))
        failed_rows = cursor.fetchall()

        conn.close()

        jobs: List[Job] = []
        # Use naive UTC timestamp for comparisons to avoid offset-aware vs naive errors
        now = datetime.utcnow()

        for row in pending_rows:
            jobs.append(Job(
                id=row["id"],
                command=row["command"],
                state=row["state"],
                attempts=row["attempts"],
                max_retries=row["max_retries"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                next_retry_at=row["next_retry_at"],
                error_message=row["error_message"],
            ))

        for row in failed_rows:
            next_retry = None
            if row["next_retry_at"]:
                try:
                    next_retry = datetime.fromisoformat(row["next_retry_at"].replace("Z", "+00:00"))
                    # convert to naive UTC for safe comparison
                    next_retry = next_retry.replace(tzinfo=None)
                except Exception:
                    next_retry = None

            if next_retry is None or next_retry <= now:
                jobs.append(Job(
                    id=row["id"],
                    command=row["command"],
                    state=row["state"],
                    attempts=row["attempts"],
                    max_retries=row["max_retries"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                    next_retry_at=row["next_retry_at"],
                    error_message=row["error_message"],
                ))

        return jobs
    
    def get_all_jobs(self) -> List[Job]:
        """Get all jobs."""
        conn = sqlite3.connect(str(self.db_path), timeout=10.0)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM jobs ORDER BY created_at DESC")
        rows = cursor.fetchall()
        conn.close()
        
        jobs = []
        for row in rows:
            jobs.append(Job(
                id=row["id"],
                command=row["command"],
                state=row["state"],
                attempts=row["attempts"],
                max_retries=row["max_retries"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                next_retry_at=row["next_retry_at"],
                error_message=row["error_message"],
            ))
        
        return jobs
    
    def update_job(self, job: Job) -> None:
        """Update an existing job."""
        job.updated_at = datetime.utcnow().isoformat() + "Z"
        
        conn = sqlite3.connect(str(self.db_path), timeout=10.0)
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE jobs SET
                command = ?,
                state = ?,
                attempts = ?,
                max_retries = ?,
                created_at = ?,
                updated_at = ?,
                next_retry_at = ?,
                error_message = ?
            WHERE id = ?
        """, (
            job.command,
            job.state,
            job.attempts,
            job.max_retries,
            job.created_at,
            job.updated_at,
            job.next_retry_at,
            job.error_message,
            job.id,
        ))
        
        conn.commit()
        conn.close()
    
    def delete_job(self, job_id: str) -> None:
        """Delete a job from the queue."""
        conn = sqlite3.connect(str(self.db_path), timeout=10.0)
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        
        conn.commit()
        conn.close()
    
    def move_to_dlq(self, job: Job, reason: str = "Max retries exceeded") -> None:
        """Move a job to the dead letter queue."""
        conn = sqlite3.connect(str(self.db_path), timeout=10.0)
        cursor = conn.cursor()
        
        moved_at = datetime.utcnow().isoformat() + "Z"
        cursor.execute("""
            INSERT INTO dlq (id, job_data, moved_at, reason)
            VALUES (?, ?, ?, ?)
        """, (
            job.id,
            job.to_json(),
            moved_at,
            reason,
        ))
        
        # Delete from main queue
        cursor.execute("DELETE FROM jobs WHERE id = ?", (job.id,))
        
        conn.commit()
        conn.close()
    
    def get_dlq_jobs(self) -> List[Job]:
        """Get all dead letter queue jobs."""
        conn = sqlite3.connect(str(self.db_path), timeout=10.0)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT job_data FROM dlq ORDER BY moved_at DESC")
        rows = cursor.fetchall()
        conn.close()
        
        jobs = []
        for row in rows:
            jobs.append(Job.from_json(row["job_data"]))
        
        return jobs
    
    def get_dlq_job(self, job_id: str) -> Optional[Job]:
        """Get a specific DLQ job."""
        conn = sqlite3.connect(str(self.db_path), timeout=10.0)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT job_data FROM dlq WHERE id = ?", (job_id,))
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            return None
        
        return Job.from_json(row["job_data"])
    
    def remove_from_dlq(self, job_id: str) -> None:
        """Remove a job from the DLQ."""
        conn = sqlite3.connect(str(self.db_path), timeout=10.0)
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM dlq WHERE id = ?", (job_id,))
        
        conn.commit()
        conn.close()
    
    def get_config(self) -> dict:
        """Get configuration."""
        with open(self.config_path, "r") as f:
            return json.load(f)
    
    def set_config(self, key: str, value) -> None:
        """Set a configuration value."""
        config = self.get_config()
        config[key] = value
        
        with open(self.config_path, "w") as f:
            json.dump(config, f, indent=2)
    
    def acquire_job_lock(self, job_id: str, duration_seconds: float = 60.0) -> bool:
        """Try to acquire a lock on a job. Returns True if successful."""
        import time
        
        conn = sqlite3.connect(str(self.db_path), timeout=10.0)
        cursor = conn.cursor()
        
        current_time = time.time()
        
        # Check if lock is available
        cursor.execute("SELECT locked_until FROM jobs WHERE id = ?", (job_id,))
        row = cursor.fetchone()
        
        if row is None:
            conn.close()
            return False
        
        locked_until = row[0]
        
        if locked_until < current_time:
            # Lock is free, acquire it
            new_lock_time = current_time + duration_seconds
            cursor.execute("UPDATE jobs SET locked_until = ? WHERE id = ?", 
                         (new_lock_time, job_id))
            conn.commit()
            conn.close()
            return True
        
        conn.close()
        return False
    
    def release_job_lock(self, job_id: str) -> None:
        """Release a lock on a job."""
        conn = sqlite3.connect(str(self.db_path), timeout=10.0)
        cursor = conn.cursor()
        
        cursor.execute("UPDATE jobs SET locked_until = 0 WHERE id = ?", (job_id,))
        
        conn.commit()
        conn.close()
    
    def clear_expired_locks(self) -> None:
        """Clear locks that have expired."""
        import time
        
        conn = sqlite3.connect(str(self.db_path), timeout=10.0)
        cursor = conn.cursor()
        
        current_time = time.time()
        cursor.execute("UPDATE jobs SET locked_until = 0 WHERE locked_until < ? AND locked_until > 0", 
                      (current_time,))
        
        conn.commit()
        conn.close()
    
    def count_jobs_by_state(self) -> dict:
        """Count jobs in each state."""
        conn = sqlite3.connect(str(self.db_path), timeout=10.0)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT state, COUNT(*) as count FROM jobs GROUP BY state
        """)
        rows = cursor.fetchall()
        conn.close()
        
        counts = {state.value: 0 for state in JobState}
        for state, count in rows:
            counts[state] = count
        
        return counts
