"""Worker process for executing jobs."""
import subprocess
import time
import signal
import os
from datetime import datetime, timedelta
from typing import Optional
from .models import Job, JobState
from .storage import JobStorage


class Worker:
    """Executes jobs from the queue with retry logic."""
    
    def __init__(self, worker_id: int):
        self.worker_id = worker_id
        self.storage = JobStorage()
        self.running = True
        self._setup_signal_handlers()
    
    def _setup_signal_handlers(self):
        """Setup graceful shutdown handlers."""
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)
    
    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signal gracefully."""
        print(f"\n[Worker-{self.worker_id}] Received shutdown signal, finishing current job...")
        self.running = False
    
    def run(self, poll_interval: float = 1.0) -> None:
        """Run the worker, continuously fetching and executing jobs."""
        print(f"[Worker-{self.worker_id}] Started")
        
        try:
            while self.running:
                self.storage.clear_expired_locks()
                
                # Get next pending job
                job = self._get_next_job()
                
                if job:
                    self._execute_job(job)
                else:
                    # No job available, wait before checking again
                    time.sleep(poll_interval)
        
        except Exception as e:
            print(f"[Worker-{self.worker_id}] Error: {e}")
        finally:
            print(f"[Worker-{self.worker_id}] Stopped")
    
    def _get_next_job(self) -> Optional[Job]:
        """Get the next pending job to execute."""
        # Include pending jobs and failed jobs whose retry time has arrived
        ready_jobs = self.storage.get_ready_jobs()

        for job in ready_jobs:
            # Try to acquire lock on this job
            if self.storage.acquire_job_lock(job.id, duration_seconds=300.0):
                # If the job was in FAILED state and its retry time has arrived,
                # bring it back to pending so other workers see it consistently.
                if job.state == JobState.FAILED:
                    job.state = JobState.PENDING
                    job.next_retry_at = None
                    self.storage.update_job(job)
                return job

        return None
    
    def _execute_job(self, job: Job) -> None:
        """Execute a job and handle the result."""
        job.state = JobState.PROCESSING
        job.attempts += 1
        self.storage.update_job(job)
        
        print(f"[Worker-{self.worker_id}] Executing job {job.id}: {job.command}")
        
        try:
            # Execute the command
            result = subprocess.run(
                job.command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=300.0,  # 5 minute timeout
            )
            
            if result.returncode == 0:
                # Job succeeded
                job.state = JobState.COMPLETED
                job.error_message = None
                print(f"[Worker-{self.worker_id}] Job {job.id} completed")
                self.storage.update_job(job)
            else:
                # Job failed
                self._handle_job_failure(job, result.stderr or result.stdout)
        
        except subprocess.TimeoutExpired:
            self._handle_job_failure(job, "Job execution timeout")
        except Exception as e:
            self._handle_job_failure(job, str(e))
        finally:
            self.storage.release_job_lock(job.id)
    
    def _handle_job_failure(self, job: Job, error_message: str) -> None:
        """Handle a failed job - either retry or move to DLQ."""
        config = self.storage.get_config()
        max_retries = config.get("max_retries", 3)
        backoff_base = config.get("backoff_base", 2)
        backoff_max = config.get("backoff_max_seconds", 600)
        
        if job.attempts < max_retries:
            # Schedule retry with exponential backoff
            delay = min(backoff_base ** (job.attempts - 1), backoff_max)
            next_retry = datetime.utcnow() + timedelta(seconds=delay)
            
            job.state = JobState.FAILED
            job.error_message = error_message
            job.next_retry_at = next_retry.isoformat() + "Z"
            
            print(f"[Worker-{self.worker_id}] Job {job.id} failed (attempt {job.attempts}/{max_retries}), "
                  f"will retry in {delay}s")
            
            self.storage.update_job(job)
            
            # Schedule for retry: move back to pending if retry time has arrived
            self._check_and_reschedule_failed_job(job, max_retries)
        else:
            # Max retries exceeded, move to DLQ
            job.state = JobState.DEAD
            job.error_message = error_message
            
            print(f"[Worker-{self.worker_id}] Job {job.id} exceeded max retries, moving to DLQ")
            
            self.storage.move_to_dlq(job, f"Max retries ({max_retries}) exceeded. Last error: {error_message}")
    
    def _check_and_reschedule_failed_job(self, job: Job, max_retries: int) -> None:
        """Check if a failed job is ready to be retried and reschedule if necessary."""
        if job.next_retry_at:
            next_retry_time = datetime.fromisoformat(job.next_retry_at.replace("Z", "+00:00"))
            current_time = datetime.utcnow()
            
            # Don't automatically reschedule, let a scheduler or next poll handle it
            # The CLI status/list commands can show which jobs are ready for retry


def start_workers(count: int, poll_interval: float = 1.0) -> None:
    """Start multiple worker processes."""
    import multiprocessing
    
    workers = []
    
    for i in range(count):
        worker = Worker(i + 1)
        process = multiprocessing.Process(target=worker.run, args=(poll_interval,))
        process.start()
        workers.append(process)
    
    print(f"Started {count} workers")
    
    try:
        for process in workers:
            process.join()
    except KeyboardInterrupt:
        print("\nShutting down workers...")
        for process in workers:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
