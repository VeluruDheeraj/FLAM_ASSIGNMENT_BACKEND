"""CLI interface for QueueCTL."""
import click
import json
import sys
import uuid
from datetime import datetime
from typing import Optional
from .models import Job, JobState
from .storage import JobStorage
from .worker import Worker, start_workers


@click.group()
def main():
    """QueueCTL - Background Job Queue System"""
    pass


@main.command()
@click.argument("job_data", required=False, default=None)
@click.option("--max-retries", default=3, type=int, help="Maximum retry attempts")
def enqueue(job_data: Optional[str], max_retries: int):
    """Enqueue a new job.
    
    Examples:
    queuectl enqueue "echo hello"
    queuectl enqueue '{"id":"job1","command":"echo hello"}' --max-retries 5
    """
    storage = JobStorage()
    
    try:
        if job_data:
            # Try to parse as JSON first
            try:
                data = json.loads(job_data)
                job = Job.from_dict(data)
            except json.JSONDecodeError:
                # If not JSON, treat as plain command string
                job = Job(
                    id=str(uuid.uuid4())[:8],
                    command=job_data, 
                    max_retries=max_retries
                )
        else:
            # Interactive mode
            click.echo("Enter job details:")
            command = click.prompt("Command to execute")
            max_retries = click.prompt("Max retries", default=3, type=int)
            
            job = Job(
                id=str(uuid.uuid4())[:8],
                command=command,
                max_retries=max_retries,
            )
        
        storage.add_job(job)
        click.echo(f"✓ Job enqueued: {job.id}")
        click.echo(json.dumps(job.to_dict(), indent=2))
    
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@main.group()
def worker():
    """Manage worker processes"""
    pass


@worker.command()
@click.option("--count", default=1, type=int, help="Number of workers to start")
@click.option("--poll-interval", default=1.0, type=float, help="Poll interval in seconds")
def start(count: int, poll_interval: float):
    """Start one or more workers.
    
    Example:
    queuectl worker start --count 3
    """
    try:
        start_workers(count, poll_interval)
    except KeyboardInterrupt:
        click.echo("\nWorkers stopped.")


@worker.command()
def stop():
    """Stop running workers gracefully.
    
    Example:
    queuectl worker stop
    """
    click.echo("Note: Workers will stop after completing current jobs.")
    click.echo("Press Ctrl+C in the worker terminal to force stop.")
    click.echo("\nTo gracefully stop workers, press Ctrl+C in the terminal where workers are running.")


@main.command()
def status():
    """Show summary of queue and worker status.
    
    Example:
    queuectl status
    """
    storage = JobStorage()
    
    try:
        counts = storage.count_jobs_by_state()
        dlq_jobs = storage.get_dlq_jobs()
        
        click.echo("\n" + "="*50)
        click.echo("Queue Status".center(50))
        click.echo("="*50)
        
        click.echo(f"\nJob States:")
        for state in JobState:
            count = counts.get(state.value, 0)
            icon = {
                JobState.PENDING: "⏳",
                JobState.PROCESSING: "⚙️ ",
                JobState.COMPLETED: "✓ ",
                JobState.FAILED: "✗ ",
                JobState.DEAD: "💀",
            }.get(state, "  ")
            click.echo(f"  {icon} {state.value.ljust(12)} : {count}")
        
        click.echo(f"\nDead Letter Queue: {len(dlq_jobs)} jobs")
        
        click.echo("\n" + "="*50 + "\n")
    
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@main.command()
@click.option("--state", default=None, help="Filter by job state (pending, completed, failed, dead, processing)")
@click.option("--limit", default=50, type=int, help="Limit number of results")
def list(state: Optional[str], limit: int):
    """List jobs, optionally filtered by state.
    
    Example:
    queuectl list --state pending
    queuectl list --state failed
    """
    storage = JobStorage()
    
    try:
        if state:
            jobs = storage.get_jobs_by_state(state)
        else:
            jobs = storage.get_all_jobs()
        
        jobs = jobs[:limit]
        
        if not jobs:
            click.echo("No jobs found.")
            return
        
        click.echo(f"\n{len(jobs)} job(s) found:\n")
        
        for job in jobs:
            click.echo(f"ID: {job.id}")
            click.echo(f"  State:      {job.state}")
            click.echo(f"  Command:    {job.command}")
            click.echo(f"  Attempts:   {job.attempts}/{job.max_retries}")
            click.echo(f"  Created:    {job.created_at}")
            click.echo(f"  Updated:    {job.updated_at}")
            if job.next_retry_at:
                click.echo(f"  Retry at:   {job.next_retry_at}")
            if job.error_message:
                click.echo(f"  Error:      {job.error_message}")
            click.echo()
    
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@main.group()
def dlq():
    """Manage Dead Letter Queue"""
    pass


@dlq.command(name="list")
@click.option("--limit", default=50, type=int, help="Limit number of results")
def dlq_list(limit: int):
    """List jobs in the Dead Letter Queue.
    
    Example:
    queuectl dlq list
    """
    storage = JobStorage()
    
    try:
        jobs = storage.get_dlq_jobs()
        jobs = jobs[:limit]
        
        if not jobs:
            click.echo("Dead Letter Queue is empty.")
            return
        
        click.echo(f"\n{len(jobs)} job(s) in DLQ:\n")
        
        for job in jobs:
            click.echo(f"ID: {job.id}")
            click.echo(f"  Command:    {job.command}")
            click.echo(f"  Attempts:   {job.attempts}")
            click.echo(f"  Max Retries: {job.max_retries}")
            click.echo(f"  Created:    {job.created_at}")
            if job.error_message:
                click.echo(f"  Error:      {job.error_message}")
            click.echo()
    
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@dlq.command(name="retry")
@click.argument("job_id")
def dlq_retry(job_id: str):
    """Retry a job from the Dead Letter Queue.
    
    Example:
    queuectl dlq retry job1
    """
    storage = JobStorage()
    
    try:
        job = storage.get_dlq_job(job_id)
        
        if not job:
            click.echo(f"Job {job_id} not found in DLQ", err=True)
            sys.exit(1)
        
        # Reset job state for retry
        job.state = JobState.PENDING
        job.attempts = 0
        job.error_message = None
        job.next_retry_at = None
        
        storage.add_job(job)
        storage.remove_from_dlq(job_id)
        
        click.echo(f"✓ Job {job_id} moved back to queue for retry")
    
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@dlq.command(name="remove")
@click.argument("job_id")
def dlq_remove(job_id: str):
    """Remove a job from the Dead Letter Queue.
    
    Example:
    queuectl dlq remove job1
    """
    storage = JobStorage()
    
    try:
        job = storage.get_dlq_job(job_id)
        
        if not job:
            click.echo(f"Job {job_id} not found in DLQ", err=True)
            sys.exit(1)
        
        storage.remove_from_dlq(job_id)
        
        click.echo(f"✓ Job {job_id} removed from DLQ")
    
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@main.group()
def config():
    """Manage configuration"""
    pass


@config.command(name="get")
@click.argument("key", required=False)
def config_get(key: Optional[str]):
    """Get configuration value(s).
    
    Example:
    queuectl config get
    queuectl config get max-retries
    """
    storage = JobStorage()
    
    try:
        cfg = storage.get_config()
        
        if key:
            if key not in cfg:
                click.echo(f"Configuration key '{key}' not found", err=True)
                sys.exit(1)
            click.echo(f"{key}: {cfg[key]}")
        else:
            click.echo("\nCurrent Configuration:")
            for k, v in cfg.items():
                click.echo(f"  {k}: {v}")
            click.echo()
    
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@config.command(name="set")
@click.argument("key")
@click.argument("value")
def config_set(key: str, value: str):
    """Set configuration value.
    
    Example:
    queuectl config set max-retries 5
    queuectl config set backoff-base 3
    """
    storage = JobStorage()
    
    try:
        # Try to convert to int if possible
        try:
            typed_value = int(value)
        except ValueError:
            try:
                typed_value = float(value)
            except ValueError:
                typed_value = value
        
        storage.set_config(key, typed_value)
        click.echo(f"✓ Configuration updated: {key} = {typed_value}")
    
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@main.command()
@click.argument("job_id")
def show(job_id: str):
    """Show detailed information about a job.
    
    Example:
    queuectl show job1
    """
    storage = JobStorage()
    
    try:
        job = storage.get_job(job_id)
        
        if not job:
            click.echo(f"Job {job_id} not found", err=True)
            sys.exit(1)
        
        click.echo(f"\nJob: {job.id}\n")
        click.echo(json.dumps(job.to_dict(), indent=2))
        click.echo()
    
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@main.command()
@click.argument("job_id")
@click.confirmation_option(prompt="Are you sure you want to delete this job?")
def delete(job_id: str):
    """Delete a job.
    
    Example:
    queuectl delete job1
    """
    storage = JobStorage()
    
    try:
        job = storage.get_job(job_id)
        
        if not job:
            click.echo(f"Job {job_id} not found", err=True)
            sys.exit(1)
        
        storage.delete_job(job_id)
        click.echo(f"✓ Job {job_id} deleted")
    
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
