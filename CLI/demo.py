#!/usr/bin/env python3
"""Interactive demo script for QueueCTL."""
import json
import time
import subprocess
import sys
from pathlib import Path
import threading

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent))

from queuectl.storage import JobStorage


def run_command(cmd):
    """Run command and return output."""
    print(f"\n$ {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.stderr and "Worker" not in result.stderr:  # Filter worker logs
        print(result.stderr, file=sys.stderr)
    return result.returncode


def demo():
    """Interactive demo of QueueCTL."""
    print("\n" + "█"*70)
    print("QueueCTL - Background Job Queue System - DEMO".center(70))
    print("█"*70)
    
    # Setup commands
    setup_cmd = "python -m queuectl.cli"
    
    # Clear any previous state
    print("\n[1] Clearing previous state...")
    storage = JobStorage()
    # Database is persistent, so we keep it
    
    # 1. Enqueue some jobs
    print("\n[2] Enqueueing jobs...")
    
    jobs_to_queue = [
        {
            "id": "demo_job_1",
            "command": "echo 'Job 1: Success' && exit 0",
        },
        {
            "id": "demo_job_2",
            "command": "echo 'Job 2: Will fail' && exit 1",
        },
        {
            "id": "demo_job_3",
            "command": "echo 'Job 3: Sleeping...' && sleep 1 && echo 'Done'",
        },
    ]
    
    for job_data in jobs_to_queue:
        run_command(f"{setup_cmd} enqueue '{json.dumps(job_data)}'")
    
    # 2. Show status
    print("\n[3] Checking queue status...")
    run_command(f"{setup_cmd} status")
    
    # 3. List pending jobs
    print("\n[4] Listing pending jobs...")
    run_command(f"{setup_cmd} list --state pending")
    
    # 4. Start a single worker to process jobs
    print("\n[5] Starting 1 worker (this will take a moment)...")
    print("    Worker will process jobs for ~5 seconds then exit...\n")
    
    # Run worker with timeout
    worker_proc = subprocess.Popen(
        f"{setup_cmd} worker start --count 1 --poll-interval 0.5",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    
    time.sleep(5)  # Let it process
    worker_proc.terminate()
    worker_proc.wait(timeout=5)
    
    # 5. Check status after execution
    print("\n[6] Checking status after worker processing...")
    run_command(f"{setup_cmd} status")
    
    # 6. List completed jobs
    print("\n[7] Listing completed jobs...")
    run_command(f"{setup_cmd} list --state completed")
    
    # 7. List failed jobs
    print("\n[8] Listing failed jobs...")
    run_command(f"{setup_cmd} list --state failed")
    
    # 8. Check DLQ
    print("\n[9] Checking Dead Letter Queue...")
    run_command(f"{setup_cmd} dlq list")
    
    # 9. Configuration
    print("\n[10] Viewing configuration...")
    run_command(f"{setup_cmd} config get")
    
    # 10. Change config
    print("\n[11] Changing max-retries to 5...")
    run_command(f"{setup_cmd} config set max_retries 5")
    
    # 11. Show a specific job
    print("\n[12] Showing details for demo_job_1...")
    run_command(f"{setup_cmd} show demo_job_1")
    
    print("\n" + "█"*70)
    print("Demo completed!".center(70))
    print("█"*70 + "\n")


if __name__ == "__main__":
    demo()
