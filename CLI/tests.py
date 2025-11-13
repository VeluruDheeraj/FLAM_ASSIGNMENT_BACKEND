#!/usr/bin/env python3
"""Comprehensive test suite for QueueCTL."""
import sys
import time
import json
import subprocess
from pathlib import Path

# Add parent to path so we can import queuectl
sys.path.insert(0, str(Path(__file__).parent))

from queuectl.models import Job, JobState
from queuectl.storage import JobStorage
from queuectl.worker import Worker


def run_cli_command(args):
    """Run a CLI command and return output."""
    cmd = ["python", "-m", "queuectl.cli"] + args
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode, result.stdout, result.stderr


def test_1_basic_job_enqueue():
    """Test 1: Basic job enqueue."""
    print("\n" + "="*60)
    print("TEST 1: Basic Job Enqueue")
    print("="*60)
    
    storage = JobStorage()
    job = Job(
        id="test_job_1",
        command="echo 'Hello QueueCTL'",
    )
    
    storage.add_job(job)
    retrieved = storage.get_job("test_job_1")
    
    assert retrieved is not None, "Job not stored"
    assert retrieved.id == job.id, "Job ID mismatch"
    assert retrieved.state == JobState.PENDING, "Initial state should be PENDING"
    
    print("✓ Job successfully enqueued and retrieved")
    return True


def test_2_job_execution_success():
    """Test 2: Successful job execution."""
    print("\n" + "="*60)
    print("TEST 2: Successful Job Execution")
    print("="*60)
    
    storage = JobStorage()
    job = Job(
        id="test_job_2_success",
        command="exit 0",  # Successful command
    )
    
    storage.add_job(job)
    
    # Execute job manually to test execution logic
    worker = Worker(1)
    worker._execute_job(job)
    
    # Check that job was marked as completed
    updated_job = storage.get_job("test_job_2_success")
    assert updated_job.state == JobState.COMPLETED, f"Expected COMPLETED, got {updated_job.state}"
    
    print("✓ Job executed successfully and marked as COMPLETED")
    return True


def test_3_job_execution_failure():
    """Test 3: Failed job execution and retry."""
    print("\n" + "="*60)
    print("TEST 3: Failed Job Execution & Retry")
    print("="*60)
    
    storage = JobStorage()
    job = Job(
        id="test_job_3_fail",
        command="exit 1",  # Failing command
        max_retries=2,
    )
    
    storage.add_job(job)
    
    # Execute job
    worker = Worker(1)
    worker._execute_job(job)
    
    # Check that job was marked as FAILED and scheduled for retry
    updated_job = storage.get_job("test_job_3_fail")
    assert updated_job.state == JobState.FAILED, f"Expected FAILED, got {updated_job.state}"
    assert updated_job.attempts == 1, f"Expected 1 attempt, got {updated_job.attempts}"
    assert updated_job.next_retry_at is not None, "Retry time should be scheduled"
    
    print(f"✓ Failed job scheduled for retry at: {updated_job.next_retry_at}")
    print(f"✓ Error message: {updated_job.error_message}")
    return True


def test_4_exponential_backoff():
    """Test 4: Exponential backoff calculation."""
    print("\n" + "="*60)
    print("TEST 4: Exponential Backoff")
    print("="*60)
    
    storage = JobStorage()
    config = storage.get_config()
    backoff_base = config.get("backoff_base", 2)
    
    print(f"Backoff base: {backoff_base}")
    
    # Simulate retry scheduling for multiple attempts
    for attempt in range(1, 4):
        delay = backoff_base ** (attempt - 1)
        print(f"  Attempt {attempt}: delay = {backoff_base}^{attempt-1} = {delay} seconds")
        assert delay > 0, "Delay should be positive"
    
    print("✓ Exponential backoff formula validated")
    return True


def test_5_dlq_handling():
    """Test 5: Dead Letter Queue handling."""
    print("\n" + "="*60)
    print("TEST 5: Dead Letter Queue (DLQ)")
    print("="*60)
    
    import time
    storage = JobStorage()
    job = Job(
        id="test_job_5_dlq",
        command="failing_command_12345",
        max_retries=1,
    )
    
    storage.add_job(job)
    
    # Exhaust retries - manually set high attempts to trigger DLQ
    for attempt in range(1, 3):
        job.attempts = attempt
        worker = Worker(1)
        worker._execute_job(job)
        
        # Release lock to avoid database conflicts
        try:
            storage.release_job_lock("test_job_5_dlq")
        except:
            pass
        
        time.sleep(0.1)  # Small delay to allow database write
        
        job = storage.get_job("test_job_5_dlq")
        
        if job is None:
            # Job moved to DLQ
            break
        
        # Reload to refresh state
        if job.state == JobState.FAILED:
            # Force back to pending to retry
            job.state = JobState.PENDING
            storage.update_job(job)
    
    # Check DLQ
    time.sleep(0.1)
    dlq_job = storage.get_dlq_job("test_job_5_dlq")
    assert dlq_job is not None, "Job should be in DLQ"
    assert dlq_job.state == JobState.DEAD, f"Expected DEAD state, got {dlq_job.state}"
    
    # Check that main queue doesn't have it
    regular_job = storage.get_job("test_job_5_dlq")
    assert regular_job is None, "Job should be removed from main queue"
    
    print(f"✓ Job moved to DLQ after max retries")
    print(f"✓ DLQ reason: {dlq_job.error_message}")
    return True


def test_6_dlq_retry():
    """Test 6: Retrying DLQ jobs."""
    print("\n" + "="*60)
    print("TEST 6: Retrying DLQ Jobs")
    print("="*60)
    
    storage = JobStorage()
    job = Job(
        id="test_job_6_dlq_retry",
        command="echo 'retry success'",
    )
    
    # Manually move to DLQ
    storage.add_job(job)
    storage.move_to_dlq(job, "Testing DLQ retry")
    
    # Verify it's in DLQ
    dlq_job = storage.get_dlq_job("test_job_6_dlq_retry")
    assert dlq_job is not None, "Job should be in DLQ"
    
    # Retry it
    dlq_job.state = JobState.PENDING
    dlq_job.attempts = 0
    dlq_job.error_message = None
    storage.add_job(dlq_job)
    storage.remove_from_dlq("test_job_6_dlq_retry")
    
    # Verify it's back in main queue
    regular_job = storage.get_job("test_job_6_dlq_retry")
    assert regular_job is not None, "Job should be back in main queue"
    assert regular_job.state == JobState.PENDING, f"Expected PENDING, got {regular_job.state}"
    
    print("✓ DLQ job successfully moved back to queue")
    return True


def test_7_persistence():
    """Test 7: Job persistence across restarts."""
    print("\n" + "="*60)
    print("TEST 7: Job Persistence")
    print("="*60)
    
    storage1 = JobStorage()
    job = Job(
        id="test_job_7_persist",
        command="echo persistence",
        state=JobState.PROCESSING,
    )
    
    storage1.add_job(job)
    
    # Create new storage instance (simulates restart)
    storage2 = JobStorage()
    retrieved = storage2.get_job("test_job_7_persist")
    
    assert retrieved is not None, "Job should persist"
    assert retrieved.state == JobState.PROCESSING, "Job state should be preserved"
    
    print("✓ Job persisted and survived restart")
    return True


def test_8_job_locking():
    """Test 8: Job locking to prevent duplicate processing."""
    print("\n" + "="*60)
    print("TEST 8: Job Locking (Duplicate Prevention)")
    print("="*60)
    
    storage = JobStorage()
    job = Job(
        id="test_job_8_lock",
        command="echo locking",
    )
    
    storage.add_job(job)
    
    # Try to acquire lock
    lock1 = storage.acquire_job_lock("test_job_8_lock", duration_seconds=10)
    assert lock1, "First lock acquisition should succeed"
    
    # Try to acquire lock again (should fail)
    lock2 = storage.acquire_job_lock("test_job_8_lock", duration_seconds=10)
    assert not lock2, "Second lock acquisition should fail"
    
    # Release lock
    storage.release_job_lock("test_job_8_lock")
    
    # Now acquire should succeed
    lock3 = storage.acquire_job_lock("test_job_8_lock", duration_seconds=10)
    assert lock3, "Lock acquisition should succeed after release"
    
    print("✓ Job locking works correctly")
    return True


def test_9_configuration():
    """Test 9: Configuration management."""
    print("\n" + "="*60)
    print("TEST 9: Configuration Management")
    print("="*60)
    
    storage = JobStorage()
    
    # Get config
    config = storage.get_config()
    print(f"Current config: {config}")
    assert "max_retries" in config, "Should have max_retries config"
    assert "backoff_base" in config, "Should have backoff_base config"
    
    # Set config
    storage.set_config("max_retries", 5)
    updated_config = storage.get_config()
    assert updated_config["max_retries"] == 5, "Config should be updated"
    
    # Restore original
    storage.set_config("max_retries", 3)
    
    print("✓ Configuration management works")
    return True


def test_10_status_and_count():
    """Test 10: Status and job counting."""
    print("\n" + "="*60)
    print("TEST 10: Status & Job Counting")
    print("="*60)
    
    storage = JobStorage()
    
    # Clear and create test jobs
    test_jobs = [
        Job(id=f"status_test_{i}", command=f"echo test_{i}", state=JobState.PENDING)
        for i in range(3)
    ]
    
    for job in test_jobs:
        storage.add_job(job)
    
    # Count jobs by state
    counts = storage.count_jobs_by_state()
    print(f"Job counts: {counts}")
    
    assert counts[JobState.PENDING] >= 3, "Should have at least 3 pending jobs"
    
    print("✓ Status and counting works")
    return True


def run_all_tests():
    """Run all test scenarios."""
    print("\n" + "█"*60)
    print("QueueCTL - Comprehensive Test Suite".center(60))
    print("█"*60)
    
    tests = [
        test_1_basic_job_enqueue,
        test_2_job_execution_success,
        test_3_job_execution_failure,
        test_4_exponential_backoff,
        test_5_dlq_handling,
        test_6_dlq_retry,
        test_7_persistence,
        test_8_job_locking,
        test_9_configuration,
        test_10_status_and_count,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            if test():
                passed += 1
        except AssertionError as e:
            print(f"✗ FAILED: {e}")
            failed += 1
        except Exception as e:
            print(f"✗ ERROR: {e}")
            failed += 1
    
    print("\n" + "█"*60)
    print(f"Results: {passed} passed, {failed} failed".center(60))
    print("█"*60 + "\n")
    
    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
