"""Data models for job queue system."""
from enum import Enum
from datetime import datetime
from typing import Optional
import json


class JobState(str, Enum):
    """Job lifecycle states."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead"


class Job:
    """Represents a background job."""
    
    def __init__(
        self,
        id: str,
        command: str,
        state: str = JobState.PENDING,
        attempts: int = 0,
        max_retries: int = 3,
        created_at: Optional[str] = None,
        updated_at: Optional[str] = None,
        next_retry_at: Optional[str] = None,
        error_message: Optional[str] = None,
    ):
        self.id = id
        self.command = command
        self.state = state
        self.attempts = attempts
        self.max_retries = max_retries
        self.created_at = created_at or datetime.utcnow().isoformat() + "Z"
        self.updated_at = updated_at or datetime.utcnow().isoformat() + "Z"
        self.next_retry_at = next_retry_at
        self.error_message = error_message
    
    def to_dict(self) -> dict:
        """Convert job to dictionary."""
        return {
            "id": self.id,
            "command": self.command,
            "state": self.state,
            "attempts": self.attempts,
            "max_retries": self.max_retries,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "next_retry_at": self.next_retry_at,
            "error_message": self.error_message,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "Job":
        """Create job from dictionary."""
        return cls(**data)
    
    def to_json(self) -> str:
        """Convert job to JSON string."""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_json(cls, json_str: str) -> "Job":
        """Create job from JSON string."""
        return cls.from_dict(json.loads(json_str))
    
    def __repr__(self) -> str:
        return f"Job(id={self.id}, state={self.state}, attempts={self.attempts})"
