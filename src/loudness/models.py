"""Pydantic models for the Yelp loudness scraper."""

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class NoiseLevel(StrEnum):
    """Yelp noise level values."""

    QUIET = "quiet"
    AVERAGE = "average"
    LOUD = "loud"
    VERY_LOUD = "very_loud"


class JobStatus(StrEnum):
    """Status of a scraping job."""

    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


class UrlStatus(StrEnum):
    """Status of a URL task within a job."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class ProxyStatus(StrEnum):
    """Health status of a proxy."""

    HEALTHY = "healthy"
    SLOW = "slow"
    BLOCKED = "blocked"
    DEAD = "dead"


class BusinessResult(BaseModel):
    """Result data for a scraped business."""

    alias: str = Field(..., description="URL slug - primary lookup key")
    enc_biz_id: str | None = Field(None, description="Internal ID used for GraphQL")
    name: str | None = Field(None, description="Business name")
    noise_level: NoiseLevel | None = Field(None, description="Noise level attribute")
    url: str = Field(..., description="Full Yelp URL")
    scraped_at: datetime = Field(default_factory=datetime.utcnow)

    def yelp_url(self) -> str:
        """Reconstruct Yelp URL from alias."""
        return f"https://www.yelp.com/biz/{self.alias}"


class FetchResult(BaseModel):
    """Result of an HTTP fetch operation."""

    success: bool
    status_code: int | None = None
    body: str | None = None
    error: str | None = None
    attempts: int = 1
    total_time_ms: float = 0.0
    proxy_used: str | None = None


class UrlTask(BaseModel):
    """A URL task to be processed."""

    id: int
    job_id: str
    alias: str
    url: str
    status: UrlStatus = UrlStatus.PENDING
    attempts: int = 0
    last_error: str | None = None
    result: BusinessResult | None = None
    updated_at: datetime | None = None


class Job(BaseModel):
    """A scraping job."""

    id: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    status: JobStatus = JobStatus.PENDING
    total_urls: int = 0
    completed: int = 0
    failed: int = 0


class Checkpoint(BaseModel):
    """Checkpoint for resuming a job."""

    job_id: str
    last_url_id: int | None = None
    state: dict[str, Any] = Field(default_factory=dict)
    saved_at: datetime = Field(default_factory=datetime.utcnow)


class ProxyHealth(BaseModel):
    """Health tracking for a proxy."""

    url: str
    status: ProxyStatus = ProxyStatus.HEALTHY
    success_count: int = 0
    fail_count: int = 0
    last_used: datetime | None = None
    avg_latency_ms: float = 0.0
    consecutive_failures: int = 0
    cooldown_until: datetime | None = None

    def is_available(self) -> bool:
        """Check if proxy is available for use."""
        if self.status == ProxyStatus.DEAD:
            return False
        if self.cooldown_until and datetime.utcnow() < self.cooldown_until:
            return False
        return True

    def success_rate(self) -> float:
        """Calculate success rate."""
        total = self.success_count + self.fail_count
        if total == 0:
            return 1.0
        return self.success_count / total
