"""Job orchestrator with graceful shutdown and checkpoint management."""

import asyncio
import signal
import sys
from collections.abc import Callable
from datetime import datetime
from typing import Any

from .config import Settings
from .fetcher import AsyncFetcher
from .models import BusinessResult, JobStatus, UrlTask
from .parser import parse_business_page
from .state import StateManager


class JobOrchestrator:
    """Orchestrates scraping jobs with graceful shutdown and checkpointing."""

    def __init__(
        self,
        state: StateManager,
        fetcher: AsyncFetcher,
        settings: Settings | None = None,
        concurrency: int = 5,
        checkpoint_interval: int = 10,
        max_retries: int = 3,
        on_progress: Callable[[int, int, int], None] | None = None,
    ):
        """Initialize orchestrator.

        Args:
            state: State manager for persistence
            fetcher: Async fetcher for HTTP requests
            settings: Optional settings override
            concurrency: Number of concurrent workers
            checkpoint_interval: Save checkpoint every N completions
            max_retries: Max retry attempts per URL
            on_progress: Callback(completed, failed, total) for progress updates
        """
        self.state = state
        self.fetcher = fetcher
        self.settings = settings
        self.concurrency = concurrency
        self.checkpoint_interval = checkpoint_interval
        self.max_retries = max_retries
        self.on_progress = on_progress

        self._stopping = False
        self._paused = False
        self._completed_count = 0
        self._failed_count = 0
        self._total_count = 0
        self._current_job_id: str | None = None
        self._in_flight: set[int] = set()
        self._original_handlers: dict[int, Any] = {}

    def _setup_signal_handlers(self) -> None:
        """Set up signal handlers for graceful shutdown."""
        if sys.platform == "win32":
            return

        def handle_signal(signum: int, frame: Any) -> None:
            if self._stopping:
                print("\nForce stopping...")
                sys.exit(1)
            print("\nGracefully stopping... (press Ctrl+C again to force)")
            self._stopping = True

        for sig in (signal.SIGINT, signal.SIGTERM):
            self._original_handlers[sig] = signal.signal(sig, handle_signal)

    def _restore_signal_handlers(self) -> None:
        """Restore original signal handlers."""
        for sig, handler in self._original_handlers.items():
            signal.signal(sig, handler)
        self._original_handlers.clear()

    async def run(self, job_id: str, resume: bool = False) -> dict[str, Any]:
        """Run or resume a scraping job.

        Args:
            job_id: Job ID to run
            resume: If True, resume from checkpoint

        Returns:
            Dict with job statistics
        """
        self._current_job_id = job_id
        self._stopping = False
        self._paused = False
        self._completed_count = 0
        self._failed_count = 0

        job = await self.state.get_job(job_id)
        if not job:
            raise ValueError(f"Job not found: {job_id}")

        self._total_count = job.total_urls

        if resume:
            reset_count = await self.state.reset_in_progress(job_id)
            if reset_count > 0:
                print(f"Reset {reset_count} in-progress URLs to pending")

            checkpoint = await self.state.load_checkpoint(job_id)
            if checkpoint:
                print(f"Resuming from checkpoint saved at {checkpoint.saved_at}")

        await self.state.update_job_status(job_id, JobStatus.RUNNING)
        self._setup_signal_handlers()

        try:
            await self._process_job(job_id)
        finally:
            self._restore_signal_handlers()

        if self._stopping or self._paused:
            await self.state.update_job_status(job_id, JobStatus.PAUSED)
            await self._save_checkpoint(job_id)
            status = "paused"
        else:
            stats = await self.state.get_job_stats(job_id)
            pending = stats.get("pending", 0) + stats.get("in_progress", 0)
            failed = stats.get("failed", 0)

            if pending == 0 and failed == 0:
                await self.state.update_job_status(job_id, JobStatus.COMPLETED)
                status = "completed"
            elif pending == 0:
                await self.state.update_job_status(job_id, JobStatus.FAILED)
                status = "failed"
            else:
                await self.state.update_job_status(job_id, JobStatus.PAUSED)
                status = "paused"

        return {
            "job_id": job_id,
            "status": status,
            "completed": self._completed_count,
            "failed": self._failed_count,
            "total": self._total_count,
        }

    async def _process_job(self, job_id: str) -> None:
        """Process all pending URLs in the job."""
        semaphore = asyncio.Semaphore(self.concurrency)
        tasks: list[asyncio.Task[None]] = []

        while not self._stopping:
            pending = await self.state.get_pending_urls(
                job_id, limit=self.concurrency * 2, max_attempts=self.max_retries
            )

            if not pending:
                break

            for url_task in pending:
                if self._stopping:
                    break

                if url_task.id in self._in_flight:
                    continue

                task = asyncio.create_task(
                    self._process_url(url_task, semaphore)
                )
                tasks.append(task)

            done, pending_tasks = await asyncio.wait(
                tasks,
                timeout=1.0,
                return_when=asyncio.FIRST_COMPLETED,
            )

            tasks = [t for t in tasks if t not in done]

            for task in done:
                try:
                    await task
                except Exception as e:
                    print(f"Task error: {e}")

        if tasks:
            print(f"Waiting for {len(tasks)} in-flight requests...")
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True),
                    timeout=30.0,
                )
            except TimeoutError:
                print("Timeout waiting for in-flight requests")

    async def _process_url(
        self, url_task: UrlTask, semaphore: asyncio.Semaphore
    ) -> None:
        """Process a single URL task."""
        async with semaphore:
            if self._stopping:
                return

            self._in_flight.add(url_task.id)

            try:
                await self.state.mark_in_progress(url_task.id)

                result = await self._scrape_business(url_task.url)

                if result:
                    await self.state.mark_completed(url_task.id, result)
                    self._completed_count += 1
                else:
                    await self.state.mark_failed(
                        url_task.id,
                        "Failed to parse business data",
                    )
                    self._failed_count += 1

                if self.on_progress:
                    self.on_progress(
                        self._completed_count,
                        self._failed_count,
                        self._total_count,
                    )

                if (
                    self._completed_count + self._failed_count
                ) % self.checkpoint_interval == 0:
                    await self._save_checkpoint(self._current_job_id or "")

            except Exception as e:
                await self.state.mark_failed(url_task.id, str(e))
                self._failed_count += 1

            finally:
                self._in_flight.discard(url_task.id)

    async def _scrape_business(self, url: str) -> BusinessResult | None:
        """Scrape a single business URL.

        Args:
            url: Yelp business URL

        Returns:
            BusinessResult or None if scraping failed
        """
        fetch_result = await self.fetcher.get(url)

        if not fetch_result.success or not fetch_result.body:
            return None

        try:
            return parse_business_page(fetch_result.body, url)
        except Exception:
            return None

    async def _save_checkpoint(self, job_id: str) -> None:
        """Save current job checkpoint."""
        state_dict = {
            "completed": self._completed_count,
            "failed": self._failed_count,
            "saved_at": datetime.utcnow().isoformat(),
        }
        await self.state.save_checkpoint(job_id, state_dict)

    async def pause(self) -> None:
        """Request graceful pause of current job."""
        self._paused = True
        self._stopping = True

    def is_stopping(self) -> bool:
        """Check if orchestrator is stopping."""
        return self._stopping
