"""Typer CLI for the Yelp loudness scraper."""

import asyncio
import csv
import json
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from .backoff import BackoffController
from .config import settings
from .fetcher import AsyncFetcher
from .models import JobStatus
from .orchestrator import JobOrchestrator
from .proxy import ProxyRotator
from .state import StateManager

app = typer.Typer(
    name="loudness",
    help="Yelp noise level scraper with state persistence and proxy rotation.",
    no_args_is_help=True,
)
console = Console()


def load_urls_from_file(path: Path) -> list[str]:
    """Load URLs from a text file."""
    urls = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                urls.append(line)
    return urls


@app.command()
def scrape(
    urls_file: Annotated[
        Path,
        typer.Argument(help="Text file with one Yelp business URL per line"),
    ],
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Output file for results (JSON or CSV)"),
    ] = None,
    concurrency: Annotated[
        int,
        typer.Option("--concurrency", "-c", help="Number of concurrent requests"),
    ] = 5,
    delay: Annotated[
        float,
        typer.Option("--delay", "-d", help="Delay between requests in seconds"),
    ] = 1.5,
    db_path: Annotated[
        Path,
        typer.Option("--db", help="Path to SQLite database"),
    ] = Path("loudness.db"),
) -> None:
    """Start a new scraping job."""
    if not urls_file.exists():
        console.print(f"[red]Error: File not found: {urls_file}[/red]")
        raise typer.Exit(1)

    urls = load_urls_from_file(urls_file)
    if not urls:
        console.print("[red]Error: No URLs found in file[/red]")
        raise typer.Exit(1)

    console.print(f"Loaded {len(urls)} URLs from {urls_file}")

    async def run_scrape() -> dict:
        async with StateManager(db_path) as state:
            job_id = await state.create_job(urls)
            console.print(f"Created job: [cyan]{job_id}[/cyan]")

            proxies = settings.get_proxies()
            proxy_rotator = ProxyRotator(proxies) if proxies else None

            if settings.scrapfly_key:
                console.print("Using ScrapFly for anti-bot bypass")
            elif proxy_rotator:
                console.print(f"Using {len(proxies)} proxies")

            backoff = BackoffController(
                base_delay=settings.backoff_base,
                max_delay=settings.backoff_max,
                multiplier=settings.backoff_multiplier,
            )

            async with AsyncFetcher(
                proxy_rotator=proxy_rotator,
                backoff=backoff,
                max_retries=settings.max_retries,
                timeout=90.0 if settings.scrapfly_key else settings.timeout,
                request_delay=delay,
                scrapfly_key=settings.scrapfly_key,
            ) as fetcher:
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    console=console,
                ) as progress:
                    task = progress.add_task("Scraping...", total=None)

                    def on_progress(completed: int, failed: int, total: int) -> None:
                        progress.update(
                            task,
                            description=f"Progress: {completed}/{total} completed, {failed} failed",
                        )

                    orchestrator = JobOrchestrator(
                        state=state,
                        fetcher=fetcher,
                        concurrency=concurrency,
                        checkpoint_interval=settings.checkpoint_interval,
                        max_retries=settings.max_retries,
                        on_progress=on_progress,
                    )

                    result = await orchestrator.run(job_id)

            if output:
                results = await state.get_results(job_id)
                await export_results(results, output)
                console.print(f"Results written to: {output}")

            return result

    result = asyncio.run(run_scrape())

    console.print()
    console.print(f"[bold]Job {result['job_id']} {result['status']}[/bold]")
    console.print(f"  Completed: {result['completed']}")
    console.print(f"  Failed: {result['failed']}")
    console.print(f"  Total: {result['total']}")

    if result["status"] == "paused":
        console.print()
        console.print(f"Resume with: [cyan]loudness resume {result['job_id']}[/cyan]")


@app.command()
def resume(
    job_id: Annotated[str, typer.Argument(help="Job ID to resume")],
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Output file for results"),
    ] = None,
    concurrency: Annotated[
        int,
        typer.Option("--concurrency", "-c", help="Number of concurrent requests"),
    ] = 5,
    db_path: Annotated[
        Path,
        typer.Option("--db", help="Path to SQLite database"),
    ] = Path("loudness.db"),
) -> None:
    """Resume a paused or failed job."""

    async def run_resume() -> dict:
        async with StateManager(db_path) as state:
            job = await state.get_job(job_id)
            if not job:
                console.print(f"[red]Error: Job not found: {job_id}[/red]")
                raise typer.Exit(1)

            console.print(f"Resuming job: [cyan]{job_id}[/cyan]")
            console.print(f"  Status: {job.status.value}")
            console.print(f"  Progress: {job.completed}/{job.total_urls}")

            proxies = settings.get_proxies()
            proxy_rotator = ProxyRotator(proxies) if proxies else None

            backoff = BackoffController(
                base_delay=settings.backoff_base,
                max_delay=settings.backoff_max,
                multiplier=settings.backoff_multiplier,
            )

            async with AsyncFetcher(
                proxy_rotator=proxy_rotator,
                backoff=backoff,
                max_retries=settings.max_retries,
                timeout=90.0 if settings.scrapfly_key else settings.timeout,
                request_delay=settings.request_delay,
                scrapfly_key=settings.scrapfly_key,
            ) as fetcher:
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    console=console,
                ) as progress:
                    task = progress.add_task("Scraping...", total=None)

                    def on_progress(completed: int, failed: int, total: int) -> None:
                        progress.update(
                            task,
                            description=f"Progress: {completed}/{total} completed, {failed} failed",
                        )

                    orchestrator = JobOrchestrator(
                        state=state,
                        fetcher=fetcher,
                        concurrency=concurrency,
                        checkpoint_interval=settings.checkpoint_interval,
                        max_retries=settings.max_retries,
                        on_progress=on_progress,
                    )

                    result = await orchestrator.run(job_id, resume=True)

            if output:
                results = await state.get_results(job_id)
                await export_results(results, output)
                console.print(f"Results written to: {output}")

            return result

    result = asyncio.run(run_resume())

    console.print()
    console.print(f"[bold]Job {result['job_id']} {result['status']}[/bold]")
    console.print(f"  Completed: {result['completed']}")
    console.print(f"  Failed: {result['failed']}")
    console.print(f"  Total: {result['total']}")


@app.command()
def status(
    job_id: Annotated[str, typer.Argument(help="Job ID to check")],
    db_path: Annotated[
        Path,
        typer.Option("--db", help="Path to SQLite database"),
    ] = Path("loudness.db"),
) -> None:
    """Check the status of a job."""

    async def get_status() -> None:
        async with StateManager(db_path) as state:
            job = await state.get_job(job_id)
            if not job:
                console.print(f"[red]Error: Job not found: {job_id}[/red]")
                raise typer.Exit(1)

            stats = await state.get_job_stats(job_id)
            checkpoint = await state.load_checkpoint(job_id)

            console.print(f"[bold]Job: {job_id}[/bold]")
            console.print(f"  Status: {job.status.value}")
            console.print(f"  Created: {job.created_at}")
            console.print()

            table = Table(title="URL Status")
            table.add_column("Status", style="cyan")
            table.add_column("Count", justify="right")

            for status_name, count in sorted(stats.items()):
                table.add_row(status_name, str(count))

            table.add_row("Total", str(job.total_urls), style="bold")
            console.print(table)

            if checkpoint:
                console.print()
                console.print(f"Last checkpoint: {checkpoint.saved_at}")

    asyncio.run(get_status())


@app.command()
def jobs(
    db_path: Annotated[
        Path,
        typer.Option("--db", help="Path to SQLite database"),
    ] = Path("loudness.db"),
) -> None:
    """List all jobs."""

    async def list_jobs() -> None:
        async with StateManager(db_path) as state:
            all_jobs = await state.get_all_jobs()

            if not all_jobs:
                console.print("No jobs found.")
                return

            table = Table(title="Jobs")
            table.add_column("ID", style="cyan")
            table.add_column("Status")
            table.add_column("Progress", justify="right")
            table.add_column("Created")

            for job in all_jobs:
                status_style = {
                    JobStatus.COMPLETED: "green",
                    JobStatus.FAILED: "red",
                    JobStatus.RUNNING: "yellow",
                    JobStatus.PAUSED: "blue",
                    JobStatus.PENDING: "dim",
                }.get(job.status, "")

                table.add_row(
                    job.id,
                    f"[{status_style}]{job.status.value}[/{status_style}]",
                    f"{job.completed}/{job.total_urls}",
                    job.created_at.strftime("%Y-%m-%d %H:%M"),
                )

            console.print(table)

    asyncio.run(list_jobs())


@app.command("export")
def export_cmd(
    job_id: Annotated[str, typer.Argument(help="Job ID to export")],
    output: Annotated[
        Path,
        typer.Option("--output", "-o", help="Output file path"),
    ] = Path("results.json"),
    format: Annotated[
        str,
        typer.Option("--format", "-f", help="Output format: json or csv"),
    ] = "json",
    db_path: Annotated[
        Path,
        typer.Option("--db", help="Path to SQLite database"),
    ] = Path("loudness.db"),
) -> None:
    """Export job results to JSON or CSV."""

    async def do_export() -> None:
        async with StateManager(db_path) as state:
            job = await state.get_job(job_id)
            if not job:
                console.print(f"[red]Error: Job not found: {job_id}[/red]")
                raise typer.Exit(1)

            results = await state.get_results(job_id)

            if not results:
                console.print("[yellow]No results to export[/yellow]")
                return

            await export_results(results, output, format)
            console.print(f"Exported {len(results)} results to: {output}")

    asyncio.run(do_export())


async def export_results(
    results: list, output: Path, format: str = "json"
) -> None:
    """Export results to file."""
    if format == "csv" or output.suffix == ".csv":
        with open(output, "w", newline="") as f:
            if results:
                fieldnames = ["alias", "name", "noise_level", "url", "enc_biz_id", "scraped_at"]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for r in results:
                    writer.writerow({
                        "alias": r.alias,
                        "name": r.name,
                        "noise_level": r.noise_level.value if r.noise_level else None,
                        "url": r.url,
                        "enc_biz_id": r.enc_biz_id,
                        "scraped_at": r.scraped_at.isoformat(),
                    })
    else:
        with open(output, "w") as f:
            data = [r.model_dump(mode="json") for r in results]
            json.dump(data, f, indent=2, default=str)


if __name__ == "__main__":
    app()
