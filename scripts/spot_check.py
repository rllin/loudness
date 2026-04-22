#!/usr/bin/env python3
"""
Spot-checker script to validate Yelp noise level data availability.

Usage:
    uv run python scripts/spot_check.py urls.txt --output results.csv

Takes a list of Yelp business URLs and checks which ones have noise level data.
"""

import argparse
import asyncio
import base64
import csv
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import httpx
from parsel import Selector

YELP_GQL_BATCH_URL = "https://www.yelp.com/gql/batch"

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
]


@dataclass
class SpotCheckResult:
    """Result of checking a single business URL."""

    alias: str
    url: str
    name: str | None = None
    enc_biz_id: str | None = None
    noise_level: str | None = None
    has_noise_level: bool = False
    error: str | None = None


def extract_alias_from_url(url: str) -> str | None:
    """Extract business alias from Yelp URL."""
    parsed = urlparse(url)
    path = parsed.path
    if path.startswith("/biz/"):
        alias = path[5:].split("?")[0].split("/")[0]
        return alias
    return None


def extract_biz_id(html: str) -> str | None:
    """Extract encrypted business ID from HTML."""
    selector = Selector(text=html)
    biz_id = selector.css('meta[name="yelp-biz-id"]::attr(content)').get()
    return biz_id


def extract_business_name(html: str) -> str | None:
    """Extract business name from HTML."""
    selector = Selector(text=html)
    name = selector.css('meta[property="og:title"]::attr(content)').get()
    if name:
        name = name.replace(" - Yelp", "").strip()
    return name


def build_gql_payload(enc_biz_id: str) -> list[dict]:
    """Build GraphQL batch payload for fetching business attributes."""
    return [
        {
            "operationName": "GetBusinessAttributes",
            "variables": {"encBizId": enc_biz_id},
            "extensions": {
                "documentId": "5d2f1e4f4c6a9d5e8c7b3a2f1d0e9c8b",  # This may need updating
            },
        }
    ]


def extract_noise_level_from_html(html: str) -> str | None:
    """Try to extract noise level directly from HTML/JSON embedded in page."""
    selector = Selector(text=html)

    for script in selector.css("script::text").getall():
        if "NoiseLevel" in script or "noise_level" in script:
            try:
                if '"alias":"NoiseLevel"' in script or '"alias": "NoiseLevel"' in script:
                    import re

                    match = re.search(
                        r'"alias"\s*:\s*"NoiseLevel".*?"singleValue"\s*:\s*\{[^}]*"alias"\s*:\s*"([^"]+)"',
                        script,
                    )
                    if match:
                        return match.group(1)

                    match = re.search(
                        r'"NoiseLevel"\s*:\s*"([^"]+)"',
                        script,
                    )
                    if match:
                        return match.group(1)
            except Exception:
                pass

    noise_section = selector.css('[aria-label*="Noise Level"]::text').get()
    if noise_section:
        return noise_section.lower().replace(" ", "_")

    return None


async def check_business(
    client: httpx.AsyncClient, url: str, delay: float = 1.0
) -> SpotCheckResult:
    """Check a single business URL for noise level data."""
    alias = extract_alias_from_url(url)
    if not alias:
        return SpotCheckResult(
            alias="unknown",
            url=url,
            error="Could not extract alias from URL",
        )

    result = SpotCheckResult(alias=alias, url=url)

    try:
        await asyncio.sleep(delay)

        response = await client.get(
            url,
            headers={
                "User-Agent": USER_AGENTS[0],
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.google.com/",
            },
            follow_redirects=True,
            timeout=15.0,
        )

        if response.status_code != 200:
            result.error = f"HTTP {response.status_code}"
            return result

        html = response.text
        result.name = extract_business_name(html)
        result.enc_biz_id = extract_biz_id(html)

        noise_level = extract_noise_level_from_html(html)
        if noise_level:
            result.noise_level = noise_level
            result.has_noise_level = True
            return result

        if not result.enc_biz_id:
            result.error = "Could not extract business ID"
            return result

    except httpx.TimeoutException:
        result.error = "Timeout"
    except httpx.HTTPError as e:
        result.error = f"HTTP error: {e}"
    except Exception as e:
        result.error = f"Error: {e}"

    return result


async def run_spot_check(
    urls: list[str],
    output_path: Path | None = None,
    delay: float = 1.5,
    concurrency: int = 3,
) -> list[SpotCheckResult]:
    """Run spot check on a list of URLs."""
    results: list[SpotCheckResult] = []

    limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
    async with httpx.AsyncClient(limits=limits) as client:
        semaphore = asyncio.Semaphore(concurrency)

        async def check_with_semaphore(url: str, index: int) -> SpotCheckResult:
            async with semaphore:
                print(f"[{index + 1}/{len(urls)}] Checking: {url[:60]}...")
                result = await check_business(client, url, delay=delay)
                status = "✓" if result.has_noise_level else "✗" if result.error else "○"
                noise = f" ({result.noise_level})" if result.noise_level else ""
                error = f" - {result.error}" if result.error else ""
                print(f"  {status} {result.name or result.alias}{noise}{error}")
                return result

        tasks = [check_with_semaphore(url, i) for i, url in enumerate(urls)]
        results = await asyncio.gather(*tasks)

    if output_path:
        write_csv(results, output_path)

    return results


def write_csv(results: list[SpotCheckResult], path: Path) -> None:
    """Write results to CSV file."""
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "alias",
                "url",
                "name",
                "has_noise_level",
                "noise_level",
                "enc_biz_id",
                "error",
            ],
        )
        writer.writeheader()
        for r in results:
            writer.writerow(
                {
                    "alias": r.alias,
                    "url": r.url,
                    "name": r.name,
                    "has_noise_level": r.has_noise_level,
                    "noise_level": r.noise_level,
                    "enc_biz_id": r.enc_biz_id,
                    "error": r.error,
                }
            )


def print_summary(results: list[SpotCheckResult]) -> None:
    """Print summary statistics."""
    total = len(results)
    with_noise = sum(1 for r in results if r.has_noise_level)
    with_error = sum(1 for r in results if r.error)
    without_noise = total - with_noise - with_error

    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    print(f"Total URLs checked:     {total}")
    print(f"With noise level:       {with_noise} ({100 * with_noise / total:.1f}%)")
    print(f"Without noise level:    {without_noise} ({100 * without_noise / total:.1f}%)")
    print(f"Errors:                 {with_error} ({100 * with_error / total:.1f}%)")

    if with_noise > 0:
        print("\nNoise level distribution:")
        noise_counts: dict[str, int] = {}
        for r in results:
            if r.noise_level:
                noise_counts[r.noise_level] = noise_counts.get(r.noise_level, 0) + 1
        for level, count in sorted(noise_counts.items()):
            print(f"  {level}: {count}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check Yelp business URLs for noise level data availability."
    )
    parser.add_argument(
        "urls_file",
        type=Path,
        help="Text file with one Yelp business URL per line",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=None,
        help="Output CSV file path",
    )
    parser.add_argument(
        "--delay",
        "-d",
        type=float,
        default=1.5,
        help="Delay between requests in seconds (default: 1.5)",
    )
    parser.add_argument(
        "--concurrency",
        "-c",
        type=int,
        default=3,
        help="Number of concurrent requests (default: 3)",
    )

    args = parser.parse_args()

    if not args.urls_file.exists():
        print(f"Error: File not found: {args.urls_file}", file=sys.stderr)
        sys.exit(1)

    urls = []
    with open(args.urls_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                urls.append(line)

    if not urls:
        print("Error: No URLs found in file", file=sys.stderr)
        sys.exit(1)

    print(f"Spot-checking {len(urls)} URLs...")
    print(f"Delay: {args.delay}s, Concurrency: {args.concurrency}")
    print()

    results = asyncio.run(
        run_spot_check(
            urls,
            output_path=args.output,
            delay=args.delay,
            concurrency=args.concurrency,
        )
    )

    print_summary(results)

    if args.output:
        print(f"\nResults written to: {args.output}")


if __name__ == "__main__":
    main()
