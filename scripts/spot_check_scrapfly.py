#!/usr/bin/env python3
"""
Spot-checker using ScrapFly API for Yelp noise level data.

Usage:
    SCRAPFLY_KEY=your_key uv run python scripts/spot_check_scrapfly.py urls.txt
"""

import argparse
import asyncio
import csv
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote, urlparse

import httpx
from parsel import Selector


@dataclass
class SpotCheckResult:
    alias: str
    url: str
    name: str | None = None
    noise_level: str | None = None
    has_noise_level: bool = False
    error: str | None = None


def extract_alias_from_url(url: str) -> str | None:
    parsed = urlparse(url)
    path = parsed.path
    if path.startswith("/biz/"):
        return path[5:].split("?")[0].split("/")[0]
    return None


def extract_business_name(html: str) -> str | None:
    selector = Selector(text=html)
    name = selector.css('meta[property="og:title"]::attr(content)').get()
    if name:
        return name.replace(" - Yelp", "").strip()
    return None


def extract_noise_level(html: str) -> str | None:
    # Decode HTML entities first
    import html as html_module
    decoded = html_module.unescape(html)
    
    patterns = [
        # Format: "displayText":"Loud","alias":"NoiseLevel"
        r'"displayText"\s*:\s*"([^"]+)"\s*,\s*"alias"\s*:\s*"NoiseLevel"',
        # Format: "alias":"NoiseLevel"..."singleValue":{"alias":"loud"}
        r'"alias"\s*:\s*"NoiseLevel"[^}]*"singleValue"\s*:\s*\{[^}]*"alias"\s*:\s*"([^"]+)"',
        # Direct formats
        r'"NoiseLevel"\s*:\s*"([^"]+)"',
        r'"noiseLevel"\s*:\s*"([^"]+)"',
        r'"noise_level"\s*:\s*"([^"]+)"',
    ]
    for pattern in patterns:
        match = re.search(pattern, decoded, re.IGNORECASE)
        if match:
            value = match.group(1).lower().replace(" ", "_")
            # Normalize values
            mappings = {
                "quiet": "quiet",
                "average": "average", 
                "moderate": "average",
                "loud": "loud",
                "very_loud": "very_loud",
                "veryloud": "very_loud",
            }
            return mappings.get(value, value)
    return None


async def check_business(
    client: httpx.AsyncClient, url: str, api_key: str
) -> SpotCheckResult:
    alias = extract_alias_from_url(url)
    if not alias:
        return SpotCheckResult(alias="unknown", url=url, error="Invalid URL")

    result = SpotCheckResult(alias=alias, url=url)

    try:
        scrapfly_url = (
            f"https://api.scrapfly.io/scrape?"
            f"key={api_key}&url={quote(url, safe='')}&asp=true"
        )

        resp = await client.get(scrapfly_url)

        if resp.status_code != 200:
            result.error = f"ScrapFly HTTP {resp.status_code}"
            return result

        data = resp.json()
        
        if data.get("result", {}).get("error"):
            result.error = data["result"]["error"].get("message", "Unknown error")
            return result

        content = data.get("result", {}).get("content", "")
        if not content:
            result.error = "Empty content"
            return result

        result.name = extract_business_name(content)
        noise = extract_noise_level(content)
        
        if noise:
            result.noise_level = noise
            result.has_noise_level = True

    except Exception as e:
        result.error = str(e)

    return result


async def run_spot_check(
    urls: list[str], api_key: str, output_path: Path | None = None
) -> list[SpotCheckResult]:
    results: list[SpotCheckResult] = []

    async with httpx.AsyncClient(timeout=90.0) as client:
        for i, url in enumerate(urls):
            print(f"[{i + 1}/{len(urls)}] Checking: {url[:60]}...")
            result = await check_business(client, url, api_key)
            
            status = "✓" if result.has_noise_level else "✗" if result.error else "○"
            noise = f" ({result.noise_level})" if result.noise_level else ""
            error = f" - {result.error}" if result.error else ""
            print(f"  {status} {result.name or result.alias}{noise}{error}")
            
            results.append(result)
            
            # Brief delay between requests
            if i < len(urls) - 1:
                await asyncio.sleep(0.5)

    if output_path:
        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=["alias", "url", "name", "has_noise_level", "noise_level", "error"]
            )
            writer.writeheader()
            for r in results:
                writer.writerow({
                    "alias": r.alias,
                    "url": r.url,
                    "name": r.name,
                    "has_noise_level": r.has_noise_level,
                    "noise_level": r.noise_level,
                    "error": r.error,
                })

    return results


def print_summary(results: list[SpotCheckResult]) -> None:
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
    parser = argparse.ArgumentParser(description="Check Yelp URLs for noise level via ScrapFly")
    parser.add_argument("urls_file", type=Path, help="Text file with Yelp URLs")
    parser.add_argument("--output", "-o", type=Path, help="Output CSV file")
    parser.add_argument("--key", "-k", type=str, help="ScrapFly API key (or set SCRAPFLY_KEY)")

    args = parser.parse_args()

    api_key = args.key or os.environ.get("SCRAPFLY_KEY")
    if not api_key:
        print("Error: ScrapFly API key required. Set SCRAPFLY_KEY or use --key", file=sys.stderr)
        sys.exit(1)

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
        print("Error: No URLs found", file=sys.stderr)
        sys.exit(1)

    print(f"Checking {len(urls)} URLs via ScrapFly...\n")
    results = asyncio.run(run_spot_check(urls, api_key, args.output))
    print_summary(results)

    if args.output:
        print(f"\nResults written to: {args.output}")


if __name__ == "__main__":
    main()
