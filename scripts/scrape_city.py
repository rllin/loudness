#!/usr/bin/env python3
"""
Scrape all restaurants in a city for noise level data.

Idempotent: loads existing results and skips already-scraped restaurants.

Usage:
    SCRAPFLY_KEY=your_key uv run python scripts/scrape_city.py "San Francisco, CA" -o sf_loudness.csv
"""

import argparse
import asyncio
import csv
import html
import os
import re
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote

import httpx

SCRAPFLY_API_URL = "https://api.scrapfly.io/scrape"


@dataclass
class Restaurant:
    alias: str
    url: str
    name: str | None = None
    noise_level: str | None = None
    scraped_at: datetime | None = None
    error: str | None = None


@dataclass 
class CrawlStats:
    discovered: int = 0
    already_scraped: int = 0
    newly_scraped: int = 0
    with_noise: int = 0
    errors: int = 0


def load_existing_results(path: Path) -> dict[str, Restaurant]:
    """Load existing results from CSV file."""
    results: dict[str, Restaurant] = {}
    
    if not path.exists():
        return results
    
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            alias = row.get("alias", "")
            if alias:
                results[alias] = Restaurant(
                    alias=alias,
                    url=row.get("url", ""),
                    name=row.get("name") or None,
                    noise_level=row.get("noise_level") or None,
                    scraped_at=datetime.fromisoformat(row["scraped_at"]) if row.get("scraped_at") else None,
                    error=row.get("error") or None,
                )
    
    return results


def save_results(path: Path, results: dict[str, Restaurant]) -> None:
    """Save all results to CSV file."""
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "alias", "name", "noise_level", "url", "scraped_at", "error"
        ])
        writer.writeheader()
        for r in sorted(results.values(), key=lambda x: x.alias):
            writer.writerow({
                "alias": r.alias,
                "name": r.name,
                "noise_level": r.noise_level,
                "url": r.url,
                "scraped_at": r.scraped_at.isoformat() if r.scraped_at else None,
                "error": r.error,
            })


def extract_noise_level(content: str) -> str | None:
    """Extract noise level from page content."""
    patterns = [
        r'"displayText"\s*:\s*"([^"]+)"\s*,\s*"alias"\s*:\s*"NoiseLevel"',
        r'"alias"\s*:\s*"NoiseLevel"[^}]*"singleValue"\s*:\s*\{[^}]*"alias"\s*:\s*"([^"]+)"',
    ]
    for pattern in patterns:
        match = re.search(pattern, content, re.IGNORECASE)
        if match:
            value = match.group(1).lower().replace(" ", "_")
            return value
    return None


def extract_business_name(content: str) -> str | None:
    """Extract business name from page."""
    # Try og:title first
    match = re.search(r'<meta property="og:title" content="([^"]+)"', content)
    if match:
        return match.group(1).replace(" - Yelp", "").strip()
    
    # Try JSON-LD name
    match = re.search(r'"@type"\s*:\s*"Restaurant"[^}]*"name"\s*:\s*"([^"]+)"', content)
    if match:
        return match.group(1).strip()
    
    # Try <title> tag
    match = re.search(r'<title>([^<]+)</title>', content)
    if match:
        name = match.group(1).replace(" - Yelp", "").replace(" | Yelp", "").strip()
        if name and name != "Yelp":
            return name
    
    return None


async def fetch_page(
    client: httpx.AsyncClient, 
    api_key: str, 
    url: str,
    max_retries: int = 3,
) -> str | None:
    """Fetch a page via ScrapFly with retries."""
    scrapfly_url = f"{SCRAPFLY_API_URL}?key={api_key}&url={quote(url, safe='')}&asp=true"
    
    for attempt in range(max_retries):
        try:
            resp = await client.get(scrapfly_url)
            
            if resp.status_code == 429:
                # Rate limited - wait and retry
                wait_time = 2 ** attempt
                print(f"rate limited, waiting {wait_time}s...", end=" ", flush=True)
                await asyncio.sleep(wait_time)
                continue
            
            if resp.status_code != 200:
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                return None
            
            data = resp.json()
            if data.get("result", {}).get("error"):
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                return None
            
            return html.unescape(data.get("result", {}).get("content", ""))
            
        except Exception:
            if attempt < max_retries - 1:
                await asyncio.sleep(1)
                continue
            return None
    
    return None


async def get_restaurants_from_search(
    client: httpx.AsyncClient,
    api_key: str,
    location: str,
    existing_aliases: set[str],
    max_pages: int = 24,
    price_filter: str | None = None,
    sort_by: str | None = None,
) -> list[str]:
    """Get restaurant aliases from search results."""
    all_aliases: set[str] = set(existing_aliases)  # Start with existing
    new_aliases: set[str] = set()
    consecutive_empty = 0
    
    for page in range(max_pages):
        offset = page * 10
        search_url = (
            f"https://www.yelp.com/search?"
            f"find_desc=Restaurants&find_loc={quote(location)}&start={offset}"
        )
        if price_filter:
            search_url += f"&attrs=RestaurantsPriceRange2.{price_filter}"
        if sort_by:
            search_url += f"&sortby={sort_by}"
        
        print(f"  Page {page + 1} (offset {offset})...", end=" ", flush=True)
        content = await fetch_page(client, api_key, search_url)
        
        if not content:
            print("error, retrying next page")
            consecutive_empty += 1
            if consecutive_empty >= 3:
                print("  Too many consecutive errors, stopping discovery")
                break
            await asyncio.sleep(1)
            continue
        
        consecutive_empty = 0
        
        # Extract aliases from this page
        pattern = r'href="/biz/([a-zA-Z0-9\-_]+)'
        matches = re.findall(pattern, content)
        page_aliases = {a for a in matches if a and not a.startswith('yelp-') and len(a) > 3}
        
        # Count new ones (not in existing or already found)
        truly_new = page_aliases - all_aliases
        new_aliases.update(truly_new)
        all_aliases.update(page_aliases)
        
        print(f"found {len(truly_new)} new ({len(new_aliases)} total new)")
        
        # Stop if no new results for 2 consecutive pages
        if len(truly_new) == 0:
            consecutive_empty += 1
            if consecutive_empty >= 2:
                print("  No new results for 2 pages, stopping discovery")
                break
        
        await asyncio.sleep(0.5)
    
    return list(new_aliases)


async def scrape_restaurant(
    client: httpx.AsyncClient,
    api_key: str,
    alias: str,
    semaphore: asyncio.Semaphore,
) -> Restaurant:
    """Scrape a single restaurant page."""
    async with semaphore:
        url = f"https://www.yelp.com/biz/{alias}"
        restaurant = Restaurant(alias=alias, url=url)
        
        content = await fetch_page(client, api_key, url)
        
        if not content:
            restaurant.error = "Failed to fetch"
            return restaurant
        
        restaurant.name = extract_business_name(content)
        restaurant.noise_level = extract_noise_level(content)
        restaurant.scraped_at = datetime.now(UTC)
        
        return restaurant


async def scrape_city(
    api_key: str,
    location: str,
    output_path: Path,
    max_pages: int = 24,
    concurrency: int = 3,
    retry_errors: bool = False,
    premium: bool = False,
    moderate: bool = False,
    price: str | None = None,
) -> CrawlStats:
    """Scrape all restaurants in a city (idempotent)."""
    stats = CrawlStats()
    
    # Load existing results
    existing = load_existing_results(output_path)
    existing_aliases = set(existing.keys())
    stats.already_scraped = len(existing)
    
    if existing:
        print(f"Loaded {len(existing)} existing results from {output_path}")
        
        # Count existing with noise data
        existing_with_noise = sum(1 for r in existing.values() if r.noise_level)
        print(f"  {existing_with_noise} have noise data")
        
        # Find ones that errored (for retry)
        errored = [alias for alias, r in existing.items() if r.error]
        if errored:
            print(f"  {len(errored)} had errors")
    
    async with httpx.AsyncClient(timeout=90.0) as client:
        # Phase 1: Discover new restaurants from search
        print(f"\n=== Phase 1: Discovering restaurants in {location} ===\n")
        
        # Define discovery passes
        if price:
            price_labels = {"1": "$", "2": "$$", "3": "$$$", "4": "$$$$"}
            passes = [
                {"price_filter": price, "sort_by": None, "label": f"{price_labels.get(price, price)} only"},
            ]
        elif premium:
            passes = [
                {"price_filter": "4", "sort_by": None, "label": "$$$$ (Most Expensive)"},
                {"price_filter": "3", "sort_by": None, "label": "$$$ (Expensive)"},
                {"price_filter": None, "sort_by": "rating", "label": "Top Rated"},
            ]
        elif moderate:
            passes = [
                {"price_filter": "2", "sort_by": None, "label": "$$ (Moderate)"},
                {"price_filter": "2", "sort_by": "rating", "label": "$$ Top Rated"},
            ]
        else:
            passes = [
                {"price_filter": None, "sort_by": None, "label": "Default"},
            ]
        
        new_aliases: list[str] = []
        for p in passes:
            print(f"\n--- Discovering: {p['label']} ---\n")
            found = await get_restaurants_from_search(
                client, api_key, location, existing_aliases,
                max_pages=max_pages,
                price_filter=p.get("price_filter"),
                sort_by=p.get("sort_by"),
            )
            new_aliases.extend(found)
            existing_aliases.update(found)
            print(f"  Found {len(found)} new in this pass")
        
        stats.discovered = len(new_aliases)
        
        # Add aliases that need retry (had errors before)
        to_scrape = list(new_aliases)
        if retry_errors:
            errored_aliases = [alias for alias, r in existing.items() if r.error]
            to_scrape.extend(errored_aliases)
            if errored_aliases:
                print(f"\nRetrying {len(errored_aliases)} previously errored restaurants")
        
        print(f"\nDiscovered {len(new_aliases)} new restaurants")
        print(f"To scrape: {len(to_scrape)}")
        
        if not to_scrape:
            print("Nothing new to scrape!")
            # Still save to update the file
            save_results(output_path, existing)
            return stats
        
        # Phase 2: Scrape concurrently
        print(f"\n=== Phase 2: Scraping {len(to_scrape)} restaurants ===\n")
        
        semaphore = asyncio.Semaphore(concurrency)
        results = dict(existing)  # Start with existing
        completed = 0
        lock = asyncio.Lock()
        
        async def scrape_one(alias: str) -> Restaurant:
            nonlocal completed
            restaurant = await scrape_restaurant(client, api_key, alias, semaphore)
            
            async with lock:
                completed += 1
                results[alias] = restaurant
                
                # Progress output
                if restaurant.error:
                    status = f"✗ {restaurant.error}"
                elif restaurant.noise_level:
                    status = f"✓ {restaurant.noise_level}"
                else:
                    status = "○ no noise data"
                print(f"[{completed}/{len(to_scrape)}] {alias[:45]} {status}")
                
                # Periodic save (every 20 restaurants)
                if completed % 20 == 0:
                    save_results(output_path, results)
            
            return restaurant
        
        # Launch all tasks concurrently (limited by semaphore)
        scraped = await asyncio.gather(*[scrape_one(alias) for alias in to_scrape])
        
        # Update stats
        for r in scraped:
            stats.newly_scraped += 1
            if r.error:
                stats.errors += 1
            elif r.noise_level:
                stats.with_noise += 1
        
        # Final save
        save_results(output_path, results)
    
    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Scrape restaurant noise levels for a city (idempotent)"
    )
    parser.add_argument(
        "location",
        type=str,
        help="City to scrape (e.g., 'San Francisco, CA')",
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        required=True,
        help="Output CSV file (will append to existing)",
    )
    parser.add_argument(
        "--pages", "-p",
        type=int,
        default=24,
        help="Max search pages (default: 24, ~240 restaurants)",
    )
    parser.add_argument(
        "--concurrency", "-c",
        type=int,
        default=3,
        help="Concurrent requests (default: 3)",
    )
    parser.add_argument(
        "--retry-errors",
        action="store_true",
        help="Retry restaurants that previously failed",
    )
    parser.add_argument(
        "--premium",
        action="store_true",
        help="Focus on $$$/$$$$ and top-rated restaurants",
    )
    parser.add_argument(
        "--moderate",
        action="store_true",
        help="Focus on $$ moderate-price restaurants",
    )
    parser.add_argument(
        "--price",
        type=str,
        choices=["1", "2", "3", "4"],
        help="Filter by price tier: 1=$, 2=$$, 3=$$$, 4=$$$$",
    )
    parser.add_argument(
        "--key", "-k",
        type=str,
        help="ScrapFly API key (or set SCRAPFLY_KEY)",
    )
    
    args = parser.parse_args()
    
    api_key = args.key or os.environ.get("SCRAPFLY_KEY")
    if not api_key:
        print("Error: ScrapFly API key required", file=sys.stderr)
        sys.exit(1)
    
    print(f"Scraping restaurants in: {args.location}")
    print(f"Output: {args.output}")
    print(f"Max pages: {args.pages}")
    print(f"Retry errors: {args.retry_errors}")
    print(f"Premium mode: {args.premium}")
    
    stats = asyncio.run(scrape_city(
        api_key=api_key,
        location=args.location,
        output_path=args.output,
        max_pages=args.pages,
        concurrency=args.concurrency,
        retry_errors=args.retry_errors,
        premium=args.premium,
        moderate=args.moderate,
        price=args.price,
    ))
    
    print(f"\n{'='*50}")
    print("SUMMARY")
    print(f"{'='*50}")
    print(f"Already had:     {stats.already_scraped}")
    print(f"Discovered new:  {stats.discovered}")
    print(f"Newly scraped:   {stats.newly_scraped}")
    print(f"With noise data: {stats.with_noise}")
    print(f"Errors:          {stats.errors}")
    print(f"\nResults saved to: {args.output}")


if __name__ == "__main__":
    main()
