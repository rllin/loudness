#!/usr/bin/env python3
"""
Add price and rating to restaurants from Yelp via ScrapFly.

Usage:
    SCRAPFLY_KEY=... uv run python scripts/add_details.py sf_loudness.csv
"""

import argparse
import asyncio
import csv
import os
import re
import sys
from pathlib import Path
from urllib.parse import quote

import httpx

SCRAPFLY_API_URL = "https://api.scrapfly.io/scrape"


def extract_price(content: str) -> str | None:
    """Extract price range ($-$$$$) from page."""
    # Look for price in various formats
    patterns = [
        r'display"\s*:\s*"(\$+)"',  # JSON priceRange.display format
        r'"priceRange"\s*:\s*"(\$+)"',  # Simple JSON format
        r'aria-label="[^"]*Price[^"]*"[^>]*>(\$+)<',
        r'class="[^"]*price[^"]*"[^>]*>(\$+)<',
        r'>(\${1,4})</span>',
    ]
    for pattern in patterns:
        match = re.search(pattern, content, re.IGNORECASE)
        if match:
            price = match.group(1)
            if 1 <= len(price) <= 4 and all(c == '$' for c in price):
                return price
    return None


def extract_rating(content: str) -> str | None:
    """Extract rating (e.g., 4.5) from page."""
    patterns = [
        r'"aggregateRating"[^}]*"ratingValue"\s*:\s*"?([0-9.]+)"?',
        r'"ratingValue"\s*:\s*"?([0-9.]+)"?',
        r'aria-label="([0-9.]+) star rating"',
        r'([0-9.]+)\s*star rating',
    ]
    for pattern in patterns:
        match = re.search(pattern, content, re.IGNORECASE)
        if match:
            try:
                rating = float(match.group(1))
                if 1.0 <= rating <= 5.0:
                    return str(rating)
            except ValueError:
                continue
    return None


async def fetch_details(
    client: "httpx.AsyncClient",
    api_key: str,
    alias: str,
    semaphore: asyncio.Semaphore,
    delay: float = 0.5,
) -> tuple[str, str | None, str | None, str | None]:
    """Fetch price and rating for a restaurant. Returns (alias, price, rating, error)."""
    async with semaphore:
        await asyncio.sleep(delay)  # Rate limit protection
        url = f"https://www.yelp.com/biz/{alias}"
        scrapfly_url = f"{SCRAPFLY_API_URL}?key={api_key}&url={quote(url, safe='')}&asp=true"
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                resp = await client.get(scrapfly_url)
                
                if resp.status_code == 429:
                    if attempt < max_retries - 1:
                        await asyncio.sleep(5 * (attempt + 1))  # Backoff: 5s, 10s, 15s
                        continue
                    return (alias, None, None, "rate_limited")
                
                if resp.status_code != 200:
                    return (alias, None, None, f"http_{resp.status_code}")
                
                data = resp.json()
                result = data.get("result", {})
                
                if result.get("error"):
                    return (alias, None, None, "scrapfly_error")
                
                content = result.get("content", "")
                price = extract_price(content) or "-"  # "-" = confirmed no price
                rating = extract_rating(content)
                
                return (alias, price, rating, None)
                
            except Exception as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                    continue
                return (alias, None, None, str(e)[:50])
        
        return (alias, None, None, "max_retries")


def load_csv(csv_path: Path) -> tuple[list[str], list[dict]]:
    """Load CSV and ensure price/rating columns exist."""
    rows: list[dict] = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)
    
    # Add new columns if not present
    if "price" not in fieldnames:
        idx = fieldnames.index("noise_level") + 1 if "noise_level" in fieldnames else 3
        fieldnames.insert(idx, "price")
    if "rating" not in fieldnames:
        idx = fieldnames.index("price") + 1
        fieldnames.insert(idx, "rating")
    
    # Ensure all rows have the new fields
    for row in rows:
        if "price" not in row:
            row["price"] = ""
        if "rating" not in row:
            row["rating"] = ""
    
    return fieldnames, rows


def save_csv(csv_path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    """Save CSV."""
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


async def add_details(csv_path: Path, api_key: str, concurrency: int = 3) -> None:
    """Add price and rating to restaurants in CSV. Idempotent with progress saves."""
    
    fieldnames, rows = load_csv(csv_path)
    
    if not fieldnames:
        print("Error: Could not read CSV headers")
        return
    
    # Build index for quick lookup
    alias_to_idx = {row["alias"]: i for i, row in enumerate(rows)}
    
    # Find rows missing price or rating (and not errored)
    # Skip if price is "-" (confirmed no price) or has actual value
    to_fetch: list[str] = []
    for row in rows:
        if row.get("error"):
            continue
        price_done = row.get("price") in ("-", "$", "$$", "$$$", "$$$$")
        rating_done = bool(row.get("rating"))
        if not price_done or not rating_done:
            to_fetch.append(row["alias"])
    
    total = len(to_fetch)
    print(f"Found {total} restaurants needing price/rating data")
    
    if not to_fetch:
        print("Nothing to fetch!")
        return
    
    # Fetch with progress tracking
    semaphore = asyncio.Semaphore(concurrency)
    completed = 0
    fetched = 0
    failed = 0
    lock = asyncio.Lock()
    
    async def fetch_one(alias: str) -> None:
        nonlocal completed, fetched, failed
        
        result = await fetch_details(client, api_key, alias, semaphore)
        alias, price, rating, error = result
        
        async with lock:
            completed += 1
            idx = alias_to_idx[alias]
            
            if error:
                failed += 1
                status = f"x {error}"
            else:
                if price:
                    rows[idx]["price"] = price
                if rating:
                    rows[idx]["rating"] = rating
                if price or rating:
                    fetched += 1
                status = f"{price or '-':4} {rating or '-'}"
            
            # Progress
            print(f"[{completed}/{total}] {alias[:40]} {status}", flush=True)
            
            # Save every 25 restaurants
            if completed % 25 == 0:
                save_csv(csv_path, fieldnames, rows)
                print(f"  ... saved progress ({completed}/{total})")
    
    async with httpx.AsyncClient(timeout=90.0) as client:
        print(f"\nFetching details via ScrapFly (concurrency={concurrency})...\n")
        await asyncio.gather(*[fetch_one(alias) for alias in to_fetch])
    
    # Final save
    save_csv(csv_path, fieldnames, rows)
    
    print(f"\n{'='*50}")
    print(f"Done! Fetched: {fetched}, Failed: {failed}")
    print(f"Updated {csv_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Add price and rating from Yelp to CSV"
    )
    parser.add_argument(
        "csv_path",
        type=Path,
        help="Path to CSV file with Yelp data",
    )
    parser.add_argument(
        "--concurrency", "-c",
        type=int,
        default=5,
        help="Number of concurrent requests (default: 5)",
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
    
    if not args.csv_path.exists():
        print(f"Error: {args.csv_path} not found")
        return
    
    asyncio.run(add_details(args.csv_path, api_key, args.concurrency))


if __name__ == "__main__":
    main()
