#!/usr/bin/env python3
"""
Resolve hash-style Yelp aliases to readable names via ScrapFly.

Usage:
    SCRAPFLY_KEY=... uv run python scripts/fix_names.py sf_loudness.csv
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


def is_hash_alias(alias: str) -> bool:
    """Check if alias looks like encoded ID vs readable name."""
    if len(alias) < 15:
        return False
    # Hash aliases are base64-ish with no hyphens in the middle
    # Readable aliases like "garaje-san-francisco" have multiple hyphens
    hyphen_count = alias.count('-')
    # Hash aliases have 0-1 hyphens (sometimes at start), readable have 2+
    if hyphen_count >= 2:
        return False
    # Check if it's mostly alphanumeric with underscores
    return bool(re.match(r'^[A-Za-z0-9_-]+$', alias))


def alias_to_name(alias: str) -> str:
    """Convert alias to readable name, removing city suffix."""
    # garaje-san-francisco -> Garaje
    # state-bird-provisions-san-francisco -> State Bird Provisions
    parts = alias.split('-')
    
    # Common city suffixes to remove
    city_markers = {'san', 'los', 'new', 'south', 'north', 'east', 'west'}
    
    # Find where city name starts (heuristic: common city word)
    cut_idx = len(parts)
    for i, part in enumerate(parts):
        if part.lower() in city_markers and i > 0:
            cut_idx = i
            break
    
    name_parts = parts[:cut_idx]
    if not name_parts:
        name_parts = parts  # Fallback to full alias
    
    return ' '.join(p.title() for p in name_parts)


async def resolve_alias(
    client: httpx.AsyncClient,
    api_key: str,
    hash_alias: str,
    semaphore: asyncio.Semaphore,
) -> tuple[str, str | None, str | None]:
    """Fetch page via ScrapFly to get name. Returns (hash_alias, name, error)."""
    async with semaphore:
        url = f"https://www.yelp.com/biz/{hash_alias}"
        scrapfly_url = f"{SCRAPFLY_API_URL}?key={api_key}&url={quote(url, safe='')}&asp=true"
        
        try:
            resp = await client.get(scrapfly_url)
            
            if resp.status_code == 429:
                return (hash_alias, None, "rate_limited")
            
            if resp.status_code != 200:
                return (hash_alias, None, f"http_{resp.status_code}")
            
            data = resp.json()
            result = data.get("result", {})
            
            if result.get("error"):
                return (hash_alias, None, "scrapfly_error")
            
            # Check final URL for redirect
            final_url = result.get("url", "")
            match = re.search(r'/biz/([^?]+)', final_url)
            if match:
                resolved_alias = match.group(1)
                if resolved_alias != hash_alias:
                    return (hash_alias, alias_to_name(resolved_alias), None)
            
            # Try to extract name from content
            content = result.get("content", "")
            
            # Try og:title
            match = re.search(r'<meta property="og:title" content="([^"]+)"', content)
            if match:
                name = match.group(1).replace(" - Yelp", "").strip()
                if name:
                    return (hash_alias, name, None)
            
            # Try <title>
            match = re.search(r'<title>([^<]+)</title>', content)
            if match:
                name = match.group(1).replace(" - Yelp", "").replace(" | Yelp", "").strip()
                if name and name != "Yelp":
                    return (hash_alias, name, None)
            
            return (hash_alias, None, "no_name_found")
            
        except Exception as e:
            return (hash_alias, None, str(e)[:50])


async def fix_names(csv_path: Path, api_key: str, concurrency: int = 5) -> None:
    """Fix hash names in CSV via ScrapFly."""
    
    # Load CSV
    rows: list[dict] = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)
    
    if not fieldnames:
        print("Error: Could not read CSV headers")
        return
    
    # Find rows with hash aliases and no name
    to_resolve: list[tuple[int, str]] = []
    for i, row in enumerate(rows):
        alias = row.get("alias", "")
        name = row.get("name", "")
        if alias and not name and is_hash_alias(alias):
            to_resolve.append((i, alias))
    
    print(f"Found {len(to_resolve)} hash aliases to resolve")
    
    if not to_resolve:
        print("Nothing to fix!")
        return
    
    # Resolve in parallel
    semaphore = asyncio.Semaphore(concurrency)
    resolved_count = 0
    failed_count = 0
    
    async with httpx.AsyncClient(timeout=90.0) as client:
        tasks = [resolve_alias(client, api_key, alias, semaphore) for _, alias in to_resolve]
        
        print(f"\nResolving aliases via ScrapFly (concurrency={concurrency})...\n")
        
        results = await asyncio.gather(*tasks)
        
        # Build lookup of resolved names
        resolved_map: dict[str, str] = {}
        for hash_alias, name, error in results:
            if name:
                resolved_map[hash_alias] = name
                resolved_count += 1
            else:
                failed_count += 1
                if error:
                    print(f"  Failed: {hash_alias[:25]} ({error})")
    
    print(f"\nResolved: {resolved_count}, Failed: {failed_count}")
    
    # Update rows with names
    updated = 0
    for i, alias in to_resolve:
        if alias in resolved_map:
            rows[i]["name"] = resolved_map[alias]
            updated += 1
    
    # Save CSV
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"\nUpdated {updated} names in {csv_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Fix hash-style Yelp aliases via ScrapFly"
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
    
    asyncio.run(fix_names(args.csv_path, api_key, args.concurrency))


if __name__ == "__main__":
    main()
