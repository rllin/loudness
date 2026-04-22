#!/usr/bin/env python3
"""
Scrape Yelp search results to get restaurant URLs for a city.

Usage:
    SCRAPFLY_KEY=your_key uv run python scripts/get_sf_restaurants.py --output sf_restaurants.txt
"""

import argparse
import asyncio
import html
import os
import re
import sys
from pathlib import Path
from urllib.parse import quote

import httpx

SCRAPFLY_API_URL = "https://api.scrapfly.io/scrape"


async def get_search_page(
    client: httpx.AsyncClient,
    api_key: str,
    location: str,
    offset: int = 0,
) -> tuple[list[str], int]:
    """Fetch a page of search results and extract business URLs.
    
    Returns:
        Tuple of (list of business aliases, total results count)
    """
    search_url = (
        f"https://www.yelp.com/search?"
        f"find_desc=Restaurants&find_loc={quote(location)}&start={offset}"
    )
    
    scrapfly_url = (
        f"{SCRAPFLY_API_URL}?"
        f"key={api_key}&url={quote(search_url, safe='')}&asp=true"
    )
    
    resp = await client.get(scrapfly_url)
    
    if resp.status_code != 200:
        print(f"  Error: HTTP {resp.status_code}")
        return [], 0
    
    data = resp.json()
    result = data.get("result", {})
    
    if result.get("error"):
        print(f"  Error: {result['error'].get('message', 'Unknown')}")
        return [], 0
    
    content = html.unescape(result.get("content", ""))
    
    # Extract business aliases from search results
    # Pattern: /biz/business-alias-city or href="/biz/..."
    aliases = set()
    
    # Find all /biz/ links
    pattern = r'href="?/biz/([a-zA-Z0-9\-]+)'
    matches = re.findall(pattern, content)
    
    for alias in matches:
        # Filter out non-business links
        if alias and not alias.startswith('yelp-') and len(alias) > 3:
            aliases.add(alias)
    
    # Try to get total count
    total = 0
    total_match = re.search(r'"totalResults"\s*:\s*(\d+)', content)
    if total_match:
        total = int(total_match.group(1))
    else:
        # Alternative pattern
        total_match = re.search(r'(\d+)\s+results', content)
        if total_match:
            total = int(total_match.group(1))
    
    return list(aliases), total


async def get_all_restaurants(
    api_key: str,
    location: str,
    max_pages: int = 10,
    output_path: Path | None = None,
) -> list[str]:
    """Get all restaurant URLs for a location."""
    all_aliases: set[str] = set()
    
    async with httpx.AsyncClient(timeout=90.0) as client:
        # First page to get total
        print(f"Fetching page 1...")
        aliases, total = await get_search_page(client, api_key, location, 0)
        all_aliases.update(aliases)
        print(f"  Found {len(aliases)} restaurants (total: {total})")
        
        # Yelp shows 10 results per page, max 240 results accessible
        max_offset = min(total, 240)
        pages_needed = min((max_offset // 10) + 1, max_pages)
        
        for page in range(1, pages_needed):
            offset = page * 10
            if offset >= max_offset:
                break
                
            print(f"Fetching page {page + 1} (offset {offset})...")
            await asyncio.sleep(1)  # Rate limit
            
            aliases, _ = await get_search_page(client, api_key, location, offset)
            new_count = len(aliases - all_aliases)
            all_aliases.update(aliases)
            print(f"  Found {new_count} new restaurants (total: {len(all_aliases)})")
    
    # Convert to full URLs
    urls = [f"https://www.yelp.com/biz/{alias}" for alias in sorted(all_aliases)]
    
    if output_path:
        with open(output_path, "w") as f:
            for url in urls:
                f.write(url + "\n")
        print(f"\nWrote {len(urls)} URLs to {output_path}")
    
    return urls


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Get Yelp restaurant URLs for a city"
    )
    parser.add_argument(
        "--location", "-l",
        type=str,
        default="San Francisco, CA",
        help="City to search (default: San Francisco, CA)",
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=Path("sf_restaurants.txt"),
        help="Output file for URLs",
    )
    parser.add_argument(
        "--pages", "-p",
        type=int,
        default=24,  # 24 pages = 240 results (Yelp's max)
        help="Max pages to fetch (default: 24, max 240 results)",
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
    
    print(f"Searching for restaurants in: {args.location}")
    print(f"Max pages: {args.pages}")
    print()
    
    asyncio.run(get_all_restaurants(
        api_key=api_key,
        location=args.location,
        max_pages=args.pages,
        output_path=args.output,
    ))


if __name__ == "__main__":
    main()
