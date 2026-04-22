#!/usr/bin/env python3
"""
Deduplicate restaurants in CSV by name.
Keeps readable alias over hash alias, and row with most data.

Usage:
    uv run python scripts/dedup_csv.py sf_loudness.csv
"""

import argparse
import csv
import re
from pathlib import Path


def is_hash_alias(alias: str) -> bool:
    """Check if alias is a hash (22 char base64-ish) vs readable name."""
    return bool(re.match(r'^[A-Za-z0-9_-]{22}$', alias))


def row_score(row: dict) -> int:
    """Score a row by data completeness. Higher = better."""
    score = 0
    if not is_hash_alias(row.get("alias", "")):
        score += 100  # Prefer readable alias
    if row.get("price") and row["price"] not in ("-", ""):
        score += 10
    if row.get("rating"):
        score += 10
    if row.get("name"):
        score += 5
    return score


def dedup_csv(csv_path: Path) -> None:
    """Deduplicate CSV by name, keeping best row."""
    
    rows: list[dict] = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)
    
    print(f"Loaded {len(rows)} rows")
    
    # Group by name (lowercase for matching)
    by_name: dict[str, list[dict]] = {}
    no_name: list[dict] = []
    
    for row in rows:
        name = (row.get("name") or "").strip().lower()
        if name:
            by_name.setdefault(name, []).append(row)
        else:
            no_name.append(row)
    
    # Keep best row for each name
    deduped: list[dict] = []
    duplicates_removed = 0
    
    for name, group in by_name.items():
        if len(group) == 1:
            deduped.append(group[0])
        else:
            # Sort by score descending, pick best
            group.sort(key=row_score, reverse=True)
            deduped.append(group[0])
            duplicates_removed += len(group) - 1
            if len(group) > 2 or True:  # Show all dedup decisions
                aliases = [r["alias"][:30] for r in group]
                print(f"  {name}: kept {group[0]['alias'][:30]}, removed {len(group)-1}")
    
    # Add rows without names (can't dedup these)
    deduped.extend(no_name)
    
    print(f"\nRemoved {duplicates_removed} duplicates")
    print(f"Final: {len(deduped)} rows ({len(no_name)} without names)")
    
    # Sort by name
    deduped.sort(key=lambda r: (r.get("name") or r.get("alias") or "").lower())
    
    # Save
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(deduped)
    
    print(f"Saved to {csv_path}")


def main():
    parser = argparse.ArgumentParser(description="Deduplicate CSV by name")
    parser.add_argument("csv_file", type=Path, help="CSV file to deduplicate")
    args = parser.parse_args()
    
    if not args.csv_file.exists():
        print(f"Error: {args.csv_file} not found")
        return
    
    dedup_csv(args.csv_file)


if __name__ == "__main__":
    main()
