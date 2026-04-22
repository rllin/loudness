# Loudness

Resilient Yelp noise level scraper with state persistence, proxy rotation, and crash recovery.

## Features

- **State Persistence**: SQLite-backed job tracking with checkpoint/resume support
- **Proxy Rotation**: Health tracking with round-robin, weighted, or cooldown strategies
- **Graceful Shutdown**: SIGINT/SIGTERM handlers save state before exit
- **Exponential Backoff**: Jittered retry logic for rate limiting (HTTP 429) and transient errors
- **CLI**: Full-featured command-line interface for job management

## Installation

```bash
uv sync
```

## Quick Start

### 1. Validate Data Availability (Recommended)

Before scraping at scale, check how many restaurants in your target area have noise level data:

```bash
# Create a file with Yelp business URLs (one per line)
cat > urls.txt << 'EOF'
https://www.yelp.com/biz/house-of-prime-rib-san-francisco
https://www.yelp.com/biz/tartine-bakery-san-francisco
https://www.yelp.com/biz/zuni-cafe-san-francisco
EOF

# Run spot-checker
uv run python scripts/spot_check.py urls.txt --output results.csv
```

### 2. Scrape Businesses

```bash
# Start a new scraping job
uv run loudness scrape urls.txt --output results.json

# With custom settings
uv run loudness scrape urls.txt -c 3 -d 2.0 -o results.csv
```

### 3. Resume Interrupted Jobs

```bash
# List all jobs
uv run loudness jobs

# Check job status
uv run loudness status <job_id>

# Resume a paused job
uv run loudness resume <job_id>
```

### 4. Export Results

```bash
uv run loudness export <job_id> -o results.json
uv run loudness export <job_id> -o results.csv --format csv
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `loudness scrape <urls_file>` | Start new scraping job |
| `loudness resume <job_id>` | Resume paused/failed job |
| `loudness status <job_id>` | Check job status and stats |
| `loudness jobs` | List all jobs |
| `loudness export <job_id>` | Export results to JSON/CSV |

## Anti-Bot Protection

Yelp uses Datadome for bot protection. Direct requests from datacenter IPs will be blocked with HTTP 403. To use this scraper, you need one of:

1. **Residential Proxies**: Use a proxy service with residential IPs (e.g., Bright Data, Oxylabs)
2. **Proxy Services with Anti-Bot Bypass**: Services like ScrapFly or ZenRows that handle CAPTCHA challenges
3. **Browser Automation**: For small-scale scraping, Playwright with stealth plugins may work

Configure proxies via environment variables (see Configuration below).

## Configuration

Set environment variables or create a `.env` file:

```bash
# Proxies (comma-separated or file path)
LOUDNESS_PROXIES=http://user:pass@proxy1:8080,http://user:pass@proxy2:8080
LOUDNESS_PROXIES_FILE=proxies.txt

# Timing
LOUDNESS_REQUEST_DELAY=1.5        # Base delay between requests (seconds)
LOUDNESS_BACKOFF_MAX=60.0         # Max backoff delay (seconds)
LOUDNESS_TIMEOUT=15.0             # Request timeout (seconds)

# Concurrency
LOUDNESS_CONCURRENCY=5            # Parallel requests
LOUDNESS_CHECKPOINT_INTERVAL=10   # Save checkpoint every N completions

# Retry
LOUDNESS_MAX_RETRIES=3            # Max retry attempts per URL
LOUDNESS_PROXY_MAX_FAILURES=3     # Mark proxy dead after N consecutive failures
```

## Output Format

Results include the business alias (URL slug) for easy Yelp URL reconstruction:

```json
{
  "alias": "house-of-prime-rib-san-francisco",
  "name": "House of Prime Rib",
  "noise_level": "loud",
  "url": "https://www.yelp.com/biz/house-of-prime-rib-san-francisco",
  "enc_biz_id": "abc123...",
  "scraped_at": "2024-01-15T10:30:00"
}
```

Noise level values: `quiet`, `average`, `loud`, `very_loud`

## How It Works

1. **Fetch HTML**: GET the Yelp business page
2. **Extract IDs**: Parse `encBizId` from `<meta name="yelp-biz-id">`
3. **Parse Attributes**: Extract `NoiseLevel` from embedded JSON/scripts
4. **Persist Results**: Store in SQLite with checkpoint support

The scraper extracts noise level directly from the HTML page content, avoiding the need for additional GraphQL requests in most cases.

## Error Handling

| Error | Action |
|-------|--------|
| HTTP 200 | Success, reset backoff |
| HTTP 429 | Retry with 2x backoff multiplier |
| HTTP 403 | Mark proxy as blocked, rotate |
| HTTP 5xx | Retry with standard backoff |
| Timeout | Retry, mark proxy as slow |
| Parse error | Skip URL, log error |

## Legal Notice

Yelp prohibits scraping in their Terms of Service. This tool is for educational and personal use only. For production use, consider the [Yelp Fusion API](https://docs.developer.yelp.com/) with a Premium plan that includes noise level attributes.

## License

MIT
