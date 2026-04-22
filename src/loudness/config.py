"""Configuration settings via pydantic-settings."""

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_prefix="LOUDNESS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Proxies
    proxies: list[str] = Field(
        default_factory=list,
        description="Comma-separated proxy URLs",
    )
    proxies_file: Path | None = Field(
        default=None,
        description="Path to file with proxy URLs (one per line)",
    )

    # Timing
    request_delay: float = Field(
        default=1.5,
        description="Base delay between requests in seconds",
    )
    backoff_base: float = Field(
        default=1.0,
        description="Base delay for exponential backoff",
    )
    backoff_max: float = Field(
        default=60.0,
        description="Maximum backoff delay in seconds",
    )
    backoff_multiplier: float = Field(
        default=2.0,
        description="Backoff multiplier",
    )
    timeout: float = Field(
        default=15.0,
        description="Request timeout in seconds",
    )

    # Concurrency
    concurrency: int = Field(
        default=5,
        description="Number of parallel requests",
    )
    checkpoint_interval: int = Field(
        default=10,
        description="Save checkpoint every N completions",
    )

    # Retry
    max_retries: int = Field(
        default=3,
        description="Maximum retry attempts per URL",
    )
    proxy_max_failures: int = Field(
        default=3,
        description="Mark proxy dead after N consecutive failures",
    )
    proxy_cooldown: float = Field(
        default=5.0,
        description="Proxy cooldown period in seconds",
    )

    # Database
    db_path: Path = Field(
        default=Path("loudness.db"),
        description="Path to SQLite database",
    )

    # ScrapFly
    scrapfly_key: str | None = Field(
        default=None,
        description="ScrapFly API key for anti-bot bypass",
    )

    def get_proxies(self) -> list[str]:
        """Get all configured proxies from both sources."""
        proxies = list(self.proxies)

        if self.proxies_file and self.proxies_file.exists():
            with open(self.proxies_file) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        proxies.append(line)

        return proxies


# Global settings instance
settings = Settings()
