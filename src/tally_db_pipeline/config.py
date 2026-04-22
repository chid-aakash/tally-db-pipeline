from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    tally_host: str = os.getenv("TALLY_HOST", "127.0.0.1")
    tally_port: int = int(os.getenv("TALLY_PORT", "9000"))
    database_url: str = os.getenv("DATABASE_URL", "sqlite:///./data/tally_pipeline.sqlite3")
    tally_timeout_seconds: int = int(os.getenv("TALLY_TIMEOUT_SECONDS", "120"))
    tally_request_delay_ms: int = int(os.getenv("TALLY_REQUEST_DELAY_MS", "250"))
    tally_max_retries: int = int(os.getenv("TALLY_MAX_RETRIES", "2"))
    tally_retry_backoff_ms: int = int(os.getenv("TALLY_RETRY_BACKOFF_MS", "1500"))
    tally_lock_file: str = os.getenv("TALLY_LOCK_FILE", "./data/tally_http.lock")
    tally_lock_stale_seconds: int = int(os.getenv("TALLY_LOCK_STALE_SECONDS", "21600"))


def get_settings() -> Settings:
    return Settings()
