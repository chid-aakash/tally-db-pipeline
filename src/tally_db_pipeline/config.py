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


def get_settings() -> Settings:
    return Settings()
