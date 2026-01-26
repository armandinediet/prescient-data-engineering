from __future__ import annotations
import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Settings:
    database_url: str
    openweather_api_key: str | None

def load_settings() -> Settings:
    db = os.getenv("DATABASE_URL", "").strip()
    if not db:
        raise RuntimeError("DATABASE_URL is required")
    key = os.getenv("OPENWEATHER_API_KEY", "").strip() or None
    return Settings(database_url=db, openweather_api_key=key)
