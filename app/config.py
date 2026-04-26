from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import List

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    SQLITE_PATH: str = "./data/indicadores.db"
    API_KEYS: str = "dev-key-12345"
    CORS_ORIGINS: str = "http://localhost:3000,http://localhost:3002"

    @property
    def api_keys_list(self) -> List[str]:
        return [k.strip() for k in self.API_KEYS.split(",") if k.strip()]

    @property
    def cors_origins_list(self) -> List[str]:
        return [o.strip() for o in self.CORS_ORIGINS.split(",") if o.strip()]

    @property
    def sqlite_abs_path(self) -> Path:
        p = Path(self.SQLITE_PATH)
        if not p.is_absolute():
            p = (Path.cwd() / p).resolve()
        return p


@lru_cache
def get_settings() -> Settings:
    return Settings()
