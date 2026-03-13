from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    sqlite_connection_string: str = Field(default='sqlite:///database/redbook.db')
    qdrant_url: str = Field(default='http://localhost:6333')
    qdrant_collection: str = Field(default='gwaihir_chunks')

    llm_provider: str = Field(default='ollama')
    llm_api_key: str | None = Field(default=None)
    llm_model: str = Field(default='gpt-4o-mini')
    embedding_model: str = Field(default='text-embedding-3-large')
    top_k: int = Field(default=5, ge=1)

    cors_allowed_origins: list[str] = Field(default_factory=lambda: ['*'])


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
