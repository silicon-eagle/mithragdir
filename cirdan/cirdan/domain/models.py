from __future__ import annotations

from pydantic import BaseModel, Field


class DependencyCheck(BaseModel):
    name: str
    ok: bool
    detail: str | None = None


class ReadyStatus(BaseModel):
    status: str
    dependencies: list[DependencyCheck]


class ChatRequest(BaseModel):
    message: str = Field(min_length=1)
    top_k: int | None = Field(default=None, ge=1)


class RetrievedChunk(BaseModel):
    chunk_id: str
    content: str
    score: float | None = None
    source: str | None = None


class VectorHit(BaseModel):
    chunk_id: str
    score: float


class ChatChunk(BaseModel):
    type: str
    content: str | None = None
    sources: list[RetrievedChunk] | None = None
