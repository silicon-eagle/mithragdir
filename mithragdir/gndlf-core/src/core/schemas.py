from typing import Any

from pydantic import BaseModel, ConfigDict


class PageIndex(BaseModel):
    pageid: int
    title: str
    url: str

    model_config = ConfigDict(from_attributes=True)


class Page(BaseModel):
    title: str
    url: str
    content: str
    pageid: int
    categories: list[str] = []
    images: list[str] = []
    links: list[str] = []
    external_links: list[str] = []
    sections: list[Any] = []  # JSON list
    revid: int | None = None
    displaytitle: str | None = None
    properties: list[Any] = []  # JSON list

    model_config = ConfigDict(from_attributes=True)


class Text(BaseModel):
    title: str
    url: str | None = None
    source_path: str
    content: str
    author: str | None = None
    publisher: str | None = None
    published_year: int | None = None
    isbn: str | None = None
    language: str | None = None
    file_format: str | None = None

    model_config = ConfigDict(from_attributes=True)


class Chunk(BaseModel):
    id: int | None = None
    document_id: int
    chunk_index: int
    content: str
    token_count: int
    meta_data: dict[str, Any] | None = None
    created_at: str | None = None

    model_config = ConfigDict(from_attributes=True)
