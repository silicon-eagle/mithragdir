from typing import Any, Optional
from pydantic import BaseModel, ConfigDict


class Index(BaseModel):
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
    revid: Optional[int] = None
    displaytitle: Optional[str] = None
    properties: list[Any] = []  # JSON list

    model_config = ConfigDict(from_attributes=True)


class Text(BaseModel):
    title: str
    url: Optional[str] = None
    source_path: str
    content: str
    author: Optional[str] = None
    publisher: Optional[str] = None
    published_year: Optional[int] = None
    isbn: Optional[str] = None
    language: Optional[str] = None
    file_format: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class Chunk(BaseModel):
    id: Optional[int] = None
    document_id: int
    chunk_index: int
    content: str
    token_count: int
    meta_data: Optional[dict[str, Any]] = None
    created_at: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)
