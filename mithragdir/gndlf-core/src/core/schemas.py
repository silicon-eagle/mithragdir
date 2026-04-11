from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict

from core.models import Document as PeeweeDocument


class Document(BaseModel):
    document_id: int
    title: str
    url: str | None = None
    raw_content: str
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)

    @classmethod
    def from_peewee(cls, document: PeeweeDocument) -> 'Document':
        data = document.__data__
        title_value = data.get('title')
        url_value = data.get('url')
        raw_content_value = data.get('raw_content')
        created_at_value = data.get('created_at')

        if not isinstance(created_at_value, datetime):
            raise ValueError('Document.created_at must be a datetime')

        return cls(
            document_id=int(document.get_id()),
            title=str(title_value),
            url=None if url_value is None else str(url_value),
            raw_content=str(raw_content_value),
            created_at=created_at_value,
        )


class CrawlIndex(BaseModel):
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
