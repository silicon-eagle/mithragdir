from pydantic import BaseModel


class Index(BaseModel):
    title: str
    pageid: int
    url: str


class Page(BaseModel):
    title: str
    pageid: int
    url: str
    content: str
    categories: list[dict[str, object]] | None = None
    images: list[str] | None = None
    links: list[dict[str, object]] | None = None
    external_links: list[str] | None = None
    sections: list[dict[str, object]] | None = None
    revid: int | str | None = None
    displaytitle: str | None = None
    properties: list[dict[str, object]] | None = None


class Text(BaseModel):
    title: str
    content: str
    author: str | None = None
    url: str | None = None
    source_path: str | None = None
    publisher: str | None = None
    published_year: int | None = None
    isbn: str | None = None
    language: str | None = None
    file_format: str | None = None
