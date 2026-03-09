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


class Chunk(BaseModel):
    id: int
    document_id: int
    chunk_index: int
    content: str
    token_count: int
    meta_data: dict[str, object]
    created_at: str | None = None

    def build_metadata_payload(self) -> dict[str, object]:
        """Build the Qdrant payload for one chunk."""
        return {
            'chunk_id': self.id,
            'document_id': self.document_id,
            'chunk_index': self.chunk_index,
            'token_count': self.token_count,
            'created_at': self.created_at,
            'meta_data': self.meta_data,
        }


class EncodedChunk(BaseModel):
    chunk_id: int
    document_id: int
    chunk_index: int
    vector: list[float]
    payload: dict[str, object]
