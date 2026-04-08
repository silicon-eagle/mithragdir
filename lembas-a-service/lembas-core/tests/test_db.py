from __future__ import annotations
import os

import pytest
from lembas_core.db import RedbookDatabase
from lembas_core.models import Chunk, Index, WikiPage, Text, Document
from lembas_core.schemas import (
    Index as IndexSchema,
    Page as PageSchema,
    Text as TextSchema,
)


@pytest.fixture
def db() -> RedbookDatabase:
    db_url = os.getenv("DEV_DATABASE_URL")
    if not db_url:
        pytest.skip("DEV_DATABASE_URL is required for PostgreSQL-backed tests.")

    database = RedbookDatabase(db_url=db_url)
    database.execute(
        'TRUNCATE TABLE chunks, text, wiki_page, "index", document RESTART IDENTITY CASCADE'
    )
    return database


class TestRedbookDatabase:
    def test_insert_index(self, db: RedbookDatabase) -> None:
        index_id = db.insert_index(
            IndexSchema(title="Title", pageid=123, url="http://example.com")
        )
        assert isinstance(index_id, int)
        assert index_id == 123

    def test_insert_indexes_batch(self, db: RedbookDatabase) -> None:
        inserted = db.insert_indexes(
            [
                IndexSchema(title="Doc 1", pageid=1, url="http://a.com"),
                IndexSchema(title="Doc 2", pageid=2, url="http://b.com"),
                IndexSchema(title="Doc 1 duplicate", pageid=1, url="http://a-dup.com"),
            ]
        )
        assert inserted == 2

        rows = list(Index.select().order_by(Index.page_id))
        assert len(rows) == 2
        assert rows[0].page_id == 1
        assert rows[1].page_id == 2

    def test_insert_indexes_is_idempotent(self, db: RedbookDatabase) -> None:
        first = [
            IndexSchema(title="Doc 1", pageid=1, url="http://a.com"),
            IndexSchema(title="Doc 2", pageid=2, url="http://b.com"),
        ]
        inserted_first = db.insert_indexes(first)
        assert inserted_first == 2

        second = [
            IndexSchema(
                title="Doc 1 duplicate pageid", pageid=1, url="http://new-a.com"
            ),
            IndexSchema(title="Doc 2 duplicate url", pageid=3, url="http://b.com"),
            IndexSchema(title="Doc 3", pageid=4, url="http://d.com"),
        ]
        inserted_second = db.insert_indexes(second)
        assert inserted_second == 1

        count = Index.select().count()
        assert count == 3

    def test_insert_document(self, db: RedbookDatabase) -> None:
        doc_id = db.insert_document(
            PageSchema(
                title="Title", pageid=123, url="http://example.com", content="content"
            )
        )
        assert isinstance(doc_id, int)
        assert doc_id > 0

    def test_insert_document_returns_unique_ids(self, db: RedbookDatabase) -> None:
        id1 = db.insert_document(
            PageSchema(title="Doc 1", pageid=1, url="http://a.com", content="content a")
        )
        id2 = db.insert_document(
            PageSchema(title="Doc 2", pageid=2, url="http://b.com", content="content b")
        )
        assert id1 != id2
        assert id1 > 0
        assert id2 > 0

    def test_insert_chunk(self, db: RedbookDatabase) -> None:
        doc_id = db.insert_document(
            PageSchema(
                title="Title", pageid=123, url="http://example.com", content="content"
            )
        )
        chunk_id = db.insert_chunk(doc_id, 0, "chunk text", 10, {"source": "unit-test"})
        assert isinstance(chunk_id, int)
        assert chunk_id > 0

        # Verify metadata stored correctly using Peewee model
        chunk = Chunk.get_by_id(chunk_id)
        # JSONField automatically deserializes
        assert chunk.meta_data == {"source": "unit-test"}

    def test_insert_multiple_chunks(self, db: RedbookDatabase) -> None:
        doc_id = db.insert_document(
            PageSchema(
                title="Title", pageid=123, url="http://example.com", content="content"
            )
        )
        chunk_ids = [db.insert_chunk(doc_id, i, f"chunk {i}", i + 1) for i in range(3)]
        assert len(set(chunk_ids)) == 3

    def test_get_chunks_returns_chunk_objects_with_parsed_metadata(
        self, db: RedbookDatabase
    ) -> None:
        doc_id = db.insert_document(
            PageSchema(
                title="Title", pageid=123, url="http://example.com", content="content"
            )
        )
        db.insert_chunk(
            doc_id, 0, "first chunk", 2, {"source": "unit-test", "position": 0}
        )
        db.insert_chunk(
            doc_id, 1, "second chunk", 2, {"source": "unit-test", "position": 1}
        )

        chunks = db.get_chunks(document_id=doc_id)

        assert len(chunks) == 2
        assert all(isinstance(chunk, Chunk) for chunk in chunks)
        assert chunks[0].chunk_index == 0
        assert chunks[1].chunk_index == 1
        assert chunks[0].meta_data == {"source": "unit-test", "position": 0}
        assert isinstance(chunks[0].meta_data, dict)

    def test_document_exists(self, db: RedbookDatabase) -> None:
        db.insert_document(
            PageSchema(
                title="Title", pageid=123, url="http://example.com", content="content"
            )
        )
        assert db.document_exists("Title")
        assert not db.document_exists("Missing Title")

    def test_insert_document_stores_wiki_metadata(self, db: RedbookDatabase) -> None:
        page = PageSchema(
            title="Metadata Title",
            pageid=456,
            url="http://example.com/meta",
            content="metadata content",
            categories=["Category:Test"],
            images=["img.png"],
            links=["Other"],
            external_links=["https://example.org"],
            sections=[{"index": "1", "line": "Intro"}],
            revid=42,
            displaytitle="Metadata Title",
            properties=[{"name": "foo", "*": "bar"}],
        )
        document_id = db.insert_document(page)

        wiki_page = WikiPage.get(WikiPage.document_id == document_id)
        assert wiki_page.page_id == 456
        assert wiki_page.categories == ["Category:Test"]
        # ... verify other fields if needed

    def test_insert_text_creates_document_and_text(self, db: RedbookDatabase) -> None:
        book = TextSchema(
            title="The Hobbit",
            content="In a hole in the ground there lived a hobbit.",
            author="J.R.R. Tolkien",
            publisher="Allen & Unwin",
            published_year=1937,
            isbn="9780000000001",
            language="en",
            source_path="/tmp/the_hobbit.pdf",
            file_format="pdf",
        )
        document_id = db.insert_text(book)

        doc = Document.get_by_id(document_id)
        assert doc.title == "The Hobbit"

        text = Text.get(Text.document == doc)
        assert text.author == "J.R.R. Tolkien"
        assert text.published_year == 1937

    def test_text_exists(self, db: RedbookDatabase) -> None:
        source_path = "/tmp/fellowship.epub"
        db.insert_text(
            TextSchema(
                title="Fellowship",
                content="One Ring",
                author="J.R.R. Tolkien",
                source_path=source_path,
                file_format="epub",
            )
        )

        assert db.text_exists(source_path)
        assert not db.text_exists("/tmp/missing.pdf")

    def test_requires_postgres_database_url(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("DATABASE_URL", raising=False)
        with pytest.raises(ValueError, match="DATABASE_URL"):
            RedbookDatabase()
