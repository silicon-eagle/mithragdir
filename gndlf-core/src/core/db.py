from __future__ import annotations

import os
from typing import Any

from loguru import logger
from peewee import Database, PeeweeException
from playhouse.db_url import connect

from core.models import (
    Chunk,
    Document,
    Text,
    WikiPage,
)
from core.models import (
    database as db_proxy,
)
from core.schemas import Page as PageSchema
from core.schemas import Text as TextSchema


class RedbookDatabase:
    def __init__(self, db_url: str | None = None) -> None:
        """Create a PostgreSQL-backed database wrapper.

        Args:
            db_url: Optional database URL. Supports postgres URLs such as
                postgresql://user:password@host:5432/dbname. If omitted,
                DATABASE_URL env var is used.
        """
        self.db_url = db_url or os.getenv('DATABASE_URL')
        if not self.db_url:
            raise ValueError('PostgreSQL DATABASE_URL is required.')
        if not self.db_url.startswith(('postgresql://', 'postgres://')):
            raise ValueError('Only postgres URLs are supported for db_url.')

        self.db: Database
        self.db = connect(self.db_url)
        logger.info(f'Using PostgreSQL database backend at {self.db_url}')

        db_proxy.initialize(self.db)

        # Create tables if they don't exist
        with self.db:
            self.db.create_tables([Document, WikiPage, Text, Chunk])

    def execute(self, query: str, params: tuple = ()) -> None:
        """Execute a single SQL statement in its own transaction.

        Args:
            query: SQL statement to execute.
            params: Positional SQL parameters.
        """
        with self.db.atomic():
            self.db.execute_sql(query, params)

    def truncate_all_tables(self) -> None:
        """Truncate all known application tables and reset identities."""
        with self.db.atomic():
            self.db.execute_sql('TRUNCATE TABLE chunks, text, wiki_page, document RESTART IDENTITY CASCADE')

    def insert_document(self, page: PageSchema) -> int:
        """Insert a wiki page into document and wiki_page tables.

        Args:
            page: Parsed page payload.

        Returns:
            Document id, or -1 on insert failure.
        """
        try:
            with self.db.atomic():
                doc = Document.create(title=page.title, url=page.url, raw_content=page.content)

                WikiPage.create(
                    document=doc,
                    page_id=page.pageid,
                    categories=page.categories,
                    images=page.images,
                    links=page.links,
                    external_links=page.external_links,
                    sections=page.sections,
                    revid=page.revid,
                    displaytitle=page.displaytitle,
                    properties=page.properties,
                )
                return doc.get_id()
        except PeeweeException as e:
            logger.error(f'Failed to insert document: {e}')
            return -1

    def insert_text(self, text: TextSchema) -> int:
        """Insert a text source into document and text tables.

        Args:
            text: Text payload with metadata.

        Returns:
            Document id, or -1 on insert failure.
        """
        source_url = text.url or text.source_path
        try:
            with self.db.atomic():
                doc = Document.create(title=text.title, url=source_url, raw_content=text.content)

                Text.create(
                    document=doc,
                    author=text.author,
                    publisher=text.publisher,
                    published_year=text.published_year,
                    isbn=text.isbn,
                    language=text.language,
                    source_path=text.source_path,
                    file_format=text.file_format,
                )
                return doc.get_id()
        except PeeweeException as e:
            logger.error(f'Failed to insert text: {e}')
            return -1

    def insert_chunk(
        self,
        document_id: int,
        chunk_index: int,
        content: str,
        token_count: int,
        meta_data: dict[str, Any] | None = None,
    ) -> int:
        """Insert one chunk row.

        Args:
            document_id: Parent document id.
            chunk_index: Sequential index inside document.
            content: Chunk text.
            token_count: Estimated token count.
            meta_data: Optional chunk metadata.

        Returns:
            Inserted chunk id, or -1 on failure.
        """
        try:
            with self.db.atomic():
                chunk = Chunk.create(
                    document_id=document_id,
                    chunk_index=chunk_index,
                    content=content,
                    token_count=token_count,
                    meta_data=meta_data,
                )
                return chunk.get_id()
        except PeeweeException as e:
            logger.error(f'Failed to insert chunk: {e}')
            return -1

    def get_chunks(self, document_id: int | None = None) -> list[Chunk]:
        """Load chunks, optionally filtered by document id.

        Args:
            document_id: Optional document id filter.

        Returns:
            List of parsed chunk models.
        """
        try:
            query = Chunk.select().order_by(Chunk.document, Chunk.chunk_index)
            if document_id is not None:
                query = query.where(Chunk.document == document_id)

            return list(query)
        except PeeweeException as e:
            logger.error(f'Failed to get chunks: {e}')
            return []

    def get_document_ids_with_chunks(self, document_id: int | None = None) -> list[int]:
        """Return all distinct document IDs that have chunks.

        Args:
            document_id: Optional document id filter.

        Returns:
            List of document ids.
        """
        try:
            query = Chunk.select(Chunk.document).distinct().order_by(Chunk.document)
            if document_id is not None:
                query = query.where(Chunk.document == document_id)
            return [chunk.document_id for chunk in query]
        except PeeweeException as e:
            logger.error(f'Failed to get document ids: {e}')
            return []

    def document_count(self) -> int:
        """Return total number of documents."""
        return Document.select().count()

    def document_exists(self, title: str) -> bool:
        """Check if a document with the given title exists."""
        return Document.select().where(Document.title == title).exists()

    def text_exists(self, source_path: str) -> bool:
        """Check if a text with the given source path exists."""
        return Text.select().where(Text.source_path == source_path).exists()

    def deploy(self) -> None:
        """Initialize the database (create tables).

        This is called explicitly in the CLI. In this implementation tables are created in __init__,
        but keeping this method for API compatibility.
        """
        pass

    def delete_all_tables(self) -> None:
        """Delete all known application tables."""
        with self.db.atomic():
            self.db.execute_sql('DROP TABLE IF EXISTS chunks CASCADE')
            self.db.execute_sql('DROP TABLE IF EXISTS text CASCADE')
            self.db.execute_sql('DROP TABLE IF EXISTS wiki_page CASCADE')
            self.db.execute_sql('DROP TABLE IF EXISTS document CASCADE')

    def close(self) -> None:
        """Close the database connection."""
        if not self.db.is_closed():
            self.db.close()
