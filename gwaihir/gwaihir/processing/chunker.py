from __future__ import annotations

from collections.abc import Sequence
from enum import StrEnum
from typing import Any

from langchain_text_splitters import HTMLHeaderTextSplitter, RecursiveCharacterTextSplitter
from loguru import logger

from gwaihir.db.db import RedbookDatabase


class ContentType(StrEnum):
    TEXT = 'text'
    HTML = 'html'


class Chunker:
    def __init__(
        self,
        db: RedbookDatabase,
        chunk_size: int = 1_000,
        chunk_overlap: int = 200,
        html_headers_to_split_on: Sequence[tuple[str, str]] | None = None,
    ) -> None:
        """Configure text and HTML chunking strategies.

        Args:
            db: Database used to persist chunks.
            chunk_size: Maximum chunk size for text splitter.
            chunk_overlap: Number of overlapping characters between chunks.
            html_headers_to_split_on: HTML headers used as split boundaries.
        """
        self.db = db
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.html_headers_to_split_on = list(
            html_headers_to_split_on
            or [
                ('h1', 'Header 1'),
                ('h2', 'Header 2'),
                ('h3', 'Header 3'),
            ]
        )
        self._text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=self.chunk_size,
            chunk_overlap=self.chunk_overlap,
            add_start_index=True,
        )
        self._html_splitter = HTMLHeaderTextSplitter(headers_to_split_on=self.html_headers_to_split_on)

    def chunk_document(
        self,
        document_id: int,
        content: str,
        content_type: ContentType = ContentType.TEXT,
        metadata: dict[str, Any] | None = None,
    ) -> int:
        """Split a document into chunks and store them in the database.

        Args:
            document_id: Parent document id in the database.
            content: Raw document body to split.
            content_type: Content mode controlling splitter strategy.
            metadata: Optional metadata copied into each chunk payload.

        Returns:
            Number of chunks inserted.
        """
        base_metadata: dict[str, Any] = dict(metadata or {})
        base_metadata['content_type'] = content_type.value

        if content_type == ContentType.HTML:
            documents = self._html_splitter.split_text(content)
            chunk_method = 'html_header_splitter'
        else:
            documents = self._text_splitter.create_documents([content], metadatas=[base_metadata])
            chunk_method = 'recursive_character'

        chunks_inserted = 0
        chunk_lengths: list[int] = []
        for chunk_index, document in enumerate(documents):
            text = document.page_content.strip()
            if not text:
                continue

            chunk_metadata: dict[str, Any] = {
                **base_metadata,
                **document.metadata,
                'chunk_method': chunk_method,
                'chunk_index': chunk_index,
            }
            token_count = self._token_count(text)
            self.db.insert_chunk(
                document_id=document_id,
                chunk_index=chunk_index,
                content=text,
                token_count=token_count,
                meta_data=chunk_metadata,
            )
            chunks_inserted += 1
            chunk_length = len(text)
            chunk_lengths.append(chunk_length)
            logger.debug(f'Created chunk document_id={document_id} chunk_index={chunk_index} length={chunk_length} chars token_count={token_count}')

        logger.info(
            f'Chunking completed document_id={document_id} '
            f'content_type={content_type} '
            f'chunks_created={chunks_inserted} '
            f'chunk_lengths={chunk_lengths}'
        )

        return chunks_inserted

    def _token_count(self, text: str) -> int:
        """Estimate token count using whitespace tokenization.

        Args:
            text: Chunk content.

        Returns:
            Approximate token count.
        """
        return len(text.split())
