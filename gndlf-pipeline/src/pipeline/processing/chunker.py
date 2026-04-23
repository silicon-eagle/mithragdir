from __future__ import annotations

import re
from enum import StrEnum
from typing import Any

import click
from bs4 import BeautifulSoup
from core.config import DEFAULT_CHUNK_OVERLAP, DEFAULT_CHUNK_SIZE, DEFAULT_CHUNK_TOKENIZER_NAME
from core.db import RedbookDatabase
from core.models import Chunk, Document, Text, WikiPage
from langchain_text_splitters import RecursiveCharacterTextSplitter
from loguru import logger
from markdownify import markdownify as md
from transformers import AutoTokenizer


class ContentType(StrEnum):
    TEXT = 'text'
    HTML = 'html'


class ChunkUnit(StrEnum):
    CHARACTERS = 'characters'
    TOKENS = 'tokens'


def clean_wiki_html_for_chunking(html_content: str) -> str:
    """Strip Tolkien Gateway wiki noise and return markdown-ready text for chunking."""
    soup = BeautifulSoup(html_content, 'html.parser')

    main_content = soup.find(id='mw-content-text')
    if not main_content:
        main_content = soup

    noise_selectors = [
        '.mw-editsection',
        '.reference',
        '.toc',
        '.navbox',
        '.infobox',
        '.mbox-small',
        '.gallery',
        '.printfooter',
    ]

    for selector in noise_selectors:
        for element in main_content.select(selector):
            element.decompose()

    for tag in main_content(['script', 'style']):
        tag.decompose()

    clean_html = str(main_content)
    markdown_text = md(clean_html, heading_style='ATX', strip=['img']).strip()
    markdown_text = re.sub(r'\n{3,}', '\n\n', markdown_text)
    return markdown_text


class Chunker:
    def __init__(
        self,
        db: RedbookDatabase,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        chunk_overlap: int = DEFAULT_CHUNK_OVERLAP,
        chunk_unit: ChunkUnit = ChunkUnit.CHARACTERS,
        tokenizer_name: str = DEFAULT_CHUNK_TOKENIZER_NAME,
    ) -> None:
        """Configure text and HTML chunking strategies.

        Args:
            db: Database used to persist chunks.
            chunk_size: Maximum chunk size for text splitter.
            chunk_overlap: Number of overlapping units between chunks.
            chunk_unit: Unit used by splitter; either characters or tokens.
            tokenizer_name: Tokenizer used when chunk_unit='tokens'.
        """
        self.db = db
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.chunk_unit = chunk_unit
        self.tokenizer_name = tokenizer_name
        self._tokenizer = None

        if self.chunk_unit == ChunkUnit.TOKENS:
            self._tokenizer = AutoTokenizer.from_pretrained(self.tokenizer_name, use_fast=True)
            self._text_splitter = RecursiveCharacterTextSplitter(
                chunk_size=self.chunk_size,
                chunk_overlap=self.chunk_overlap,
                length_function=self._token_length,
                add_start_index=True,
            )
        else:
            self._text_splitter = RecursiveCharacterTextSplitter(
                chunk_size=self.chunk_size,
                chunk_overlap=self.chunk_overlap,
                add_start_index=True,
            )

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
            cleaned_content = clean_wiki_html_for_chunking(content)
            documents = self._text_splitter.create_documents([cleaned_content], metadatas=[base_metadata])
            chunk_method = 'html_cleaned_recursive_character' if self.chunk_unit == ChunkUnit.CHARACTERS else 'html_cleaned_recursive_tokens'
        else:
            documents = self._text_splitter.create_documents([content], metadatas=[base_metadata])
            chunk_method = 'recursive_character' if self.chunk_unit == ChunkUnit.CHARACTERS else 'recursive_tokens'

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

        logger.debug(
            f'Chunking completed document_id={document_id} '
            f'content_type={content_type} '
            f'chunks_created={chunks_inserted} '
            f'chunk_lengths={chunk_lengths}'
        )

        return chunks_inserted

    def _token_count(self, text: str) -> int:
        """Estimate token count for chunk content.

        Args:
            text: Chunk content.

        Returns:
            Approximate token count.
        """
        if self._tokenizer is not None:
            return self._token_length(text)
        return len(text.split())

    def _token_length(self, text: str) -> int:
        """Return tokenizer length without emitting max-sequence warnings."""
        if self._tokenizer is None:
            return len(text.split())
        return len(self._tokenizer.encode(text, add_special_tokens=False, verbose=False))

    def clear_chunks(self) -> int:
        """Delete all chunk rows and return how many existed before deletion.

        Returns:
            Number of previously stored chunks.
        """
        count = Chunk.select().count()
        Chunk.delete().execute()
        return count

    def chunk_documents(self, show_progress: bool = True) -> tuple[int, int]:
        """Chunk all ingested documents (wiki and text) and store chunk rows.

        Args:
            show_progress: Whether to render a terminal progress bar.

        Returns:
            Tuple of ``(processed_documents, inserted_chunks)``.
        """
        # Using Peewee to iterate over all documents.
        documents = Document.select().order_by(Document.document_id)

        # Convert to list to know length for progress bar (could be large though)
        docs = list(documents)

        processed_documents = 0
        inserted_chunks = 0

        logger.info(f'Starting chunking of {len(docs)} documents.')

        # Define processing logic
        def process_doc(doc: Document) -> None:
            nonlocal processed_documents, inserted_chunks
            content = doc.raw_content
            if not content or not str(content).strip():
                logger.warning(f'Skipping empty document content for document_id={doc.document_id}')
                return

            is_wiki = WikiPage.select().where(WikiPage.document == doc).exists()
            content_type = ContentType.HTML if is_wiki else ContentType.TEXT

            if not is_wiki:
                is_text = Text.select().where(Text.document == doc).exists()
                if not is_text:
                    return

            logger.debug(f'Chunking document_id={doc.document_id} title="{doc.title}" content_type={content_type}')

            inserted = self.chunk_document(
                document_id=doc.get_id(),
                content=str(content),
                content_type=content_type,
                metadata={'document_id': doc.document_id, 'title': doc.title, 'url': doc.url},
            )
            inserted_chunks += inserted
            processed_documents += 1

        if show_progress:
            with click.progressbar(docs, label='Chunking documents', show_pos=True) as progress_docs:
                for doc in progress_docs:
                    process_doc(doc)
        else:
            for doc in docs:
                process_doc(doc)

        logger.info(f'Chunking finished: processed_documents={processed_documents}, inserted_chunks={inserted_chunks}')

        return processed_documents, inserted_chunks
