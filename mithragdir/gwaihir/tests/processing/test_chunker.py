import os

import pytest
from gwaihir.processing.chunker import Chunker, ContentType
from lembas_core.db import RedbookDatabase
from lembas_core.models import Chunk
from lembas_core.schemas import Page, Text


@pytest.fixture
def db() -> RedbookDatabase:
    db_url = os.getenv('DEV_DATABASE_URL')
    if not db_url:
        pytest.skip('DEV_DATABASE_URL is required for PostgreSQL-backed tests.')

    database = RedbookDatabase(db_url=db_url)
    database.execute('TRUNCATE TABLE chunks, text, wiki_page, "index", document RESTART IDENTITY CASCADE')
    return database


class TestChunker:
    def test_chunk_text_document_stores_recursive_chunks(self, db: RedbookDatabase) -> None:
        document_id = db.insert_text(
            Text(
                title='Long Text',
                content=' '.join(['hobbit'] * 120),
                author='J.R.R. Tolkien',
                source_path='/tmp/long_text.txt',
                file_format='txt',
            )
        )
        chunker = Chunker(db=db, chunk_size=120, chunk_overlap=20)

        inserted = chunker.chunk_document(
            document_id=document_id,
            content=' '.join(['hobbit'] * 120),
            content_type=ContentType.TEXT,
            metadata={'title': 'Long Text', 'source_path': '/tmp/long_text.txt'},
        )

        assert inserted > 1

        chunks = db.get_chunks(document_id=document_id)
        assert len(chunks) == inserted
        assert all(isinstance(chunk, Chunk) for chunk in chunks)
        assert chunks[0].meta_data['content_type'] == 'text'
        assert chunks[0].meta_data['chunk_method'] == 'recursive_character'
        assert chunks[0].meta_data['title'] == 'Long Text'
        assert isinstance(chunks[0].meta_data, dict)
        assert all(chunk.id > 0 for chunk in chunks)

    def test_chunk_html_document_stores_html_chunks(self, db: RedbookDatabase) -> None:
        html_content = (
            '<h1>Valinor</h1><p>The light of the Trees shone in Aman.</p><h2>Two Trees</h2><p>Telperion and Laurelin gave alternating light.</p>'
        )
        document_id = db.insert_document(
            Page(
                title='Valinor',
                pageid=123,
                url='https://example.com/wiki/Valinor',
                content=html_content,
            )
        )
        chunker = Chunker(db=db)

        inserted = chunker.chunk_document(
            document_id=document_id,
            content=html_content,
            content_type=ContentType.HTML,
            metadata={'title': 'Valinor', 'source': 'wiki'},
        )

        assert inserted >= 1

        chunks = db.get_chunks(document_id=document_id)
        assert len(chunks) == inserted
        assert all(isinstance(chunk, Chunk) for chunk in chunks)
        assert chunks[0].meta_data['content_type'] == 'html'
        assert chunks[0].meta_data['chunk_method'] == 'html_cleaned_recursive_character'
        assert chunks[0].meta_data['source'] == 'wiki'
        assert isinstance(chunks[0].meta_data, dict)
        assert chunks[0].content.startswith('# Valinor')
        assert '<h1>' not in chunks[0].content

    def test_chunk_documents_processes_text_and_html_and_skips_empty(self, db: RedbookDatabase) -> None:
        text_document_id = db.insert_text(
            Text(
                title='The Shire',
                content='In a hole in the ground there lived a hobbit.',
                author='J.R.R. Tolkien',
                source_path='/tmp/shire.txt',
                file_format='txt',
            )
        )

        html_content = '<h1>Gondor</h1><p>The realm in exile was founded by Elendil.</p>'
        wiki_document_id = db.insert_document(
            Page(
                title='Gondor',
                pageid=42,
                url='https://example.com/wiki/Gondor',
                content=html_content,
            )
        )

        _empty_document_id = db.insert_text(
            Text(
                title='Empty Source',
                content='   ',
                author='Unknown',
                source_path='/tmp/empty.txt',
                file_format='txt',
            )
        )

        chunker = Chunker(db=db, chunk_size=120, chunk_overlap=20)
        processed_documents, inserted_chunks = chunker.chunk_documents()

        assert processed_documents == 2
        assert inserted_chunks >= 2

        chunks = db.get_chunks()
        assert len(chunks) == inserted_chunks
        chunk_document_ids = {chunk.document_id for chunk in chunks}
        assert text_document_id in chunk_document_ids
        assert wiki_document_id in chunk_document_ids

        for chunk in chunks:
            assert 'content_type' in chunk.meta_data
            assert 'chunk_method' in chunk.meta_data
