import json
from pathlib import Path

import pytest
from gwaihir.db.db import RedbookDatabase
from gwaihir.db.models import Chunk, Page, Text
from gwaihir.processing.chunker import Chunker


@pytest.fixture
def db(tmp_path: Path) -> RedbookDatabase:
    database = RedbookDatabase(db_path=tmp_path / 'test_chunker.db')
    database._create_document_table()
    database._create_wiki_page_table()
    database._create_text_table()
    database._create_chunks_table()
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
            content_type='text',
            metadata={'title': 'Long Text', 'source_path': '/tmp/long_text.txt'},
        )

        assert inserted > 1

        with db.connect() as conn:
            rows = conn.execute(
                'SELECT chunk_index, content, token_count, meta_data FROM chunks WHERE document_id = ? ORDER BY chunk_index',
                (document_id,),
            ).fetchall()

        assert len(rows) == inserted
        first_meta = json.loads(str(rows[0][3]))
        assert first_meta['content_type'] == 'text'
        assert first_meta['chunk_method'] == 'recursive_character'
        assert first_meta['title'] == 'Long Text'
        assert int(rows[0][2]) > 0

        chunks = db.get_chunks(document_id=document_id)
        assert len(chunks) == inserted
        assert all(isinstance(chunk, Chunk) for chunk in chunks)
        assert chunks[0].meta_data['content_type'] == 'text'
        assert chunks[0].meta_data['chunk_method'] == 'recursive_character'
        assert isinstance(chunks[0].meta_data, dict)

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
            content_type='html',
            metadata={'title': 'Valinor', 'source': 'wiki'},
        )

        assert inserted >= 1

        with db.connect() as conn:
            rows = conn.execute(
                'SELECT content, token_count, meta_data FROM chunks WHERE document_id = ? ORDER BY chunk_index',
                (document_id,),
            ).fetchall()

        assert len(rows) == inserted
        first_meta = json.loads(str(rows[0][2]))
        assert first_meta['content_type'] == 'html'
        assert first_meta['chunk_method'] == 'html_header_splitter'
        assert first_meta['source'] == 'wiki'
        assert int(rows[0][1]) > 0

        chunks = db.get_chunks(document_id=document_id)
        assert len(chunks) == inserted
        assert all(isinstance(chunk, Chunk) for chunk in chunks)
        assert chunks[0].meta_data['content_type'] == 'html'
        assert chunks[0].meta_data['chunk_method'] == 'html_header_splitter'
        assert chunks[0].meta_data['source'] == 'wiki'
        assert isinstance(chunks[0].meta_data, dict)
