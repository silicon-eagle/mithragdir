from pathlib import Path

import pytest
from gwaihir.db.db import RedbookDatabase
from gwaihir.db.models import Index, Page


@pytest.fixture
def db(tmp_path: Path) -> RedbookDatabase:
    database = RedbookDatabase(db_path=tmp_path / 'test.db')
    database._create_index_table()
    database._create_page_table()
    database._create_chunks_table()
    return database


class TestRedbookDatabase:
    def test_insert_index(self, db: RedbookDatabase) -> None:
        index_id = db.insert_index(Index(title='Title', pageid=123, url='http://example.com'))
        assert isinstance(index_id, int)

    def test_insert_indexes_batch(self, db: RedbookDatabase) -> None:
        inserted = db.insert_indexes(
            [
                Index(title='Doc 1', pageid=1, url='http://a.com'),
                Index(title='Doc 2', pageid=2, url='http://b.com'),
                Index(title='Doc 1 duplicate', pageid=1, url='http://a-dup.com'),
            ]
        )
        assert inserted == 3

        with db.connect() as conn:
            rows = conn.execute('SELECT page_id, title, url FROM "index" ORDER BY page_id').fetchall()

        assert len(rows) == 2
        assert rows[0][0] == 1
        assert rows[1][0] == 2

    def test_insert_page(self, db: RedbookDatabase) -> None:
        db.insert_index(Index(title='Title', pageid=123, url='http://example.com'))
        doc_id = db.insert_page(Page(title='Title', pageid=123, url='http://example.com', content='content'))
        assert isinstance(doc_id, int)
        assert doc_id > 0

    def test_insert_page_returns_unique_ids(self, db: RedbookDatabase) -> None:
        db.insert_index(Index(title='Doc 1', pageid=1, url='http://a.com'))
        db.insert_index(Index(title='Doc 2', pageid=2, url='http://b.com'))
        id1 = db.insert_page(Page(title='Doc 1', pageid=1, url='http://a.com', content='content a'))
        id2 = db.insert_page(Page(title='Doc 2', pageid=2, url='http://b.com', content='content b'))
        assert id1 != id2

    def test_insert_chunk(self, db: RedbookDatabase) -> None:
        db.insert_index(Index(title='Title', pageid=123, url='http://example.com'))
        doc_id = db.insert_page(Page(title='Title', pageid=123, url='http://example.com', content='content'))
        chunk_id = db.insert_chunk(doc_id, 0, 'chunk text', 10)
        assert isinstance(chunk_id, int)
        assert chunk_id > 0

    def test_insert_multiple_chunks(self, db: RedbookDatabase) -> None:
        db.insert_index(Index(title='Title', pageid=123, url='http://example.com'))
        doc_id = db.insert_page(Page(title='Title', pageid=123, url='http://example.com', content='content'))
        chunk_ids = [db.insert_chunk(doc_id, i, f'chunk {i}', i + 1) for i in range(3)]
        assert len(set(chunk_ids)) == 3

    def test_page_exists(self, db: RedbookDatabase) -> None:
        db.insert_index(Index(title='Title', pageid=123, url='http://example.com'))
        db.insert_page(Page(title='Title', pageid=123, url='http://example.com', content='content'))
        assert db.page_exists('Title')
        assert not db.page_exists('Missing Title')

    def test_db_file_created(self, tmp_path: Path) -> None:
        db_path = tmp_path / 'test.db'
        database = RedbookDatabase(db_path=db_path)
        database._create_index_table()
        database._create_page_table()
        assert db_path.exists()

    def test_parent_directory_created(self, tmp_path: Path) -> None:
        db_path = tmp_path / 'nested' / 'dir' / 'test.db'
        RedbookDatabase(db_path=db_path)
        assert db_path.parent.exists()
