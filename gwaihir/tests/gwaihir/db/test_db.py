from pathlib import Path

import pytest
from gwaihir.db.db import RedbookDatabase


@pytest.fixture
def db(tmp_path: Path) -> RedbookDatabase:
    database = RedbookDatabase(db_path=tmp_path / 'test.db')
    database._create_documents_table()
    database._create_chunks_table()
    return database


class TestRedbookDatabase:
    def test_insert_document(self, db: RedbookDatabase) -> None:
        doc_id = db.insert_document('Title', 'http://example.com', 'content')
        assert isinstance(doc_id, int)
        assert doc_id > 0

    def test_insert_document_returns_unique_ids(self, db: RedbookDatabase) -> None:
        id1 = db.insert_document('Doc 1', 'http://a.com', 'content a')
        id2 = db.insert_document('Doc 2', 'http://b.com', 'content b')
        assert id1 != id2

    def test_insert_chunk(self, db: RedbookDatabase) -> None:
        doc_id = db.insert_document('Title', 'http://example.com', 'content')
        chunk_id = db.insert_chunk(doc_id, 0, 'chunk text', 10)
        assert isinstance(chunk_id, int)
        assert chunk_id > 0

    def test_insert_multiple_chunks(self, db: RedbookDatabase) -> None:
        doc_id = db.insert_document('Title', 'http://example.com', 'content')
        chunk_ids = [db.insert_chunk(doc_id, i, f'chunk {i}', i + 1) for i in range(3)]
        assert len(set(chunk_ids)) == 3

    def test_db_file_created(self, tmp_path: Path) -> None:
        db_path = tmp_path / 'test.db'
        database = RedbookDatabase(db_path=db_path)
        database._create_documents_table()
        assert db_path.exists()

    def test_parent_directory_created(self, tmp_path: Path) -> None:
        db_path = tmp_path / 'nested' / 'dir' / 'test.db'
        RedbookDatabase(db_path=db_path)
        assert db_path.parent.exists()
