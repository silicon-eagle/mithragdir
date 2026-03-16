from pathlib import Path

import pytest
from gwaihir.retriever.text_client import TextClient
from lembas_core.db import RedbookDatabase


@pytest.fixture
def db(tmp_path: Path) -> RedbookDatabase:
    database = RedbookDatabase(db_path=tmp_path / 'test_text_client.db')
    return database


@pytest.fixture
def source_folder(tmp_path: Path) -> Path:
    folder = tmp_path / 'database' / 'books'
    folder.mkdir(parents=True, exist_ok=True)
    return folder


@pytest.fixture
def client(db: RedbookDatabase, source_folder: Path) -> TextClient:
    return TextClient(source_folder=source_folder, db=db, batch_size=2)


class TestTextClient:
    def test_constructor_uses_explicit_source_folder(self, db: RedbookDatabase, source_folder: Path) -> None:
        client = TextClient(db=db, source_folder=source_folder)
        assert client.source_folder == source_folder
        assert client.index_path == source_folder / 'index.csv'

    def test_ingest_local_books_stores_documents_and_books(
        self,
        client: TextClient,
        source_folder: Path,
        db: RedbookDatabase,
    ) -> None:
        first_path = source_folder / 'book_one.txt'
        second_path = source_folder / 'book_two.txt'
        _missing_path = source_folder / 'missing.txt'

        first_path.write_text('content from book one', encoding='utf-8')
        second_path.write_text('content from book two', encoding='utf-8')
        (source_folder / 'index.csv').write_text(
            '\n'.join(
                [
                    'file;author;title;publisher;published_year;isbn;language',
                    'book_one;Author One;Book One;Pub A;2001;111;en',
                    'book_two;Author Two;Book Two;Pub B;2002;222;en',
                    'missing;Author Three;Missing Book;Pub C;2003;333;en',
                ]
            ),
            encoding='utf-8',
        )

        flushed = client.ingest(pause_seconds=0.0)

        assert flushed == 0
        assert db.document_count() == 2

        with db.connect() as conn:
            book_count_row = conn.execute('SELECT COUNT(*) FROM text;').fetchone()

        assert book_count_row is not None
        assert int(book_count_row[0]) == 2

        with db.connect() as conn:
            row = conn.execute('SELECT author, publisher, published_year, isbn, language FROM text ORDER BY id LIMIT 1').fetchone()

        assert row is not None
        assert row[0] == 'Author One'
        assert row[1] == 'Pub A'
        assert int(row[2]) == 2001
        assert row[3] == '111'
        assert row[4] == 'en'

    def test_ingest_skips_already_ingested_books(
        self,
        client: TextClient,
        source_folder: Path,
        db: RedbookDatabase,
    ) -> None:
        txt_path = source_folder / 'book_one.txt'
        txt_path.write_text('same content', encoding='utf-8')
        (source_folder / 'index.csv').write_text('file;author;title\nbook_one;Author;Book One\n', encoding='utf-8')

        client.ingest(pause_seconds=0.0)
        first_count = db.document_count()

        client.ingest(pause_seconds=0.0)
        second_count = db.document_count()

        assert first_count == 1
        assert second_count == 1
