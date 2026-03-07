from pathlib import Path

import pytest
from gwaihir.db.db import RedbookDatabase
from gwaihir.db.models import Index, Page, Text


@pytest.fixture
def db(tmp_path: Path) -> RedbookDatabase:
    database = RedbookDatabase(db_path=tmp_path / 'test.db')
    database._create_index_table()
    database._create_document_table()
    database._create_wiki_page_table()
    database._create_text_table()
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
        assert inserted == 2

        with db.connect() as conn:
            rows = conn.execute('SELECT page_id, title, url FROM "index" ORDER BY page_id').fetchall()

        assert len(rows) == 2
        assert rows[0][0] == 1
        assert rows[1][0] == 2

    def test_insert_indexes_is_idempotent(self, db: RedbookDatabase) -> None:
        first = [
            Index(title='Doc 1', pageid=1, url='http://a.com'),
            Index(title='Doc 2', pageid=2, url='http://b.com'),
        ]
        inserted_first = db.insert_indexes(first)
        assert inserted_first == 2

        second = [
            Index(title='Doc 1 duplicate pageid', pageid=1, url='http://new-a.com'),
            Index(title='Doc 2 duplicate url', pageid=3, url='http://b.com'),
            Index(title='Doc 3', pageid=4, url='http://d.com'),
        ]
        inserted_second = db.insert_indexes(second)
        assert inserted_second == 1

        with db.connect() as conn:
            count = conn.execute('SELECT COUNT(*) FROM "index"').fetchone()

        assert count is not None
        assert int(count[0]) == 3

    def test_insert_document(self, db: RedbookDatabase) -> None:
        db.insert_index(Index(title='Title', pageid=123, url='http://example.com'))
        doc_id = db.insert_document(Page(title='Title', pageid=123, url='http://example.com', content='content'))
        assert isinstance(doc_id, int)
        assert doc_id > 0

    def test_insert_document_returns_unique_ids(self, db: RedbookDatabase) -> None:
        db.insert_index(Index(title='Doc 1', pageid=1, url='http://a.com'))
        db.insert_index(Index(title='Doc 2', pageid=2, url='http://b.com'))
        id1 = db.insert_document(Page(title='Doc 1', pageid=1, url='http://a.com', content='content a'))
        id2 = db.insert_document(Page(title='Doc 2', pageid=2, url='http://b.com', content='content b'))
        assert id1 != id2

    def test_insert_chunk(self, db: RedbookDatabase) -> None:
        db.insert_index(Index(title='Title', pageid=123, url='http://example.com'))
        doc_id = db.insert_document(Page(title='Title', pageid=123, url='http://example.com', content='content'))
        chunk_id = db.insert_chunk(doc_id, 0, 'chunk text', 10, {'source': 'unit-test'})
        assert isinstance(chunk_id, int)
        assert chunk_id > 0

        with db.connect() as conn:
            row = conn.execute('SELECT meta_data FROM chunks WHERE id = ?', (chunk_id,)).fetchone()

        assert row is not None
        assert row[0] == '{"source": "unit-test"}'

    def test_insert_multiple_chunks(self, db: RedbookDatabase) -> None:
        db.insert_index(Index(title='Title', pageid=123, url='http://example.com'))
        doc_id = db.insert_document(Page(title='Title', pageid=123, url='http://example.com', content='content'))
        chunk_ids = [db.insert_chunk(doc_id, i, f'chunk {i}', i + 1) for i in range(3)]
        assert len(set(chunk_ids)) == 3

    def test_document_exists(self, db: RedbookDatabase) -> None:
        db.insert_index(Index(title='Title', pageid=123, url='http://example.com'))
        db.insert_document(Page(title='Title', pageid=123, url='http://example.com', content='content'))
        assert db.document_exists('Title')
        assert not db.document_exists('Missing Title')

    def test_insert_document_stores_wiki_metadata(self, db: RedbookDatabase) -> None:
        page = Page(
            title='Metadata Title',
            pageid=456,
            url='http://example.com/meta',
            content='metadata content',
            categories=[{'ns': 14, '*': 'Category:Test'}],
            images=['img.png'],
            links=[{'ns': 0, '*': 'Other'}],
            external_links=['https://example.org'],
            sections=[{'index': '1', 'line': 'Intro'}],
            revid=42,
            displaytitle='Metadata Title',
            properties=[{'name': 'foo', '*': 'bar'}],
        )
        document_id = db.insert_document(page)

        with db.connect() as conn:
            row = conn.execute(
                'SELECT document_id, page_id, categories, images, links, external_links, sections, revid, displaytitle, properties '
                'FROM wiki_page WHERE document_id = ?',
                (document_id,),
            ).fetchone()

        assert row is not None
        assert int(row[0]) == document_id
        assert int(row[1]) == 456
        assert row[2] is not None
        assert row[3] is not None
        assert row[4] is not None
        assert row[5] is not None
        assert row[6] is not None
        assert int(row[7]) == 42
        assert row[8] == 'Metadata Title'
        assert row[9] is not None

    def test_insert_text_creates_document_and_text(self, db: RedbookDatabase) -> None:
        book = Text(
            title='The Hobbit',
            content='In a hole in the ground there lived a hobbit.',
            author='J.R.R. Tolkien',
            publisher='Allen & Unwin',
            published_year=1937,
            isbn='9780000000001',
            language='en',
            source_path='/tmp/the_hobbit.pdf',
            file_format='pdf',
        )
        document_id = db.insert_text(book)

        with db.connect() as conn:
            document_row = conn.execute('SELECT document_id, title, raw_content FROM document WHERE document_id = ?', (document_id,)).fetchone()
            text_row = conn.execute(
                'SELECT document_id, author, publisher, published_year, isbn, language, source_path, file_format FROM text WHERE document_id = ?',
                (document_id,),
            ).fetchone()

        assert document_row is not None
        assert int(document_row[0]) == document_id
        assert document_row[1] == 'The Hobbit'
        assert document_row[2] == 'In a hole in the ground there lived a hobbit.'

        assert text_row is not None
        assert int(text_row[0]) == document_id
        assert text_row[1] == 'J.R.R. Tolkien'
        assert text_row[2] == 'Allen & Unwin'
        assert int(text_row[3]) == 1937
        assert text_row[4] == '9780000000001'
        assert text_row[5] == 'en'
        assert text_row[6] == '/tmp/the_hobbit.pdf'
        assert text_row[7] == 'pdf'

    def test_text_exists(self, db: RedbookDatabase) -> None:
        source_path = '/tmp/fellowship.epub'
        db.insert_text(Text(title='Fellowship', content='One Ring', author='J.R.R. Tolkien', source_path=source_path, file_format='epub'))

        assert db.text_exists(source_path)
        assert not db.text_exists('/tmp/missing.pdf')

    def test_db_file_created(self, tmp_path: Path) -> None:
        db_path = tmp_path / 'test.db'
        database = RedbookDatabase(db_path=db_path)
        database._create_index_table()
        database._create_document_table()
        database._create_wiki_page_table()
        database._create_text_table()
        assert db_path.exists()

    def test_parent_directory_created(self, tmp_path: Path) -> None:
        db_path = tmp_path / 'nested' / 'dir' / 'test.db'
        RedbookDatabase(db_path=db_path)
        assert db_path.parent.exists()
