import json
import sqlite3
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from loguru import logger

from gwaihir.db.models import Index, Page, Text


class RedbookDatabase:
    def __init__(self, db_path: Path = Path('storage/redbook.db')) -> None:
        self.db_path = db_path
        self.connection = None
        if not self.db_path.parent.exists():
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.db_path)
        connection.execute('PRAGMA foreign_keys = ON;')
        self.connection = connection
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
            self.connection = None

    def execute(self, query: str, params: tuple = ()) -> None:
        with self.connect() as conn:
            conn.execute(query, params)

    def insert_index(self, index: Index) -> int:
        exists_query = 'SELECT 1 FROM "index" WHERE page_id = ? OR url = ? LIMIT 1;'
        insert_query = """
        INSERT OR IGNORE INTO "index" (page_id, title, url)
        VALUES (?, ?, ?);
        """
        with self.connect() as conn:
            existing = conn.execute(exists_query, (index.pageid, index.url)).fetchone()
            if existing is not None:
                return index.pageid

            cursor = conn.execute(insert_query, (index.pageid, index.title, index.url))
            if cursor.lastrowid is None:
                return index.pageid
            return cursor.lastrowid

    def insert_indexes(self, indexes: Sequence[Index]) -> int:
        if not indexes:
            return 0

        insert_query = """
        INSERT OR IGNORE INTO "index" (page_id, title, url)
        VALUES (?, ?, ?);
        """

        rows: list[tuple[int, str, str]] = []
        with self.connect() as conn:
            existing_rows = conn.execute('SELECT page_id, url FROM "index"').fetchall()
            existing_page_ids = {int(row[0]) for row in existing_rows}
            existing_urls = {str(row[1]) for row in existing_rows if row[1] is not None}

            seen_page_ids: set[int] = set()
            seen_urls: set[str] = set()
            for index in indexes:
                if index.pageid in existing_page_ids or index.url in existing_urls:
                    continue
                if index.pageid in seen_page_ids or index.url in seen_urls:
                    continue

                rows.append((index.pageid, index.title, index.url))
                seen_page_ids.add(index.pageid)
                seen_urls.add(index.url)

            if not rows:
                return 0

            conn.executemany(insert_query, rows)

        return len(rows)

    def insert_document(self, page: Page) -> int:
        insert_document_query = """
        INSERT INTO document (
            title,
            url,
            raw_content
        )
        VALUES (?, ?, ?);
        """
        insert_wiki_page_query = """
        INSERT INTO wiki_page (
            document_id,
            page_id,
            categories,
            images,
            links,
            external_links,
            sections,
            revid,
            displaytitle,
            properties
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        with self.connect() as conn:
            cursor = conn.execute(insert_document_query, (page.title, page.url, page.content))
            if cursor.lastrowid is None:
                logger.warning('Failed to insert document: lastrowid is None')
                return -1

            document_id = int(cursor.lastrowid)
            conn.execute(
                insert_wiki_page_query,
                (
                    document_id,
                    page.pageid,
                    json.dumps(page.categories or [], ensure_ascii=False),
                    json.dumps(page.images or [], ensure_ascii=False),
                    json.dumps(page.links or [], ensure_ascii=False),
                    json.dumps(page.external_links or [], ensure_ascii=False),
                    json.dumps(page.sections or [], ensure_ascii=False),
                    page.revid,
                    page.displaytitle,
                    json.dumps(page.properties or [], ensure_ascii=False),
                ),
            )
            return document_id

    def insert_text(self, text: Text) -> int:
        insert_document_query = """
        INSERT INTO document (
            title,
            url,
            raw_content
        )
        VALUES (?, ?, ?);
        """
        insert_text_query = """
        INSERT INTO text (
            document_id,
            author,
            publisher,
            published_year,
            isbn,
            language,
            source_path,
            file_format
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """

        source_url = text.url or text.source_path
        with self.connect() as conn:
            cursor = conn.execute(insert_document_query, (text.title, source_url, text.content))
            if cursor.lastrowid is None:
                logger.warning('Failed to insert document for text: lastrowid is None')
                return -1

            document_id = int(cursor.lastrowid)
            conn.execute(
                insert_text_query,
                (
                    document_id,
                    text.author,
                    text.publisher,
                    text.published_year,
                    text.isbn,
                    text.language,
                    text.source_path,
                    text.file_format,
                ),
            )
            return document_id

    def insert_chunk(
        self,
        document_id: int,
        chunk_index: int,
        content: str,
        token_count: int,
        meta_data: dict[str, Any] | None = None,
    ) -> int:
        insert_query = """
        INSERT INTO chunks (document_id, chunk_index, content, token_count, meta_data)
        VALUES (?, ?, ?, ?, ?);
        """
        meta_data_json = json.dumps(meta_data or {}, ensure_ascii=False)
        with self.connect() as conn:
            cursor = conn.execute(insert_query, (document_id, chunk_index, content, token_count, meta_data_json))
            if cursor.lastrowid is None:
                logger.warning('Failed to insert chunk: lastrowid is None')
                return -1
            return cursor.lastrowid

    def document_count(self) -> int:
        query = 'SELECT COUNT(*) FROM document;'
        with self.connect() as conn:
            row = conn.execute(query).fetchone()
            if row is None:
                return 0
            return int(row[0])

    def document_exists(self, title: str) -> bool:
        query = 'SELECT 1 FROM document WHERE title = ? LIMIT 1;'
        with self.connect() as conn:
            row = conn.execute(query, (title,)).fetchone()
            return row is not None

    def text_exists(self, source_path: str) -> bool:
        query = 'SELECT 1 FROM text WHERE source_path = ? LIMIT 1;'
        with self.connect() as conn:
            row = conn.execute(query, (source_path,)).fetchone()
            return row is not None

    def _create_index_table(self) -> None:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS "index" (
            page_id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            url TEXT
        );
        """
        self.execute(create_table_query)

    def _create_document_table(self) -> None:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS document (
            document_id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            url TEXT,
            raw_content TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        self.execute(create_table_query)

    def _create_wiki_page_table(self) -> None:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS wiki_page (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id INTEGER NOT NULL,
            page_id INTEGER NOT NULL,
            categories TEXT,
            images TEXT,
            links TEXT,
            external_links TEXT,
            sections TEXT,
            revid INTEGER,
            displaytitle TEXT,
            properties TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (document_id) REFERENCES document (document_id) ON DELETE CASCADE
        );
        """
        self.execute(create_table_query)

    def _table_exists(self, conn: sqlite3.Connection, table_name: str) -> bool:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1;",
            (table_name,),
        ).fetchone()
        return row is not None

    def _create_text_table(self) -> None:
        with self.connect() as conn:
            if self._table_exists(conn, 'book') and not self._table_exists(conn, 'text'):
                conn.execute('ALTER TABLE book RENAME TO text;')

        create_table_query = """
        CREATE TABLE IF NOT EXISTS text (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id INTEGER NOT NULL UNIQUE,
            author TEXT,
            publisher TEXT,
            published_year INTEGER,
            isbn TEXT,
            language TEXT,
            source_path TEXT,
            file_format TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (document_id) REFERENCES document (document_id) ON DELETE CASCADE
        );
        """
        with self.connect() as conn:
            conn.execute(create_table_query)

    def _create_chunks_table(self) -> None:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS chunks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id INTEGER NOT NULL,
            chunk_index INTEGER NOT NULL,
            content TEXT NOT NULL,
            token_count INTEGER NOT NULL,
            meta_data TEXT NOT NULL DEFAULT '{}',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (document_id) REFERENCES document (document_id) ON DELETE CASCADE
        );
        """
        with self.connect() as conn:
            conn.execute(create_table_query)
            columns = {str(row[1]) for row in conn.execute('PRAGMA table_info(chunks);').fetchall()}
            if 'meta_data' not in columns:
                conn.execute("ALTER TABLE chunks ADD COLUMN meta_data TEXT NOT NULL DEFAULT '{}';")

    def deploy(self) -> None:
        self._create_index_table()
        self._create_document_table()
        self._create_wiki_page_table()
        self._create_text_table()
        self._create_chunks_table()

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None
