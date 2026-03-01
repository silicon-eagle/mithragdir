import json
import sqlite3
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path

from loguru import logger

from gwaihir.db.models import Index, Page


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
        insert_query = """
        INSERT OR IGNORE INTO "index" (page_id, title, url)
        VALUES (?, ?, ?);
        """
        with self.connect() as conn:
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
        rows = [(index.pageid, index.title, index.url) for index in indexes]
        with self.connect() as conn:
            conn.executemany(insert_query, rows)

        return len(indexes)

    def insert_page(self, page: Page) -> int:
        insert_query = """
        INSERT INTO page (
            page_id,
            title,
            url,
            raw_content,
            categories,
            images,
            links,
            external_links,
            sections,
            revid,
            displaytitle,
            properties
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        with self.connect() as conn:
            cursor = conn.execute(
                insert_query,
                (
                    page.pageid,
                    page.title,
                    page.url,
                    page.content,
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
            if cursor.lastrowid is None:
                logger.warning('Failed to insert page: lastrowid is None')
                return -1
            return cursor.lastrowid

    def insert_chunk(self, document_id: int, chunk_index: int, content: str, token_count: int) -> int:
        insert_query = """
        INSERT INTO chunks (document_id, chunk_index, content, token_count)
        VALUES (?, ?, ?, ?);
        """
        with self.connect() as conn:
            cursor = conn.execute(insert_query, (document_id, chunk_index, content, token_count))
            if cursor.lastrowid is None:
                logger.warning('Failed to insert chunk: lastrowid is None')
                return -1
            return cursor.lastrowid

    def page_count(self) -> int:
        query = 'SELECT COUNT(*) FROM page;'
        with self.connect() as conn:
            row = conn.execute(query).fetchone()
            if row is None:
                return 0
            return int(row[0])

    def page_exists(self, title: str) -> bool:
        query = 'SELECT 1 FROM page WHERE title = ? LIMIT 1;'
        with self.connect() as conn:
            row = conn.execute(query, (title,)).fetchone()
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

    def _create_page_table(self) -> None:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS page (
            document_id INTEGER PRIMARY KEY AUTOINCREMENT,
            page_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            url TEXT,
            raw_content TEXT NOT NULL,
            categories TEXT,
            images TEXT,
            links TEXT,
            external_links TEXT,
            sections TEXT,
            revid INTEGER,
            displaytitle TEXT,
            properties TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        self.execute(create_table_query)

    def _create_chunks_table(self) -> None:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS chunks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id INTEGER NOT NULL,
            chunk_index INTEGER NOT NULL,
            content TEXT NOT NULL,
            token_count INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (document_id) REFERENCES page (document_id) ON DELETE CASCADE
        );
        """
        self.execute(create_table_query)

    def deploy(self) -> None:
        self._create_index_table()
        self._create_page_table()
        self._create_chunks_table()
