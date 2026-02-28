import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from loguru import logger


class RedbookDatabase:
    def __init__(self, db_path: Path = Path('storage/redbook.db')) -> None:
        self.db_path = db_path
        self.connection = None
        if not self.db_path.parent.exists():
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.db_path)
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

    def insert_document(self, title: str, url: str, raw_content: str) -> int:
        insert_query = """
        INSERT INTO documents (title, url, raw_content)
        VALUES (?, ?, ?);
        """
        with self.connect() as conn:
            cursor = conn.execute(insert_query, (title, url, raw_content))
            if cursor.lastrowid is None:
                logger.warning('Failed to insert document: lastrowid is None')
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
                logger.warning('Failed to insert document: lastrowid is None')
                return -1
            return cursor.lastrowid

    def document_count(self) -> int:
        query = 'SELECT COUNT(*) FROM documents;'
        with self.connect() as conn:
            row = conn.execute(query).fetchone()
            if row is None:
                return 0
            return int(row[0])

    def _create_documents_table(self) -> None:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS documents (
            document_id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            url TEXT,
            raw_content TEXT NOT NULL,
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
            FOREIGN KEY (document_id) REFERENCES documents (id) ON DELETE CASCADE
        );
        """
        self.execute(create_table_query)
