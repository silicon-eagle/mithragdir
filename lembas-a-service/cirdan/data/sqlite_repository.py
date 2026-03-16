from __future__ import annotations

import sqlite3
from urllib.parse import urlparse

from cirdan.domain.models import RetrievedChunk


class SQLiteRepository:
    def __init__(self, connection_string: str) -> None:
        self.connection_string = connection_string

    def _db_path(self) -> str:
        if self.connection_string.startswith('sqlite:///'):
            return self.connection_string.removeprefix('sqlite:///')

        parsed = urlparse(self.connection_string)
        if parsed.scheme == 'sqlite':
            return parsed.path

        return self.connection_string

    async def ping(self) -> bool:
        try:
            with sqlite3.connect(self._db_path()) as connection:
                cursor = connection.execute('SELECT 1;')
                row = cursor.fetchone()
            return row is not None and int(row[0]) == 1
        except sqlite3.Error:
            return False

    async def get_chunks_by_ids(self, chunk_ids: list[str]) -> list[RetrievedChunk]:
        if not chunk_ids:
            return []

        placeholders = ','.join('?' for _ in chunk_ids)
        query = f'SELECT id, content FROM chunks WHERE CAST(id AS TEXT) IN ({placeholders}) ORDER BY id ASC;'

        with sqlite3.connect(self._db_path()) as connection:
            rows = connection.execute(query, tuple(chunk_ids)).fetchall()

        return [
            RetrievedChunk(
                chunk_id=str(row[0]),
                content=str(row[1]),
            )
            for row in rows
        ]
