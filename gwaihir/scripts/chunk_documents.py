from __future__ import annotations

from pathlib import Path

from gwaihir.db.db import RedbookDatabase
from gwaihir.processing.chunker import Chunker
from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / 'database' / 'redbook.db'
CHUNK_SIZE = 1_000
CHUNK_OVERLAP = 200


def clear_chunks(db: RedbookDatabase) -> int:
    with db.connect() as conn:
        row = conn.execute('SELECT COUNT(*) FROM chunks;').fetchone()
        existing = int(row[0]) if row is not None else 0
        conn.execute('DELETE FROM chunks;')
    return existing


def chunk_documents(db: RedbookDatabase, chunk_size: int, chunk_overlap: int) -> tuple[int, int]:
    query = """
    SELECT
        d.document_id,
        d.title,
        d.url,
        d.raw_content,
        CASE
            WHEN wp.document_id IS NOT NULL THEN 'html'
            ELSE 'text'
        END AS content_type
    FROM document AS d
    LEFT JOIN wiki_page AS wp ON wp.document_id = d.document_id
    LEFT JOIN text AS t ON t.document_id = d.document_id
    WHERE wp.document_id IS NOT NULL OR t.document_id IS NOT NULL
    ORDER BY d.document_id;
    """

    with db.connect() as conn:
        rows = conn.execute(query).fetchall()

    chunker = Chunker(db=db, chunk_size=chunk_size, chunk_overlap=chunk_overlap)

    processed_documents = 0
    inserted_chunks = 0

    for row in rows:
        document_id = int(row[0])
        title = str(row[1]) if row[1] is not None else None
        url = str(row[2]) if row[2] is not None else None
        content = str(row[3]) if row[3] is not None else ''
        content_type = str(row[4])

        if not content.strip():
            logger.warning(f'Skipping empty document content for document_id={document_id}')
            continue

        inserted = chunker.chunk_document(
            document_id=document_id,
            content=content,
            content_type=content_type,
            metadata={'document_id': document_id, 'title': title, 'url': url},
        )
        inserted_chunks += inserted
        processed_documents += 1

    return processed_documents, inserted_chunks


def main() -> int:
    db = RedbookDatabase(db_path=DEFAULT_DB_PATH)
    db.deploy()

    removed = clear_chunks(db)
    processed_documents, inserted_chunks = chunk_documents(db=db, chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP)

    logger.info(f'Removed {removed} existing chunks')
    logger.info(f'Processed {processed_documents} documents')
    logger.info(f'Inserted {inserted_chunks} chunks')
    return 0


if __name__ == '__main__':
    main()
