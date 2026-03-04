from pathlib import Path

from gwaihir.db.db import RedbookDatabase
from gwaihir.retriever.book_client import BookClient
from loguru import logger


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    books_folder = project_root / 'database' / 'books'

    db = RedbookDatabase()
    db.deploy()

    client = BookClient(db=db, source_folder=books_folder)
    try:
        client.ingest()
    finally:
        client.close()

    logger.info(f'Book ingestion finished from {books_folder}')


if __name__ == '__main__':
    main()
