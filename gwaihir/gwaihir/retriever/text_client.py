from __future__ import annotations

import csv
import time
from pathlib import Path

from loguru import logger

from gwaihir.db.db import RedbookDatabase
from gwaihir.db.models import Text


class TextClient:
    def __init__(
        self,
        db: RedbookDatabase,
        source_folder: str | Path,
        index_filename: str = 'index.csv',
        batch_size: int = 10,
    ) -> None:
        self.source_folder = Path(source_folder)
        self.index_path = self.source_folder / index_filename
        self.db = db
        self.batch_size = batch_size
        self._pending_books: list[Text] = []
        logger.info(f'Initialized TextClient(source_folder={self.source_folder}, index_path={self.index_path}, batch_size={self.batch_size})')

    def _iter_index_rows(self) -> list[dict[str, str]]:
        if not self.index_path.exists():
            logger.warning(f'Book index file does not exist: {self.index_path}')
            return []

        with self.index_path.open(encoding='utf-8', newline='') as file:
            reader = csv.DictReader(file, delimiter=';')
            if reader.fieldnames is None or 'file' not in reader.fieldnames:
                raise ValueError('Book index must contain a `file` column (delimiter=`;`).')

            rows: list[dict[str, str]] = []
            for row in reader:
                normalized = {key: (value.strip() if value is not None else '') for key, value in row.items() if key is not None}
                if normalized.get('file'):
                    rows.append(normalized)
            return rows

    def _resolve_index_entries(self) -> list[tuple[Path, dict[str, str]]]:
        rows = self._iter_index_rows()
        entries: list[tuple[Path, dict[str, str]]] = []
        missing_files = 0

        for row in rows:
            relative_file = row['file']
            base_path = self.source_folder / relative_file
            if base_path.suffix:
                base_path = base_path.with_suffix('')
            file_path = (base_path.parent / f'{base_path.name}.txt').resolve()
            if not file_path.exists() or not file_path.is_file():
                missing_files += 1
                logger.warning(f'Indexed file is missing and will be skipped: {file_path}')
                continue

            if file_path.suffix.lower() != '.txt':
                logger.warning(f'Indexed file has unsupported format and will be skipped: {file_path}')
                continue

            entries.append((file_path, row))

        if missing_files:
            logger.info(f'Index validation: {missing_files} indexed file(s) missing.')

        return entries

    def _extract_text(self, file_path: Path) -> str:
        suffix = file_path.suffix.lower()
        if suffix == '.txt':
            return file_path.read_text(encoding='utf-8')
        raise ValueError(f'Unsupported book format: {file_path.suffix}')

    def _build_book(self, file_path: Path, metadata: dict[str, str]) -> Text:
        logger.debug(f'Extracting text from book file: {file_path}')
        content = self._extract_text(file_path)
        title = metadata.get('title') or file_path.stem.replace('_', ' ').strip()
        source_path = str(file_path.resolve())

        published_year = None
        published_year_value = metadata.get('published_year', '')
        if published_year_value.isdigit():
            published_year = int(published_year_value)

        return Text(
            title=title,
            content=content,
            author=metadata.get('author') or 'Unknown',
            url=source_path,
            source_path=source_path,
            publisher=metadata.get('publisher') or None,
            published_year=published_year,
            isbn=metadata.get('isbn') or None,
            language=metadata.get('language') or None,
            file_format=file_path.suffix.lower().lstrip('.'),
        )

    def store_book(self, book: Text) -> None:
        self._pending_books.append(book)
        logger.debug(f'Buffered book {book.title} (pending={len(self._pending_books)}/{self.batch_size})')
        if len(self._pending_books) >= self.batch_size:
            logger.info('Book batch size reached; flushing pending books')
            self.flush()

    def flush(self) -> int:
        if not self._pending_books:
            logger.debug('Flush called with empty book buffer')
            return 0

        stored = 0
        for book in self._pending_books:
            self.db.insert_text(book)
            stored += 1

        logger.info(f'Flushed {stored} books to database')
        self._pending_books.clear()
        return stored

    def ingest(self, limit: int | None = None, pause_seconds: float = 0.0) -> int:
        entries = self._resolve_index_entries()
        if limit is not None:
            entries = entries[:limit]

        total = len(entries)
        logger.info(f'Starting local book ingestion from index for {total} files (limit={limit})')

        processed = 0
        stored = 0
        skipped = 0
        failed = 0
        try:
            for file_path, metadata in entries:
                source_path = str(file_path.resolve())
                if self.db.text_exists(source_path):
                    skipped += 1
                    processed += 1
                    logger.info(f'Book progress: {processed}/{total} (stored={stored}, skipped={skipped}, failed={failed})')
                    continue

                try:
                    book = self._build_book(file_path, metadata)
                    self.store_book(book)
                    stored += 1
                except Exception as exc:  # noqa: BLE001
                    failed += 1
                    logger.warning(f'Failed to ingest book file {file_path}: {exc}')

                processed += 1
                logger.info(f'Book progress: {processed}/{total} (stored={stored}, skipped={skipped}, failed={failed})')
                time.sleep(pause_seconds)
        finally:
            flushed = self.flush()

        logger.info(
            f'Book ingestion completed: processed={processed}, stored={stored}, skipped={skipped}, failed={failed}, flushed_remaining={flushed}'
        )
        return flushed

    def close(self) -> None:
        logger.info('Closing TextClient and flushing pending books')
        self.flush()
