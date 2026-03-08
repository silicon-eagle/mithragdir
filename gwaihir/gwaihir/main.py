from __future__ import annotations

from datetime import datetime
from pathlib import Path
from sys import stderr

import click
from loguru import logger

from gwaihir.db.db import RedbookDatabase
from gwaihir.processing.chunker import Chunker
from gwaihir.processing.embedding import ChunkEmbedder
from gwaihir.retriever.text_client import TextClient
from gwaihir.retriever.tolkien_gateway_client import TolkienGatewayClient

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / 'database' / 'redbook.db'
DEFAULT_LOG_DIR = PROJECT_ROOT / '.log'
DEFAULT_WIKI_BASE_URL = 'https://tolkiengateway.net'
DEFAULT_TEXT_SOURCE_FOLDER = PROJECT_ROOT / 'database' / 'texts'

DEFAULT_WIKI_CLIENT_BATCH_SIZE = 50
DEFAULT_WIKI_TIMEOUT_SECONDS = 30.0
DEFAULT_INDEX_BATCH_SIZE = 250
DEFAULT_INDEX_PAUSE_SECONDS = 0.5
DEFAULT_CRAWL_PAUSE_SECONDS = 0.5
DEFAULT_CRAWL_NR_ATTEMPTS = 2
DEFAULT_CRAWL_RETRY_SLEEP_SECONDS = 30.0

DEFAULT_CHUNK_SIZE = 1000
DEFAULT_CHUNK_OVERLAP = 200


def _default_logger_format() -> str:
    return (
        '<green>{time:YYYY-MM-DD HH:mm:ss}</green> | '
        '<level>{level: <8}</level> | '
        '<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - '
        '<level>{message}</level>'
    )


def _setup_logger(level: str, log_dir: Path, fmt: str | None = None) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f'{datetime.now().strftime("%Y-%m-%d")}_logfile.log'

    logger.remove()
    logger.add(stderr, format=fmt or _default_logger_format(), level=level, colorize=True)
    logger.add(log_file, rotation='10 MB', level=level, format=fmt or _default_logger_format(), enqueue=True, backtrace=True, diagnose=True)


@click.command()
@click.option(
    '--step',
    'steps',
    multiple=True,
    type=click.Choice(['read-data', 'chunk', 'encode'], case_sensitive=False),
    help='Pipeline stage(s) to run. If omitted, all stages run in order.',
)
@click.option('--db-path', type=click.Path(path_type=Path), default=DEFAULT_DB_PATH, show_default=True)
@click.option('--log-level', default='DEBUG', show_default=True)
@click.option('--log-dir', type=click.Path(path_type=Path), default=DEFAULT_LOG_DIR, show_default=True)
@click.option('--wiki-base-url', default=DEFAULT_WIKI_BASE_URL, show_default=True)
@click.option('--wiki-client-batch-size', default=DEFAULT_WIKI_CLIENT_BATCH_SIZE, type=int, show_default=True)
@click.option('--wiki-timeout-seconds', default=DEFAULT_WIKI_TIMEOUT_SECONDS, type=float, show_default=True)
@click.option('--index-limit', type=int, default=None, help='Optional max number of wiki index entries to fetch.')
@click.option('--index-batch-size', default=DEFAULT_INDEX_BATCH_SIZE, type=int, show_default=True)
@click.option('--index-pause-seconds', default=DEFAULT_INDEX_PAUSE_SECONDS, type=float, show_default=True)
@click.option('--index-nr-attempts', default=DEFAULT_CRAWL_NR_ATTEMPTS, type=int, show_default=True)
@click.option('--index-retry-sleep-seconds', default=5.0, type=float, show_default=True)
@click.option('--crawl-limit', type=int, default=None, help='Optional max number of pages to crawl when no external index is provided.')
@click.option('--crawl-pause-seconds', default=DEFAULT_CRAWL_PAUSE_SECONDS, type=float, show_default=True)
@click.option('--crawl-nr-attempts', default=DEFAULT_CRAWL_NR_ATTEMPTS, type=int, show_default=True)
@click.option('--crawl-retry-sleep-seconds', default=DEFAULT_CRAWL_RETRY_SLEEP_SECONDS, type=float, show_default=True)
@click.option('--text-source-folder', type=click.Path(path_type=Path), default=DEFAULT_TEXT_SOURCE_FOLDER, show_default=True)
@click.option('--text-index-filename', default='index.csv', show_default=True)
@click.option('--text-batch-size', default=10, type=int, show_default=True)
@click.option('--text-limit', type=int, default=None, help='Optional max number of text files to ingest from index.')
@click.option('--text-pause-seconds', default=0.0, type=float, show_default=True)
@click.option('--chunk-size', default=DEFAULT_CHUNK_SIZE, type=int, show_default=True)
@click.option('--chunk-overlap', default=DEFAULT_CHUNK_OVERLAP, type=int, show_default=True)
@click.option('--clear-existing-chunks/--no-clear-existing-chunks', default=True, show_default=True)
@click.option('--encode-document-id', type=int, default=None, help='Optional document_id filter when encoding chunks.')
@click.option('--encode-batch-size', default=32, type=int, show_default=True)
@click.option('--qdrant-url', default=None, help='Qdrant URL. If omitted, QDRANT_URL env var is used.')
@click.option('--qdrant-api-key', default=None, help='Optional Qdrant API key. If omitted, QDRANT_API_KEY env var is used.')
@click.option('--qdrant-collection-name', default='gwaihir_chunks', show_default=True)
@click.option('--dense-model-name', default='google/embeddinggemma-300m', show_default=True)
@click.option('--sparse-model-name', default='Qdrant/bm25', show_default=True)
@click.option('--dense-vector-name', default='dense', show_default=True)
@click.option('--sparse-vector-name', default='sparse', show_default=True)
def cli(
    steps: tuple[str, ...],
    db_path: Path,
    log_level: str,
    log_dir: Path,
    wiki_base_url: str,
    wiki_client_batch_size: int,
    wiki_timeout_seconds: float,
    index_limit: int | None,
    index_batch_size: int,
    index_pause_seconds: float,
    index_nr_attempts: int,
    index_retry_sleep_seconds: float,
    crawl_limit: int | None,
    crawl_pause_seconds: float,
    crawl_nr_attempts: int,
    crawl_retry_sleep_seconds: float,
    text_source_folder: Path,
    text_index_filename: str,
    text_batch_size: int,
    text_limit: int | None,
    text_pause_seconds: float,
    chunk_size: int,
    chunk_overlap: int,
    clear_existing_chunks: bool,
    encode_document_id: int | None,
    encode_batch_size: int,
    qdrant_url: str | None,
    qdrant_api_key: str | None,
    qdrant_collection_name: str,
    dense_model_name: str,
    sparse_model_name: str,
    dense_vector_name: str,
    sparse_vector_name: str,
) -> None:
    """Run data ingestion, chunking, and embedding pipeline stages."""
    _setup_logger(level=log_level, log_dir=log_dir)

    selected_steps = [step.lower() for step in steps] if steps else ['read-data', 'chunk', 'encode']
    logger.info(f'Running pipeline steps: {selected_steps}')

    db = RedbookDatabase(db_path=db_path)
    db.deploy()

    try:
        if 'read-data' in selected_steps:
            wiki_client = TolkienGatewayClient(
                base_url=wiki_base_url,
                db=db,
                batch_size=wiki_client_batch_size,
                timeout_seconds=wiki_timeout_seconds,
            )
            text_client = TextClient(
                db=db,
                source_folder=text_source_folder,
                index_filename=text_index_filename,
                batch_size=text_batch_size,
            )
            try:
                index = wiki_client.get_index(
                    limit=index_limit,
                    batch_size=index_batch_size,
                    pause_seconds=index_pause_seconds,
                    nr_attempts=index_nr_attempts,
                    retry_sleep_seconds=index_retry_sleep_seconds,
                )
                wiki_client.crawl(
                    index=index,
                    limit=crawl_limit,
                    pause_seconds=crawl_pause_seconds,
                    nr_attempts=crawl_nr_attempts,
                    retry_sleep_seconds=crawl_retry_sleep_seconds,
                )
                text_client.ingest(limit=text_limit, pause_seconds=text_pause_seconds)
            finally:
                text_client.close()
                wiki_client.close()

        if 'chunk' in selected_steps:
            chunker = Chunker(db=db, chunk_size=chunk_size, chunk_overlap=chunk_overlap)
            if clear_existing_chunks:
                removed = chunker.clear_chunks()
                logger.info(f'Removed {removed} existing chunks')
            processed_documents, inserted_chunks = chunker.chunk_documents()
            logger.info(f'Processed {processed_documents} documents')
            logger.info(f'Inserted {inserted_chunks} chunks')

        if 'encode' in selected_steps:
            embedder = ChunkEmbedder(
                db=db,
                dense_model_name=dense_model_name,
                dense_vector_name=dense_vector_name,
                sparse_model_name=sparse_model_name,
                sparse_vector_name=sparse_vector_name,
                collection_name=qdrant_collection_name,
                qdrant_url=qdrant_url,
                qdrant_api_key=qdrant_api_key,
            )
            upserted = embedder.encode_and_upsert_hybrid_chunks(
                document_id=encode_document_id,
                batch_size=encode_batch_size,
            )
            logger.info(f'Upserted {upserted} chunk vectors to qdrant collection {qdrant_collection_name}')
    finally:
        db.close()


if __name__ == '__main__':
    cli()
