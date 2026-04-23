from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import click
from core.db import RedbookDatabase
from dotenv import load_dotenv
from loguru import logger

from pipeline.processing.chunker import Chunker, ChunkUnit
from pipeline.processing.embedding import ChunkEmbedder
from pipeline.retriever.text_client import TextClient
from pipeline.retriever.tolkien_gateway_client import TolkienGatewayClient

PROJECT_ROOT = Path(__file__).resolve().parents[3]
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

DEFAULT_CHUNK_SIZE = 512
DEFAULT_CHUNK_OVERLAP = 64
DEFAULT_CHUNK_UNIT = 'tokens'
DEFAULT_CHUNK_TOKENIZER_NAME = 'google/embeddinggemma-300m'


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
    logger.add(log_file, rotation='10 MB', level=level, format=fmt or _default_logger_format(), enqueue=True, backtrace=True, diagnose=True)


@click.group()
@click.option('--db-url', default=None, envvar='DATABASE_URL', help='Postgres database URL. Defaults to DATABASE_URL.')
@click.option('--dev', is_flag=True, help='Use DEV environment variables.')
@click.option('--log-level', default='INFO', show_default=True)
@click.option('--log-dir', type=click.Path(path_type=Path), default=DEFAULT_LOG_DIR, show_default=True)
@click.pass_context
def cli(ctx: click.Context, db_url: str | None, dev: bool, log_level: str, log_dir: Path) -> None:
    """Pipeline: Data ingestion, chunking, and embedding pipeline."""
    load_dotenv(dotenv_path=PROJECT_ROOT / '.env')
    _setup_logger(level=log_level, log_dir=log_dir)

    env_var = 'DEV_DATABASE_URL' if dev else 'DATABASE_URL'
    resolved_db_url = db_url or os.getenv(env_var)

    ctx.ensure_object(dict)
    ctx.obj['db_url'] = resolved_db_url
    ctx.obj['db'] = RedbookDatabase(db_url=resolved_db_url)
    ctx.obj['dev'] = dev
    ctx.obj['db'].deploy()

    # Display database connection info to terminal
    if resolved_db_url:
        # Parse and show host/database from URL.
        parsed = urlparse(resolved_db_url)
        db_name = parsed.path.lstrip('/')
        if parsed.hostname and parsed.port and db_name:
            click.echo(f'Database: {parsed.hostname}:{parsed.port}/{db_name}', err=True)
        else:
            click.echo('Database connected via --db-url', err=True)
    else:
        click.echo(f'Database connected via {env_var} environment variable', err=True)


@cli.command(name='wiki')
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
@click.option('--progress/--no-progress', default=True, show_default=True, help='Show progress output.')
@click.pass_obj
def wiki_cmd(
    obj: dict,
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
    progress: bool,
) -> None:
    """Crawl wiki and ingest text data into the database."""
    db = obj['db']

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
            show_progress=progress,
        )
        wiki_client.crawl(
            index=index,
            limit=crawl_limit,
            pause_seconds=crawl_pause_seconds,
            nr_attempts=crawl_nr_attempts,
            retry_sleep_seconds=crawl_retry_sleep_seconds,
            show_progress=progress,
        )
        text_client.ingest(limit=text_limit, pause_seconds=text_pause_seconds)
    finally:
        text_client.close()
        wiki_client.close()
    click.echo('Wiki crawl and text ingest completed.', err=True)


@cli.command(name='pipeline')
@click.option('--clear/--no-clear', default=False, show_default=True, help='Clear chunks in PostgreSQL and reset embeddings in Qdrant.')
@click.option('--run/--no-run', default=False, show_default=True, help='Run chunking and embedding pipeline.')
@click.option('--chunk-size', default=DEFAULT_CHUNK_SIZE, type=int, show_default=True)
@click.option('--chunk-overlap', default=DEFAULT_CHUNK_OVERLAP, type=int, show_default=True)
@click.option('--chunk-unit', type=click.Choice(['characters', 'tokens']), default=DEFAULT_CHUNK_UNIT, show_default=True)
@click.option('--chunk-tokenizer-name', default=DEFAULT_CHUNK_TOKENIZER_NAME, show_default=True)
@click.option('--encode-document-id', type=int, default=None, help='Optional document_id filter when encoding chunks.')
@click.option('--encode-batch-size', default=32, type=int, show_default=True)
@click.option('--qdrant-url', default=None, envvar='QDRANT_URL', help='Qdrant URL. Defaults to QDRANT_URL.')
@click.option('--qdrant-collection-name', default='gwaihir_chunks', show_default=True)
@click.option('--dense-model-name', default='google/embeddinggemma-300m', show_default=True)
@click.option('--sparse-model-name', default='Qdrant/bm25', show_default=True)
@click.option('--dense-vector-name', default='dense', show_default=True)
@click.option('--sparse-vector-name', default='sparse', show_default=True)
@click.option('--progress/--no-progress', default=True, show_default=True, help='Show progress output.')
@click.pass_obj
def pipeline_cmd(
    obj: dict,
    clear: bool,
    run: bool,
    chunk_size: int,
    chunk_overlap: int,
    chunk_unit: str,
    chunk_tokenizer_name: str,
    encode_document_id: int | None,
    encode_batch_size: int,
    qdrant_url: str | None,
    qdrant_collection_name: str,
    dense_model_name: str,
    sparse_model_name: str,
    dense_vector_name: str,
    sparse_vector_name: str,
    progress: bool,
) -> None:
    """Clear and/or run chunking and embedding pipeline."""
    db = obj['db']
    dev = obj.get('dev', False)

    qdrant_env_var = 'DEV_QDRANT_URL' if dev else 'QDRANT_URL'
    resolved_qdrant_url = qdrant_url or os.getenv(qdrant_env_var)

    if not clear and not run:
        raise click.UsageError('Specify at least one action: --clear and/or --run.')

    if clear:
        chunker = Chunker(
            db=db,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            chunk_unit=ChunkUnit(chunk_unit),
            tokenizer_name=chunk_tokenizer_name,
        )
        removed = chunker.clear_chunks()
        click.echo(f'Removed {removed} existing chunks', err=True)

        embedder = ChunkEmbedder(
            db=db,
            dense_model_name=dense_model_name,
            dense_vector_name=dense_vector_name,
            sparse_model_name=sparse_model_name,
            sparse_vector_name=sparse_vector_name,
            collection_name=qdrant_collection_name,
            qdrant_url=resolved_qdrant_url,
        )
        embedder.reset_collection()
        click.echo(f'Cleared embeddings in qdrant collection {qdrant_collection_name}', err=True)

    if run:
        chunker = Chunker(
            db=db,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            chunk_unit=ChunkUnit(chunk_unit),
            tokenizer_name=chunk_tokenizer_name,
        )
        processed_documents, inserted_chunks = chunker.chunk_documents(show_progress=progress)
        click.echo(f'Processed {processed_documents} documents', err=True)
        click.echo(f'Inserted {inserted_chunks} chunks', err=True)

        embedder = ChunkEmbedder(
            db=db,
            dense_model_name=dense_model_name,
            dense_vector_name=dense_vector_name,
            sparse_model_name=sparse_model_name,
            sparse_vector_name=sparse_vector_name,
            collection_name=qdrant_collection_name,
            qdrant_url=resolved_qdrant_url,
        )
        upserted = embedder.encode_and_upsert_hybrid_chunks(
            document_id=encode_document_id,
            batch_size=encode_batch_size,
            show_progress=progress,
        )
        click.echo(f'Upserted {upserted} chunk vectors to qdrant collection {qdrant_collection_name}', err=True)


if __name__ == '__main__':
    cli()
