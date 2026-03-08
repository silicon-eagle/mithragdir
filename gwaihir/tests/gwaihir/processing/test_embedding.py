from __future__ import annotations

import uuid
from pathlib import Path

import pytest
from gwaihir.db.db import RedbookDatabase
from gwaihir.db.models import Chunk, Text
from gwaihir.processing.embedding import ChunkEmbedder
from qdrant_client import QdrantClient
from qdrant_client.http.exceptions import ResponseHandlingException, UnexpectedResponse

TEST_MODEL = 'google/embeddinggemma-300m'
TEST_QDRANT_URL = 'http://localhost:6333'


def _ensure_dependencies() -> None:
    pytest.importorskip('sentence_transformers')
    pytest.importorskip('qdrant_client')


def _qdrant_reachable(url: str) -> bool:
    try:
        client = QdrantClient(url=url)
        client.get_collections()
        return True
    except (ResponseHandlingException, UnexpectedResponse, OSError):
        return False


@pytest.fixture
def db(tmp_path: Path) -> RedbookDatabase:
    database = RedbookDatabase(db_path=tmp_path / 'test_embedding.db')
    database.deploy()
    return database


@pytest.fixture
def embedder(db: RedbookDatabase) -> ChunkEmbedder:
    _ensure_dependencies()
    if not _qdrant_reachable(TEST_QDRANT_URL):
        pytest.skip('Qdrant is not reachable on http://localhost:6333')

    embedder = ChunkEmbedder(
        db=db,
        model_name=TEST_MODEL,
        qdrant_url=TEST_QDRANT_URL,
    )

    try:
        # Trigger model load early so tests can skip cleanly when access is missing.
        _ = embedder.model
    except Exception as exc:
        message = str(exc).lower()
        if 'gated repo' in message or '401' in message or 'unauthorized' in message or 'access to model' in message:
            pytest.skip(
                'Hugging Face access to google/embeddinggemma-300m is not configured. '
                'Set an auth token (e.g. `HF_TOKEN`) and ensure your account has access.'
            )
        raise

    return embedder


@pytest.mark.slow
@pytest.mark.llm
def test_encode_chunks_with_real_model(embedder: ChunkEmbedder) -> None:
    chunks = [
        Chunk(
            id=10,
            document_id=3,
            chunk_index=0,
            content='Aragorn son of Arathorn became king of Gondor.',
            token_count=9,
            meta_data={'source': 'book'},
            created_at='2026-03-08',
        )
    ]

    encoded = embedder.encode_chunks(chunks=chunks)

    assert len(encoded) == 1
    first = encoded[0]
    assert first.chunk_id == 10
    assert first.document_id == 3
    assert len(first.vector) > 10
    assert isinstance(first.vector[0], float)
    assert first.payload['content'] == chunks[0].content
    assert first.payload['meta_data'] == {'source': 'book'}


@pytest.mark.slow
@pytest.mark.llm
def test_encode_and_upsert_with_real_qdrant(embedder: ChunkEmbedder, db: RedbookDatabase) -> None:
    document_id = db.insert_text(
        Text(
            title='The Silmarillion',
            content='In the beginning Eru, the One, made the Ainur.',
            author='J.R.R. Tolkien',
            source_path='/tmp/silmarillion.txt',
            file_format='txt',
        )
    )
    db.insert_chunk(
        document_id=document_id,
        chunk_index=0,
        content='In the beginning Eru, the One, made the Ainur.',
        token_count=10,
        meta_data={'title': 'The Silmarillion'},
    )

    collection_name = f'gwaihir_test_{uuid.uuid4().hex[:8]}'

    inserted = embedder.encode_and_upsert_chunks(
        collection_name=collection_name,
        document_id=document_id,
        batch_size=8,
    )

    assert inserted == 1

    collection = embedder.qdrant_client.get_collection(collection_name=collection_name)
    assert collection is not None

    results = embedder.qdrant_client.scroll(
        collection_name=collection_name,
        limit=10,
        with_payload=True,
        with_vectors=False,
    )
    points = results[0]
    assert len(points) == 1
    assert points[0].payload is not None
    assert points[0].payload['document_id'] == document_id

    # Best-effort cleanup so tests do not accumulate collections locally.
    embedder.qdrant_client.delete_collection(collection_name=collection_name)
