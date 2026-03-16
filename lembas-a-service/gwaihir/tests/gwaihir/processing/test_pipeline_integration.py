from __future__ import annotations

import uuid
from pathlib import Path

import pytest
from gwaihir.db.db import RedbookDatabase
from gwaihir.processing.chunker import Chunker
from gwaihir.processing.embedding import ChunkEmbedder
from gwaihir.retriever.tolkien_gateway_client import TolkienGatewayClient
from qdrant_client import models as qdrant_models


@pytest.fixture
def db(tmp_path: Path) -> RedbookDatabase:
    database = RedbookDatabase(db_path=tmp_path / 'test_pipeline_integration.db')
    database.deploy()
    return database


@pytest.mark.slow
@pytest.mark.llm
def test_gandalf_page_full_pipeline_chunk_and_encode(db: RedbookDatabase) -> None:
    """Run a full pipeline for one Tolkien Gateway page using a temporary database."""
    client = TolkienGatewayClient(
        base_url='https://tolkiengateway.net',
        db=db,
        batch_size=1,
        timeout_seconds=30,
    )

    page = client.get_page('Gandalf')
    document_id = db.insert_document(page)

    assert document_id > 0
    assert db.document_count() == 1

    chunker = Chunker(db=db, chunk_size=1000, chunk_overlap=200)
    processed_documents, inserted_chunks = chunker.chunk_documents(show_progress=False)

    assert processed_documents == 1
    assert inserted_chunks > 0

    chunks = db.get_chunks(document_id=document_id)
    assert len(chunks) == inserted_chunks
    assert chunks[0].meta_data['content_type'] == 'html'
    assert chunks[0].meta_data['chunk_method'] == 'html_cleaned_recursive_character'
    assert chunks[0].content

    collection_name = f'gwaihir_integration_{uuid.uuid4().hex[:8]}'
    embedder = ChunkEmbedder(db=db, collection_name=collection_name)

    try:
        upserted_points = embedder.encode_and_upsert_hybrid_chunks(
            document_id=document_id,
            batch_size=16,
            show_progress=False,
        )

        assert upserted_points == inserted_chunks

        count_result = embedder.qdrant_client.count(
            collection_name=collection_name,
            count_filter=qdrant_models.Filter(
                must=[
                    qdrant_models.FieldCondition(
                        key='document_id',
                        match=qdrant_models.MatchValue(value=document_id),
                    )
                ]
            ),
            exact=True,
        )
        assert count_result.count == inserted_chunks

        points, _ = embedder.qdrant_client.scroll(
            collection_name=collection_name,
            limit=1,
            with_payload=True,
            with_vectors=False,
            scroll_filter=qdrant_models.Filter(
                must=[
                    qdrant_models.FieldCondition(
                        key='document_id',
                        match=qdrant_models.MatchValue(value=document_id),
                    )
                ]
            ),
        )
        assert points
        payload = points[0].payload
        assert payload is not None
        assert payload['document_id'] == document_id
        assert isinstance(payload['meta_data'], dict)
        assert payload['meta_data']['content_type'] == 'html'
    finally:
        embedder.qdrant_client.delete_collection(collection_name=collection_name)
