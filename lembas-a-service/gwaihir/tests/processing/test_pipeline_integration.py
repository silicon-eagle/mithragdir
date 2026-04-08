from __future__ import annotations

import os
import uuid

import pytest
from gwaihir.processing.chunker import Chunker
from gwaihir.processing.embedding import ChunkEmbedder
from lembas_core.db import RedbookDatabase
from lembas_core.schemas import Page
from qdrant_client import models as qdrant_models


@pytest.fixture
def db() -> RedbookDatabase:
    db_url = os.getenv('DEV_DATABASE_URL')
    if not db_url:
        pytest.skip('DEV_DATABASE_URL is required for PostgreSQL-backed tests.')

    database = RedbookDatabase(db_url=db_url)
    database.execute('TRUNCATE TABLE chunks, text, wiki_page, "index", document RESTART IDENTITY CASCADE')
    return database


def test_gandalf_page_full_pipeline_chunk_and_encode(db: RedbookDatabase) -> None:
    """Run a full pipeline for one Tolkien Gateway page using a temporary database."""
    qdrant_url = os.getenv('DEV_QDRANT_URL')
    if not qdrant_url:
        pytest.skip('DEV_QDRANT_URL is required for integration tests.')

    page = Page(
        title='Gandalf',
        url='https://tolkiengateway.net/wiki/Gandalf',
        pageid=123456,
        content=(
            '<div id="mw-content-text">'
            '<h2>Gandalf</h2>'
            '<p>Gandalf was a wizard of Middle-earth.</p>'
            '<p>He was a member of the Fellowship of the Ring.</p>'
            '</div>'
        ),
    )
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
    embedder = ChunkEmbedder(db=db, collection_name=collection_name, qdrant_url=qdrant_url)

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
        assert payload['content_type'] == 'html'
        assert payload['chunk_method'] == 'html_cleaned_recursive_character'
    finally:
        embedder.qdrant_client.delete_collection(collection_name=collection_name)
