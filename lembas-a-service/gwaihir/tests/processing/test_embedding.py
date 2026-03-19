from __future__ import annotations

import uuid
from pathlib import Path

import pytest
from gwaihir.processing.embedding import ChunkEmbedder
from lembas_core.db import RedbookDatabase
from lembas_core.schemas import Text


@pytest.fixture
def db(tmp_path: Path) -> RedbookDatabase:
    database = RedbookDatabase(db_path=tmp_path / 'test_embedding.db')
    return database


class TestChunkEmbedder:
    def test_encode_methods_return_empty_for_empty_input(self, db: RedbookDatabase) -> None:
        embedder = ChunkEmbedder(db=db, qdrant_url=':memory:')

        assert embedder.encode_texts_dense([]) == []
        assert embedder.encode_texts_sparse([]) == []

    @pytest.mark.slow
    @pytest.mark.llm
    def test_encode_and_upsert_hybrid_chunks_upserts_points(self, db: RedbookDatabase) -> None:
        document_id = db.insert_text(
            Text(
                title='The Hobbit',
                content='In a hole in the ground there lived a hobbit.',
                author='J.R.R. Tolkien',
                source_path='/tmp/the_hobbit.txt',
                file_format='txt',
            )
        )
        db.insert_chunk(
            document_id=document_id,
            chunk_index=0,
            content='In a hole in the ground there lived a hobbit.',
            token_count=10,
            meta_data={'title': 'The Hobbit'},
        )

        collection_name = f'gwaihir_test_{uuid.uuid4().hex[:8]}'
        embedder = ChunkEmbedder(
            db=db,
            collection_name=collection_name,
            qdrant_url=':memory:',
        )

        # Mock qdrant client to avoid connection error in reset_collection

        inserted = embedder.encode_and_upsert_hybrid_chunks(document_id=document_id, batch_size=8)

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

        embedder.qdrant_client.delete_collection(collection_name=collection_name)
