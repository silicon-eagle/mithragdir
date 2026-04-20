from __future__ import annotations

import os
import uuid

import pytest
from core.db import RedbookDatabase
from core.schemas import Text
from pipeline.processing.embedding import ChunkEmbedder


@pytest.fixture
def db() -> RedbookDatabase:
    db_url = os.getenv('DEV_DATABASE_URL')
    if not db_url:
        pytest.skip('DEV_DATABASE_URL is required for PostgreSQL-backed tests.')

    database = RedbookDatabase(db_url=db_url)
    database.truncate_all_tables()
    return database


class TestChunkEmbedder:
    def test_encode_methods_return_empty_for_empty_input(self, db: RedbookDatabase) -> None:
        embedder = ChunkEmbedder(db=db, qdrant_url=':memory:')

        assert embedder.encode_texts_dense([]) == []
        assert embedder.encode_texts_sparse([]) == []
        assert embedder.encode_texts_late_interaction([]) == []

    def test_encode_and_upsert_hybrid_chunks_upserts_multivectors(self, db: RedbookDatabase) -> None:
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

        inserted = embedder.encode_and_upsert_hybrid_chunks(document_id=document_id, batch_size=8, show_progress=False)

        assert inserted == 1

        points, _ = embedder.qdrant_client.scroll(
            collection_name=collection_name,
            limit=10,
            with_payload=True,
            with_vectors=True,
        )
        assert len(points) == 1
        assert isinstance(points[0].vector, dict)
        assert 'dense' in points[0].vector
        assert 'sparse' in points[0].vector
        assert 'late_interaction' in points[0].vector
        late_interaction_vector = points[0].vector['late_interaction']
        assert isinstance(late_interaction_vector, list)
        expected_late_interaction = embedder.encode_texts_late_interaction(
            ['In a hole in the ground there lived a hobbit.'],
            batch_size=8,
        )[0]
        assert len(late_interaction_vector) == len(expected_late_interaction)
        token_vectors = [token_vector for token_vector in late_interaction_vector if isinstance(token_vector, list)]
        assert len(token_vectors) == len(late_interaction_vector)
        assert all(len(token_vector) == len(expected_late_interaction[0]) for token_vector in token_vectors)

        embedder.qdrant_client.delete_collection(collection_name=collection_name)

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
