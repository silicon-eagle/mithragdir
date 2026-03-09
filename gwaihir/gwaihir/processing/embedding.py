from __future__ import annotations

import os
from collections.abc import Sequence
from itertools import groupby

import click
from fastembed import SparseTextEmbedding
from loguru import logger
from qdrant_client import QdrantClient, models
from qdrant_client.http.exceptions import UnexpectedResponse
from qdrant_client.models import Distance, PointStruct, SparseVector, SparseVectorParams, VectorParams
from sentence_transformers import SentenceTransformer

from gwaihir.db.db import RedbookDatabase
from gwaihir.db.models import Chunk

DEFAULT_DENSE_MODEL = 'google/embeddinggemma-300m'
DEFAULT_SPARSE_MODEL = 'Qdrant/bm25'
DEFAULT_QDRANT_COLLECTION = 'gwaihir_chunks'
DEFAULT_DENSE_VECTOR_NAME = 'dense'
DEFAULT_SPARSE_VECTOR_NAME = 'sparse'


class ChunkEmbedder:
    """Simple embedder for dense, sparse (BM25), and hybrid indexing in Qdrant."""

    def __init__(
        self,
        db: RedbookDatabase,
        dense_model_name: str = DEFAULT_DENSE_MODEL,
        dense_vector_name: str = DEFAULT_DENSE_VECTOR_NAME,
        sparse_model_name: str = DEFAULT_SPARSE_MODEL,
        sparse_vector_name: str = DEFAULT_SPARSE_VECTOR_NAME,
        collection_name: str = DEFAULT_QDRANT_COLLECTION,
        qdrant_url: str | None = None,
        qdrant_api_key: str | None = None,
    ) -> None:
        """Initialize embedding and vector store clients.

        Args:
            db: Database used to read chunk records.
            dense_model_name: Default dense model identifier.
            dense_vector_name: Name of the dense vector field in Qdrant.
            sparse_model_name: Default sparse model identifier.
            sparse_vector_name: Name of the sparse vector field in Qdrant.
            collection_name: Qdrant collection name for stored chunk vectors.
            qdrant_url: Qdrant HTTP endpoint. If None, uses `QDRANT_URL`
                env var.
            qdrant_api_key: Optional API key for Qdrant. If None, uses
                `QDRANT_API_KEY` env var.
        """
        self.db = db
        self.dense_model_name = dense_model_name
        self.dense_vector_name = dense_vector_name

        self.sparse_model_name = sparse_model_name
        self.sparse_vector_name = sparse_vector_name

        if qdrant_url is None and os.getenv('QDRANT_URL') is None:
            raise ValueError('Qdrant URL must be provided via argument or QDRANT_URL env var.')
        self.qdrant_url = qdrant_url or os.getenv('QDRANT_URL')
        self.qdrant_api_key = qdrant_api_key or os.getenv('QDRANT_API_KEY')
        self.collection_name = collection_name
        self._dense_model: SentenceTransformer | None = None
        self._sparse_model: SparseTextEmbedding | None = None
        self._qdrant_client: QdrantClient | None = None

    @property
    def qdrant_client(self) -> QdrantClient:
        """Return a lazily initialized Qdrant client."""
        if self._qdrant_client is None:
            self._qdrant_client = QdrantClient(url=self.qdrant_url, api_key=self.qdrant_api_key)
        return self._qdrant_client

    @property
    def dense_model(self) -> SentenceTransformer:
        """Return the configured dense model, loading it on first use."""
        if self._dense_model is None:
            self._dense_model = SentenceTransformer(self.dense_model_name)
            logger.info(f'Loaded dense model: {self.dense_model_name}')
        return self._dense_model

    @property
    def sparse_model(self) -> SparseTextEmbedding:
        """Return the configured sparse model, loading it on first use."""
        if self._sparse_model is None:
            self._sparse_model = SparseTextEmbedding(model_name=self.sparse_model_name)
            logger.info(f'Loaded sparse model: {self.sparse_model_name}')
        return self._sparse_model

    def encode_texts_dense(
        self,
        texts: Sequence[str],
        batch_size: int = 32,
    ) -> list[list[float]]:
        """Encode text strings into dense vectors."""
        if not texts:
            return []

        vectors = self.dense_model.encode(
            list(texts),
            batch_size=batch_size,
            show_progress_bar=False,
            normalize_embeddings=True,
        )
        return [[float(value) for value in vector] for vector in vectors]

    def encode_texts_sparse(
        self,
        texts: Sequence[str],
        batch_size: int = 32,
    ) -> list[SparseVector]:
        """Encode text strings into sparse vectors using FastEmbed BM25."""
        if not texts:
            return []

        vectors = self.sparse_model.embed(list(texts), batch_size=batch_size)
        return [
            SparseVector(
                indices=[int(index) for index in vector.indices],
                values=[float(value) for value in vector.values],
            )
            for vector in vectors
        ]

    def create_collection(
        self,
        dense_vector_size: int | None = None,
    ) -> None:
        """Ensure Qdrant collection exists in dense or hybrid mode."""
        collection_name = self.collection_name
        try:
            self.qdrant_client.get_collection(collection_name=collection_name)
            logger.info(f'Collection already exists: {collection_name}')
            return
        except UnexpectedResponse as exc:
            if exc.status_code != 404:
                msg = f'Error checking collection existence: {exc}'
                logger.error(msg)
                raise RuntimeError(msg) from exc

        if dense_vector_size is None:
            probe = self.encode_texts_dense(['dimension probe'])
            if not probe:
                msg = 'Could not infer vector size from dense model.'
                raise RuntimeError(msg)
            dense_vector_size = len(probe[0])

        self.qdrant_client.create_collection(
            collection_name=collection_name,
            vectors_config={self.dense_vector_name: VectorParams(size=dense_vector_size, distance=Distance.COSINE)},
            sparse_vectors_config={self.sparse_vector_name: SparseVectorParams(modifier=models.Modifier.IDF)},
        )
        return

    def reset_collection(self) -> None:
        """Delete and recreate the configured Qdrant collection."""
        collection_name = self.collection_name
        try:
            self.qdrant_client.delete_collection(collection_name=collection_name)
            logger.info(f'Deleted existing collection: {collection_name}')
        except UnexpectedResponse as exc:
            if exc.status_code != 404:
                msg = f'Error deleting collection {collection_name}: {exc}'
                logger.error(msg)
                raise RuntimeError(msg) from exc

        self.create_collection()

    def encode_and_upsert_hybrid_chunks(
        self,
        document_id: int | None = None,
        batch_size: int = 32,
        show_progress: bool = True,
    ) -> int:
        """Read DB chunks, encode dense+sparse vectors, and upsert hybrid points.

        Args:
            document_id: Optional document id filter.
            batch_size: Embedding batch size.
            show_progress: Whether to render a terminal progress bar.
        """
        chunks = self.db.get_chunks(document_id=document_id)
        scope = f'document_id={document_id}' if document_id is not None else 'all documents'
        logger.info(f'Encoding and upserting {len(chunks)} chunks for {scope}')

        if not chunks:
            return 0

        chunks_by_document = {int(doc_id): list(group_chunks) for doc_id, group_chunks in groupby(chunks, key=lambda chunk: chunk.document_id)}
        total_documents = len(chunks_by_document)

        upserted_total = 0
        self.create_collection()

        items = list(chunks_by_document.items())

        def upsert_document(document_chunks: Sequence[Chunk]) -> int:
            dense_vectors = self.encode_texts_dense(
                texts=[chunk.content for chunk in document_chunks],
                batch_size=batch_size,
            )
            sparse_vectors = self.encode_texts_sparse(
                texts=[chunk.content for chunk in document_chunks],
                batch_size=batch_size,
            )

            points = [
                PointStruct(
                    id=chunk.id,
                    vector={
                        self.dense_vector_name: dense_vector,
                        self.sparse_vector_name: sparse_vector,
                    },
                    payload=chunk.build_metadata_payload(),
                )
                for chunk, dense_vector, sparse_vector in zip(document_chunks, dense_vectors, sparse_vectors, strict=True)
            ]

            # Avoid exceeding Qdrant request-size limits by upserting in slices.
            for start in range(0, len(points), batch_size):
                point_batch = points[start : start + batch_size]
                self.qdrant_client.upsert(collection_name=self.collection_name, points=point_batch)
            return len(points)

        if show_progress:
            with click.progressbar(items, label='Encoding documents', show_pos=True) as progress_documents:
                for _current_document_id, document_chunks in progress_documents:
                    upserted_total += upsert_document(document_chunks)
        else:
            for _current_document_id, document_chunks in items:
                upserted_total += upsert_document(document_chunks)

        logger.info(f'Encoding finished: upserted_hybrid_vectors={upserted_total}, documents={total_documents}, collection={self.collection_name}')
        return upserted_total
