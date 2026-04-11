from __future__ import annotations

import os
from collections.abc import Sequence

import click
from core.config import (
    DEFAULT_DENSE_MODEL,
    DEFAULT_DENSE_VECTOR_NAME,
    DEFAULT_LATE_INTERACTION_MODEL,
    DEFAULT_LATE_INTERACTION_VECTOR_NAME,
    DEFAULT_QDRANT_COLLECTION,
    DEFAULT_QDRANT_TIMEOUT_SECONDS,
    DEFAULT_SPARSE_MODEL,
    DEFAULT_SPARSE_VECTOR_NAME,
    DEFAULT_UPSERT_SLICE_SIZE,
)
from core.db import RedbookDatabase
from core.models import Chunk
from fastembed import LateInteractionTextEmbedding, SparseTextEmbedding
from loguru import logger
from qdrant_client import QdrantClient, models
from qdrant_client.http.exceptions import UnexpectedResponse
from qdrant_client.models import (
    Distance,
    HnswConfigDiff,
    MultiVectorComparator,
    MultiVectorConfig,
    PointStruct,
    SparseVector,
    SparseVectorParams,
    VectorParams,
)
from sentence_transformers import SentenceTransformer


class ChunkEmbedder:
    """Simple embedder for dense, sparse (BM25), and late-interaction indexing in Qdrant."""

    def __init__(
        self,
        db: RedbookDatabase,
        dense_model_name: str = DEFAULT_DENSE_MODEL,
        dense_vector_name: str = DEFAULT_DENSE_VECTOR_NAME,
        sparse_model_name: str = DEFAULT_SPARSE_MODEL,
        sparse_vector_name: str = DEFAULT_SPARSE_VECTOR_NAME,
        collection_name: str = DEFAULT_QDRANT_COLLECTION,
        qdrant_url: str | None = None,
        late_interaction_model_name: str = DEFAULT_LATE_INTERACTION_MODEL,
        late_interaction_vector_name: str = DEFAULT_LATE_INTERACTION_VECTOR_NAME,
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
            late_interaction_model_name: Default ColBERT-style model
                identifier.
            late_interaction_vector_name: Name of the late-interaction vector
                field in Qdrant.
        """
        self.db = db
        self.dense_model_name = dense_model_name
        self.dense_vector_name = dense_vector_name

        self.sparse_model_name = sparse_model_name
        self.sparse_vector_name = sparse_vector_name

        self.late_interaction_model_name = late_interaction_model_name
        self.late_interaction_vector_name = late_interaction_vector_name

        if qdrant_url is None and os.getenv('QDRANT_URL') is None:
            raise ValueError('Qdrant URL must be provided via argument or QDRANT_URL env var.')
        self.qdrant_url = qdrant_url or os.getenv('QDRANT_URL')
        self.collection_name = collection_name
        self._dense_model: SentenceTransformer | None = None
        self._sparse_model: SparseTextEmbedding | None = None
        self._late_interaction_model: LateInteractionTextEmbedding | None = None
        self._qdrant_client: QdrantClient | None = None

    @property
    def qdrant_client(self) -> QdrantClient:
        """Return a lazily initialized Qdrant client."""
        if self._qdrant_client is None:
            if self.qdrant_url == ':memory:':
                self._qdrant_client = QdrantClient(location=':memory:')
            else:
                self._qdrant_client = QdrantClient(url=self.qdrant_url, timeout=DEFAULT_QDRANT_TIMEOUT_SECONDS)
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

    @property
    def late_interaction_model(self) -> LateInteractionTextEmbedding:
        """Return the configured late-interaction model, loading it on first use."""
        if self._late_interaction_model is None:
            self._late_interaction_model = LateInteractionTextEmbedding(model_name=self.late_interaction_model_name)
            logger.info(f'Loaded late-interaction model: {self.late_interaction_model_name}')
        return self._late_interaction_model

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

    def encode_texts_late_interaction(
        self,
        texts: Sequence[str],
        batch_size: int = 32,
    ) -> list[list[list[float]]]:
        """Encode text strings into token-level vectors for late interaction."""
        if not texts:
            return []

        vectors = self.late_interaction_model.embed(list(texts), batch_size=batch_size)
        return [[[float(value) for value in token_vector] for token_vector in vector] for vector in vectors]

    def create_collection(
        self,
        dense_vector_size: int | None = None,
        late_interaction_vector_size: int | None = None,
    ) -> None:
        """Ensure Qdrant collection exists for dense, sparse, and late-interaction retrieval."""
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
        except ValueError:
            # Local Qdrant raises ValueError if collection not found
            pass

        if dense_vector_size is None:
            probe = self.encode_texts_dense(['dimension probe'])
            if not probe:
                msg = 'Could not infer vector size from dense model.'
                raise RuntimeError(msg)
            dense_vector_size = len(probe[0])

        vectors_config: dict[str, VectorParams] = {self.dense_vector_name: VectorParams(size=dense_vector_size, distance=Distance.COSINE)}

        if late_interaction_vector_size is None:
            late_probe = self.encode_texts_late_interaction(['dimension probe'])
            if not late_probe or not late_probe[0]:
                msg = 'Could not infer vector size from late-interaction model.'
                raise RuntimeError(msg)
            late_interaction_vector_size = len(late_probe[0][0])

        vectors_config[self.late_interaction_vector_name] = VectorParams(
            size=late_interaction_vector_size,
            distance=Distance.COSINE,
            multivector_config=MultiVectorConfig(comparator=MultiVectorComparator.MAX_SIM),
            hnsw_config=HnswConfigDiff(m=0),
        )

        self.qdrant_client.create_collection(
            collection_name=collection_name,
            vectors_config=vectors_config,
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
        except ValueError:
            # Local Qdrant raises ValueError if collection not found
            pass

        self.create_collection()

    def encode_and_upsert_hybrid_chunks(
        self,
        document_id: int | None = None,
        batch_size: int = 32,
        show_progress: bool = True,
    ) -> int:
        """Read DB chunks, encode retrieval vectors, and upsert points.

        Args:
            document_id: Optional document id filter.
            batch_size: Embedding batch size.
            show_progress: Whether to render a terminal progress bar.
        """
        document_ids = self.db.get_document_ids_with_chunks(document_id=document_id)
        total_documents = len(document_ids)
        scope = f'document_id={document_id}' if document_id is not None else 'all documents'
        logger.info(f'Encoding and upserting chunks for {total_documents} documents ({scope})')

        if not document_ids:
            return 0

        upserted_total = 0
        self.create_collection()

        def upsert_document(document_chunks: Sequence[Chunk]) -> int:
            if not document_chunks:
                return 0
            dense_vectors = self.encode_texts_dense(
                texts=[str(chunk.content) for chunk in document_chunks],
                batch_size=batch_size,
            )
            sparse_vectors = self.encode_texts_sparse(
                texts=[str(chunk.content) for chunk in document_chunks],
                batch_size=batch_size,
            )
            late_interaction_vectors = self.encode_texts_late_interaction(
                texts=[str(chunk.content) for chunk in document_chunks],
                batch_size=batch_size,
            )

            points = [
                PointStruct(
                    id=int(chunk.get_id()),
                    vector={
                        self.dense_vector_name: dense_vectors[index],
                        self.sparse_vector_name: sparse_vectors[index],
                        self.late_interaction_vector_name: late_interaction_vectors[index],
                    },
                    payload=chunk.build_metadata_payload(),
                )
                for index, chunk in enumerate(document_chunks)
            ]

            # Keep each upsert request small to reduce timeout risk on large runs.
            upsert_slice_size = min(batch_size, DEFAULT_UPSERT_SLICE_SIZE)
            for start in range(0, len(points), upsert_slice_size):
                point_batch = points[start : start + upsert_slice_size]
                self.qdrant_client.upsert(collection_name=self.collection_name, points=point_batch)
            return len(points)

        if show_progress:
            with click.progressbar(document_ids, label='Encoding documents', show_pos=True) as progress_documents:
                for doc_id in progress_documents:
                    document_chunks = self.db.get_chunks(document_id=doc_id)
                    upserted_total += upsert_document(document_chunks)
        else:
            for doc_id in document_ids:
                document_chunks = self.db.get_chunks(document_id=doc_id)
                upserted_total += upsert_document(document_chunks)

        logger.info(f'Encoding finished: upserted_hybrid_vectors={upserted_total}, documents={total_documents}, collection={self.collection_name}')
        return upserted_total
