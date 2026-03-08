from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, cast

from loguru import logger
from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels
from qdrant_client.http.exceptions import UnexpectedResponse
from sentence_transformers import SentenceTransformer

from gwaihir.db.db import RedbookDatabase
from gwaihir.db.models import Chunk, EncodedChunk

DEFAULT_EMBEDDING_MODEL = 'google/embeddinggemma-300m'
DEFAULT_QDRANT_COLLECTION = 'gwaihir_chunks'


class SupportsToList(Protocol):
    def tolist(self) -> Sequence[float]: ...


class ChunkEmbedder:
    def __init__(
        self,
        db: RedbookDatabase,
        model_name: str = DEFAULT_EMBEDDING_MODEL,
        qdrant_url: str = 'http://localhost:6333',
        qdrant_api_key: str | None = None,
    ) -> None:
        """Initialize embedding and vector-store clients.

        Args:
            db: Database used to read chunk records.
            model_name: SentenceTransformer model identifier.
            qdrant_url: Qdrant HTTP endpoint.
            qdrant_api_key: Optional API key for Qdrant.
        """
        self.db = db
        self.model_name = model_name
        self.qdrant_url = qdrant_url
        self.qdrant_api_key = qdrant_api_key
        self._model: SentenceTransformer | None = None
        self._qdrant_client: QdrantClient | None = None

    @property
    def model(self) -> SentenceTransformer:
        """Return a lazily initialized embedding model instance.

        Returns:
            Loaded SentenceTransformer model.
        """
        if self._model is None:
            self._model = SentenceTransformer(self.model_name)
            logger.info(f'Loaded embedding model: {self.model_name}')
        return self._model

    @property
    def qdrant_client(self) -> QdrantClient:
        """Return a lazily initialized Qdrant client.

        Returns:
            Configured Qdrant client.
        """
        if self._qdrant_client is None:
            self._qdrant_client = QdrantClient(url=self.qdrant_url, api_key=self.qdrant_api_key)
        return self._qdrant_client

    def get_chunks(self, document_id: int | None = None) -> list[Chunk]:
        """Fetch chunks from the database.

        Args:
            document_id: Optional document id filter.

        Returns:
            List of chunks ordered by document/chunk index.
        """
        return self.db.get_chunks(document_id=document_id)

    def encode_texts(self, texts: Sequence[str], batch_size: int = 32) -> list[list[float]]:
        """Encode raw text strings into dense vectors.

        Args:
            texts: Text inputs to embed.
            batch_size: Number of texts per model batch.

        Returns:
            List of embedding vectors as float lists.
        """
        if not texts:
            return []

        vectors = self.model.encode(
            list(texts),
            batch_size=batch_size,
            show_progress_bar=False,
            normalize_embeddings=True,
        )
        return [self._coerce_vector(vector) for vector in vectors]

    def encode_chunks(self, chunks: Sequence[Chunk], batch_size: int = 32) -> list[EncodedChunk]:
        """Encode chunk records and attach Qdrant payload metadata.

        Args:
            chunks: Chunk records to embed.
            batch_size: Number of chunks per model batch.

        Returns:
            Embedded chunk objects with vectors and payloads.
        """
        if not chunks:
            return []

        texts = [chunk.content for chunk in chunks]
        vectors = self.encode_texts(texts=texts, batch_size=batch_size)

        encoded_chunks: list[EncodedChunk] = []
        for chunk, vector in zip(chunks, vectors, strict=True):
            payload = self._build_payload(chunk=chunk)
            encoded_chunks.append(
                EncodedChunk(
                    chunk_id=chunk.id,
                    document_id=chunk.document_id,
                    chunk_index=chunk.chunk_index,
                    vector=vector,
                    payload=payload,
                )
            )

        return encoded_chunks

    def encode_db_chunks(self, document_id: int | None = None, batch_size: int = 32) -> list[EncodedChunk]:
        """Load chunks from DB and encode them.

        Args:
            document_id: Optional document id filter.
            batch_size: Number of chunks per model batch.

        Returns:
            Embedded chunk objects.
        """
        chunks = self.get_chunks(document_id=document_id)
        return self.encode_chunks(chunks=chunks, batch_size=batch_size)

    def ensure_collection(self, collection_name: str = DEFAULT_QDRANT_COLLECTION, vector_size: int | None = None) -> None:
        """Ensure the target Qdrant collection exists.

        Args:
            collection_name: Collection to fetch/create.
            vector_size: Optional vector dimensionality override.
        """
        if vector_size is None:
            probe_vector = self.encode_texts(['dimension probe'])
            if not probe_vector:
                msg = 'Could not infer vector size from embedding model.'
                raise RuntimeError(msg)
            vector_size = len(probe_vector[0])

        try:
            self.qdrant_client.get_collection(collection_name=collection_name)
            logger.debug(f'Qdrant collection already exists: {collection_name}')
            return
        except UnexpectedResponse as exc:
            if exc.status_code != 404:
                raise
            logger.info(f'Creating Qdrant collection: {collection_name} (vector_size={vector_size})')

        self.qdrant_client.create_collection(
            collection_name=collection_name,
            vectors_config=qmodels.VectorParams(size=vector_size, distance=qmodels.Distance.COSINE),
        )

    def upsert_encoded_chunks(
        self,
        encoded_chunks: Sequence[EncodedChunk],
        collection_name: str = DEFAULT_QDRANT_COLLECTION,
        create_collection: bool = True,
    ) -> int:
        """Upsert pre-encoded chunks into Qdrant.

        Args:
            encoded_chunks: Chunks that already include vectors.
            collection_name: Target Qdrant collection.
            create_collection: Whether to auto-create collection when missing.

        Returns:
            Number of upserted vector points.
        """
        if not encoded_chunks:
            return 0

        if create_collection:
            vector_size = len(encoded_chunks[0].vector)
            self.ensure_collection(collection_name=collection_name, vector_size=vector_size)

        points = [qmodels.PointStruct(id=chunk.chunk_id, vector=chunk.vector, payload=chunk.payload) for chunk in encoded_chunks]

        self.qdrant_client.upsert(collection_name=collection_name, points=points)
        logger.info(f'Upserted {len(points)} chunk vectors to Qdrant collection={collection_name}')
        return len(points)

    def encode_and_upsert_chunks(
        self,
        collection_name: str = DEFAULT_QDRANT_COLLECTION,
        document_id: int | None = None,
        batch_size: int = 32,
        create_collection: bool = True,
    ) -> int:
        """Encode DB chunks and upsert them to Qdrant.

        Args:
            collection_name: Target Qdrant collection.
            document_id: Optional document id filter.
            batch_size: Number of chunks per embedding batch.
            create_collection: Whether to auto-create collection when missing.

        Returns:
            Number of upserted vector points.
        """
        encoded_chunks = self.encode_db_chunks(document_id=document_id, batch_size=batch_size)
        return self.upsert_encoded_chunks(
            encoded_chunks=encoded_chunks,
            collection_name=collection_name,
            create_collection=create_collection,
        )

    def _build_payload(self, chunk: Chunk) -> dict[str, object]:
        """Build a Qdrant payload dictionary for a chunk.

        Args:
            chunk: Chunk record to serialize.

        Returns:
            Payload dictionary stored with the vector.
        """
        return {
            'chunk_id': chunk.id,
            'document_id': chunk.document_id,
            'chunk_index': chunk.chunk_index,
            'content': chunk.content,
            'token_count': chunk.token_count,
            'created_at': chunk.created_at,
            'meta_data': chunk.meta_data,
        }

    def _coerce_vector(self, vector: Sequence[float] | SupportsToList) -> list[float]:
        """Convert an embedding output value into a float list.

        Args:
            vector: Iterable or tolist-capable embedding output.

        Returns:
            Normalized list of floats.
        """
        values = list(vector) if isinstance(vector, Sequence) else cast(SupportsToList, vector).tolist()
        return [float(value) for value in values]
