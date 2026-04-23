import os
from functools import lru_cache
from typing import Any

from core.config import (
    DEFAULT_DENSE_MODEL,
    DEFAULT_DENSE_VECTOR_NAME,
    DEFAULT_LATE_INTERACTION_MODEL,
    DEFAULT_LATE_INTERACTION_VECTOR_NAME,
    DEFAULT_PREFETCH_LIMIT,
    DEFAULT_QDRANT_COLLECTION,
    DEFAULT_QDRANT_URL,
    DEFAULT_RETRIEVE_LIMIT,
    DEFAULT_SPARSE_MODEL,
    DEFAULT_SPARSE_VECTOR_NAME,
)
from core.db import RedbookDatabase
from core.models import Document as PeeweeDocument
from core.schemas import Document
from fastembed import LateInteractionTextEmbedding, SparseTextEmbedding
from loguru import logger
from qdrant_client import QdrantClient, models
from sentence_transformers import SentenceTransformer

from ..graph.state import GraphState
from .node import Node


@lru_cache(maxsize=1)
def get_dense_model() -> SentenceTransformer:
    return SentenceTransformer(os.getenv('DENSE_MODEL_NAME', DEFAULT_DENSE_MODEL))


@lru_cache(maxsize=1)
def get_sparse_model() -> SparseTextEmbedding:
    return SparseTextEmbedding(model_name=os.getenv('SPARSE_MODEL_NAME', DEFAULT_SPARSE_MODEL))


@lru_cache(maxsize=1)
def get_late_interaction_model() -> LateInteractionTextEmbedding:
    return LateInteractionTextEmbedding(model_name=os.getenv('LATE_INTERACTION_MODEL_NAME', DEFAULT_LATE_INTERACTION_MODEL))


class RetrieveDocumentNode(Node):
    def __init__(self, collection_name: str | None = None) -> None:
        super().__init__('retrieve_document')
        self.collection_name = collection_name or os.getenv('QDRANT_COLLECTION', DEFAULT_QDRANT_COLLECTION)
        self.dense_vector_name = os.getenv('QDRANT_DENSE_VECTOR_NAME', DEFAULT_DENSE_VECTOR_NAME)
        self.sparse_vector_name = os.getenv('QDRANT_SPARSE_VECTOR_NAME', DEFAULT_SPARSE_VECTOR_NAME)
        self.late_interaction_vector_name = os.getenv('QDRANT_LATE_INTERACTION_VECTOR_NAME', DEFAULT_LATE_INTERACTION_VECTOR_NAME)

        self.retrieve_limit = int(os.getenv('WORKFLOW_RETRIEVE_LIMIT', str(DEFAULT_RETRIEVE_LIMIT)))
        self.prefetch_limit = int(os.getenv('WORKFLOW_RETRIEVE_PREFETCH_LIMIT', str(DEFAULT_PREFETCH_LIMIT)))

    def _get_qdrant_client(self) -> QdrantClient:
        qdrant_url = os.getenv('QDRANT_URL', DEFAULT_QDRANT_URL)
        if qdrant_url == ':memory:':
            return QdrantClient(location=':memory:')
        return QdrantClient(url=qdrant_url)

    def _get_database(self) -> RedbookDatabase:
        db_url = os.getenv('DATABASE_URL')
        if not db_url:
            raise ValueError('DATABASE_URL must be set for retrieval')
        return RedbookDatabase(db_url=db_url)

    def _hydrate_documents(self, document_ids: list[int]) -> list[Document]:
        if not document_ids:
            return []

        rows = PeeweeDocument.select().where(PeeweeDocument.document_id << document_ids)
        docs_by_id = {int(row.get_id()): Document.from_peewee(row) for row in rows}

        hydrated: list[Document] = []
        seen: set[int] = set()
        for document_id in document_ids:
            if document_id in seen:
                continue
            doc = docs_by_id.get(document_id)
            if doc is None:
                continue
            seen.add(document_id)
            hydrated.append(doc)
        return hydrated

    def _log_retrieved_documents(self, documents: list[Document]) -> None:
        if not documents:
            self.logger.info('Retrieved 0 documents')
            return

        document_summaries = [
            {
                'document_id': document.document_id,
                'title': document.title,
                'url': document.url,
            }
            for document in documents
        ]
        self.logger.info(f'Retrieved {len(documents)} documents: {document_summaries}')

    async def __call__(self, state: GraphState) -> dict[str, Any]:
        """Executes the hybrid search against the vector database."""
        logger.info(f'Running {self.name} node')

        query = state.generated_query.strip() or state.query.strip()
        if not query:
            raise ValueError("Node 'retrieve_document' requires non-empty 'generated_query' or 'query'")

        try:
            dense_vector = [float(value) for value in get_dense_model().encode([query], normalize_embeddings=True)[0]]

            sparse_embedding = next(iter(get_sparse_model().embed([query])))
            sparse_vector = models.SparseVector(
                indices=[int(index) for index in sparse_embedding.indices],
                values=[float(value) for value in sparse_embedding.values],
            )

            late_vector = [[float(value) for value in token_vector] for token_vector in next(iter(get_late_interaction_model().embed([query])))]

            qdrant_client = self._get_qdrant_client()
            response = qdrant_client.query_points(
                collection_name=self.collection_name,
                prefetch=[
                    models.Prefetch(query=dense_vector, using=self.dense_vector_name, limit=self.prefetch_limit),
                    models.Prefetch(query=sparse_vector, using=self.sparse_vector_name, limit=self.prefetch_limit),
                    models.Prefetch(query=late_vector, using=self.late_interaction_vector_name, limit=self.prefetch_limit),
                ],
                query=models.FusionQuery(fusion=models.Fusion.RRF),
                limit=self.retrieve_limit,
                with_payload=True,
                with_vectors=False,
                timeout=60,
            )

            points = list(response.points) if response.points is not None else []
            document_ids: list[int] = []
            for point in points:
                payload = point.payload
                if not isinstance(payload, dict):
                    continue
                raw_document_id = payload.get('document_id')
                if not isinstance(raw_document_id, int):
                    continue
                document_ids.append(raw_document_id)

            db = self._get_database()
            try:
                documents = self._hydrate_documents(document_ids)
                self._log_retrieved_documents(documents)
            finally:
                db.close()
        except Exception as exc:  # noqa: BLE001
            self.logger.warning(f'Retrieval failed, returning empty documents: {exc}')
            documents = []

        return {'documents': documents, 'current_state': self.name}
