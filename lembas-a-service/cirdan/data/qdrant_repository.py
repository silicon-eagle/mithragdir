from __future__ import annotations

from fastembed import LateInteractionTextEmbedding, SparseTextEmbedding
from qdrant_client import QdrantClient, models

from cirdan.domain.models import VectorHit


class QdrantRepository:
    def __init__(self, url: str, collection_name: str) -> None:
        self.url = url
        self.collection_name = collection_name
        self.client = QdrantClient(url=url)
        # Load models for hybrid search and reranking
        self.sparse_model = SparseTextEmbedding('Qdrant/bm25')
        self.late_interaction_model = LateInteractionTextEmbedding('colbert-ir/colbertv2.0')

    async def ping(self) -> bool:
        try:
            self.client.get_collections()
            return True
        except Exception:  # noqa: BLE001
            return False

    async def search(self, query_embedding: list[float], top_k: int) -> list[VectorHit]:
        points = self.client.query_points(
            collection_name=self.collection_name,
            query=query_embedding,
            limit=top_k,
            with_payload=False,
        )

        return [
            VectorHit(
                chunk_id=str(point.id),
                score=float(point.score or 0.0),
            )
            for point in points.points
        ]

    async def search_hybrid_reranked(self, query: str, dense_embedding: list[float], top_k: int) -> list[VectorHit]:
        """
        Qdrant Hybrid/Reranking methodology using LateInteractionTextEmbedding FastEmbed.
        """
        sparse_embedding = next(self.sparse_model.embed([query]))
        late_interaction_embedding = next(self.late_interaction_model.embed([query]))

        sparse_query = models.SparseVector(
            indices=sparse_embedding.indices,
            values=sparse_embedding.values,
        )

        # Uses Pre-fetch for the initial sparse + dense retrieval, then reranks with colbert (late interaction)
        points = self.client.query_points(
            collection_name=self.collection_name,
            prefetch=[
                models.Prefetch(
                    query=dense_embedding,
                    using='dense',
                    limit=top_k * 2,
                ),
                models.Prefetch(
                    query=sparse_query,
                    using='sparse',
                    limit=top_k * 2,
                ),
            ],
            query=late_interaction_embedding,
            using='colbert',  # assuming vector name is colbert
            limit=top_k,
            with_payload=False,
        )

        return [
            VectorHit(
                chunk_id=str(point.id),
                score=float(point.score or 0.0),
            )
            for point in points.points
        ]
