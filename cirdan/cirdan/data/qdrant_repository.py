from __future__ import annotations

from qdrant_client import QdrantClient

from cirdan.domain.models import VectorHit


class QdrantRepository:
    def __init__(self, url: str, collection_name: str) -> None:
        self.url = url
        self.collection_name = collection_name
        self.client = QdrantClient(url=url)

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
