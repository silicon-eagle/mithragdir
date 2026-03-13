from __future__ import annotations

from cirdan.data.qdrant_repository import QdrantRepository
from cirdan.data.sqlite_repository import SQLiteRepository
from cirdan.domain.models import RetrievedChunk
from cirdan.services.llm_service import LLMService


class RetrievalService:
    def __init__(
        self,
        llm_service: LLMService,
        qdrant_repository: QdrantRepository,
        sqlite_repository: SQLiteRepository,
    ) -> None:
        self.llm_service = llm_service
        self.qdrant_repository = qdrant_repository
        self.sqlite_repository = sqlite_repository

    async def search_relevant_chunks(self, query: str, top_k: int) -> list[RetrievedChunk]:
        query_embedding = await self.llm_service.generate_embedding(query)
        vector_hits = await self.qdrant_repository.search(query_embedding=query_embedding, top_k=top_k)

        if not vector_hits:
            return []

        chunk_ids = [hit.chunk_id for hit in vector_hits]
        chunks = await self.sqlite_repository.get_chunks_by_ids(chunk_ids=chunk_ids)

        by_id = {chunk.chunk_id: chunk for chunk in chunks}
        merged: list[RetrievedChunk] = []
        for hit in vector_hits:
            chunk = by_id.get(hit.chunk_id)
            if chunk is None:
                continue
            merged.append(
                RetrievedChunk(
                    chunk_id=chunk.chunk_id,
                    content=chunk.content,
                    score=hit.score,
                    source=chunk.source,
                )
            )

        return merged
