from __future__ import annotations

from collections.abc import AsyncGenerator

from cirdan.domain.models import ChatChunk, ChatRequest
from cirdan.services.llm_service import LLMService
from cirdan.services.prompt_builder import build_prompt, get_system_prompt
from cirdan.services.retrieval_service import RetrievalService


class RAGOrchestrator:
    def __init__(self, retrieval_service: RetrievalService, llm_service: LLMService, default_top_k: int) -> None:
        self.retrieval_service = retrieval_service
        self.llm_service = llm_service
        self.default_top_k = default_top_k

    async def execute_chat_pipeline(self, req: ChatRequest) -> AsyncGenerator[ChatChunk]:
        top_k = req.top_k or self.default_top_k
        chunks = await self.retrieval_service.search_relevant_chunks(query=req.message, top_k=top_k)

        prompt = build_prompt(user_message=req.message, chunks=chunks)
        system_prompt = get_system_prompt()

        async for token in self.llm_service.stream_chat(system_prompt=system_prompt, user_message=prompt):
            yield ChatChunk(type='token', content=token)

        yield ChatChunk(type='sources', sources=chunks)
