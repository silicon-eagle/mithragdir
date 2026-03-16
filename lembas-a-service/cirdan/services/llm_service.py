from __future__ import annotations

from collections.abc import AsyncGenerator


class LLMService:
    async def generate_embedding(self, text: str) -> list[float]:
        raise NotImplementedError('Embedding integration is not implemented yet.')

    async def stream_chat(self, system_prompt: str, user_message: str) -> AsyncGenerator[str]:
        raise NotImplementedError('Streaming chat integration is not implemented yet.')
