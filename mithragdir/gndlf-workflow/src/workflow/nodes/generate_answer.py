from typing import Any

from langchain_core.prompts import load_prompt

from ..graph.state import GraphState
from .node import Node


class GenerateAnswerNode(Node):
    def __init__(self) -> None:
        super().__init__('generate_answer')

    async def __call__(self, state: GraphState) -> dict[str, Any]:
        """Drafts the final response based strictly on the retrieved context."""
        self.logger.info(f'Running {self.name} node')

        query = state.query.strip()
        if not query:
            raise ValueError("Node 'generate_answer' requires non-empty 'query'")

        if not state.documents:
            return {
                'generation': 'I could not find enough context in the provided texts to answer that yet.',
                'current_state': self.name,
            }

        context_blocks = []
        for document in state.documents:
            snippet = document.raw_content.strip()[:1800]
            context_blocks.append(f'Title: {document.title}\nURL: {document.url or ""}\nContent:\n{snippet}')

        prompt_template = load_prompt(self.prompt_path)
        prompt = prompt_template.format(
            query=query,
            generated_query=state.generated_query.strip() or query,
            context='\n\n---\n\n'.join(context_blocks),
        )

        response = await self.model.ainvoke(prompt)
        generation = str(response.content).strip()

        return {'generation': generation, 'current_state': self.name}
