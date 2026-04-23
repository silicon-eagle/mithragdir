from typing import Any

from langchain_core.prompts import load_prompt

from ..graph.state import GraphState
from .node import Node


class GenerateQueryNode(Node):
    def __init__(self) -> None:
        super().__init__('generate_query')

    async def __call__(self, state: GraphState) -> dict[str, Any]:
        """Rewrites the user's prompt into optimized search queries."""
        self.logger.info(f'Running {self.name} node')

        query = state.query.strip()
        if not query:
            raise ValueError("Node 'generate_query' requires non-empty 'query'")

        prompt_template = load_prompt(self.prompt_path)
        prompt = prompt_template.format(query=query)

        response = await self.model.ainvoke(prompt)
        generated_query = str(response.content).strip()
        if not generated_query:
            generated_query = query

        retry_count = state.retry_count + 1

        return {'generated_query': generated_query, 'retry_count': retry_count, 'current_state': self.name}
