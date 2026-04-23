from typing import Any

from langchain_core.prompts import load_prompt

from ..graph.state import GraphState
from .node import Node


class ConversationalLLMNode(Node):
    def __init__(self) -> None:
        super().__init__('conversational_llm')

    async def __call__(self, state: GraphState) -> dict[str, Any]:
        """Handles general, non-lore chat without triggering the retrieval pipeline."""
        self.logger.info(f'Running {self.name} node')
        assert state

        if not state.query.strip():
            raise ValueError("Node 'conversational_llm' requires non-empty 'query'")

        prompt_template = load_prompt(self.prompt_path)
        prompt = prompt_template.format(query=state.query)

        response = await self.model.ainvoke(prompt)
        generation = str(response.content).strip()

        return {'generation': generation, 'current_state': self.name}
