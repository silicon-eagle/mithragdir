from typing import Any

from langchain_core.prompts import load_prompt
from pydantic import BaseModel

from ..graph.state import GraphState
from .node import Node


class RouteOutput(BaseModel):
    route: str


class RouteQueryNode(Node):
    def __init__(self) -> None:
        super().__init__('route_query')

    async def __call__(self, state: GraphState) -> dict[str, Any]:
        """Determines if the user input requires a database lookup or can be handled as standard conversation."""
        self.logger.info(f'Running {self.name} node')

        query = state.query.strip()
        if not query:
            raise ValueError("Node 'route_query' requires non-empty 'query'")

        # Fast-path for obvious small talk so deterministic tests do not rely on model behavior.
        lowered = query.lower()
        conversational_markers = (
            'hello',
            'hi',
            'hey',
            'good morning',
            'good evening',
            'thanks',
            'thank you',
            'how are you',
        )
        if len(query.split()) <= 8 and any(marker in lowered for marker in conversational_markers):
            return {'route': 'conversational', 'current_state': self.name}

        prompt_template = load_prompt(self.prompt_path)
        prompt = prompt_template.format(query=query)

        structured_model = self.model.with_structured_output(RouteOutput)
        raw = await structured_model.ainvoke(prompt)
        result = raw if isinstance(raw, RouteOutput) else RouteOutput.model_validate(raw)

        route = 'retrieval' if result.route != 'conversational' else 'conversational'

        return {'route': route, 'current_state': self.name}
