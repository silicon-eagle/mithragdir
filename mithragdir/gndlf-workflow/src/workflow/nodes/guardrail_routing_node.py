from typing import Literal

from langchain_core.prompts import load_prompt
from pydantic import BaseModel

from ..graph.state import GraphState
from .node import Node


class GuardrailRoutingOutput(BaseModel):
    guardrail_passed: bool
    route: Literal['conversational_llm', 'generate_query', 'refuse_answer']
    reason: str


class GuardrailRoutingNode(Node):
    def __init__(self) -> None:
        super().__init__('guardrail_routing')

    async def __call__(self, state: GraphState) -> dict[str, str | bool]:
        """Validates safety/domain and decides conversational vs retrieval route."""
        self.logger.info(f'Running {self.name} node')

        query = state.query.strip()
        if not query:
            raise ValueError("Node 'guardrail_routing' requires non-empty 'query'")

        prompt_template = load_prompt(self.prompt_path)
        prompt = prompt_template.format(query=query)

        structured_model = self.model.with_structured_output(GuardrailRoutingOutput)
        raw = await structured_model.ainvoke(prompt)
        result = raw if isinstance(raw, GuardrailRoutingOutput) else GuardrailRoutingOutput.model_validate(raw)

        # Force refusal route if guardrail fails.
        route = result.route if result.guardrail_passed else 'refuse_answer'
        refusal_reason = '' if result.guardrail_passed else result.reason.strip()

        return {
            'guardrail_passed': result.guardrail_passed,
            'route': route,
            'refusal_reason': refusal_reason,
            'current_state': self.name,
        }
