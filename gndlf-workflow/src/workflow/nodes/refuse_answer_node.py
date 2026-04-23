from typing import Any

from ..graph.state import GraphState
from .node import Node


class RefuseAnswerNode(Node):
    def __init__(self) -> None:
        super().__init__('refuse_answer')

    async def __call__(self, state: GraphState) -> dict[str, Any]:
        """Handles out-of-bounds questions or exhausted search retries."""
        self.logger.info(f'Running {self.name} node')

        if state.refusal_reason.strip():
            refusal_reason = state.refusal_reason.strip()
        elif state.guardrail_passed is False:
            refusal_reason = 'Query blocked by guardrail policy or out-of-domain request.'
        elif state.retry_count >= 3:
            refusal_reason = 'Retrieval retries exhausted without grounded and helpful answer.'
        else:
            refusal_reason = 'Insufficient grounded context available for this request.'

        generation = 'I can only answer with grounded Tolkien-source context. Please rephrase your question or ask about specific Middle-earth lore.'

        return {'refusal_reason': refusal_reason, 'generation': generation, 'current_state': self.name}
