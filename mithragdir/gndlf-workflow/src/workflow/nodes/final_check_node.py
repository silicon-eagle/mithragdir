from typing import Any

from langchain_core.prompts import load_prompt
from pydantic import BaseModel

from ..graph.state import GraphState
from .node import Node


class GradeOutput(BaseModel):
    generation_grounded: bool
    generation_helpful: bool
    reason: str


class FinalCheckNode(Node):
    def __init__(self) -> None:
        super().__init__('grade_generation')

    async def __call__(self, state: GraphState) -> dict[str, Any]:
        """Checks the drafted answer for hallucinations and helpfulness."""
        self.logger.info(f'Running {self.name} node')

        query = state.query.strip()
        generation = state.generation.strip()
        if not query:
            raise ValueError("Node 'grade_generation' requires non-empty 'query'")
        if not generation:
            raise ValueError("Node 'grade_generation' requires non-empty 'generation'")

        if not state.documents:
            return {'generation_grounded': False, 'generation_helpful': False, 'current_state': self.name}

        context_blocks = []
        for document in state.documents:
            snippet = document.raw_content.strip()[:1600]
            context_blocks.append(f'Title: {document.title}\nContent:\n{snippet}')

        prompt_template = load_prompt(self.prompt_path)
        prompt = prompt_template.format(
            query=query,
            generation=generation,
            context='\n\n---\n\n'.join(context_blocks),
        )

        structured_model = self.model.with_structured_output(GradeOutput)
        raw = await structured_model.ainvoke(prompt)
        result = raw if isinstance(raw, GradeOutput) else GradeOutput.model_validate(raw)

        generation_grounded = result.generation_grounded
        generation_helpful = result.generation_helpful

        return {
            'generation_grounded': generation_grounded,
            'generation_helpful': generation_helpful,
            'current_state': self.name,
        }
