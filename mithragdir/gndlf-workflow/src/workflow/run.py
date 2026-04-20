from workflow.graph.graph import compile_graph
from workflow.graph.state import GraphState


async def run_graph(query: str) -> GraphState:
    """Run the workflow graph once for a single query and return final state."""
    normalized_query = query.strip()
    if not normalized_query:
        raise ValueError('query must be non-empty')

    graph = compile_graph()
    initial_state = GraphState(query=normalized_query)
    result = await graph.ainvoke(initial_state.model_dump())
    return GraphState.model_validate(result)


def extract_answer(state: GraphState) -> str:
    """Extract display-ready answer text from final graph state."""
    if state.generation.strip():
        return state.generation.strip()
    if state.refusal_reason.strip():
        return state.refusal_reason.strip()
    return 'No answer was generated.'
