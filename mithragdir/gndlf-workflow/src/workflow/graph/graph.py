from collections.abc import Callable

from langgraph.graph import END, StateGraph
from langgraph.graph.state import CompiledStateGraph

from ..nodes import (
    FinalCheckNode,
    GenerateAnswerNode,
    GenerateQueryNode,
    GuardrailRoutingNode,
    RefuseAnswerNode,
    RetrieveDocumentNode,
)
from .state import GraphState


def create_final_check_routing(max_attempts: int = 3) -> Callable[[GraphState], str]:
    """Create routing for answer self-correction and retry exhaustion handling."""

    def final_check_routing(state: GraphState) -> str:
        if state.generation_grounded is False:
            return 'generate_answer'
        if state.generation_helpful is False:
            if state.retry_count >= max_attempts:
                return 'refuse_answer'
            return 'generate_query'
        return END

    return final_check_routing


def compile_graph() -> CompiledStateGraph:
    """Compiles the execution graph for the Agentic RAG workflow."""
    builder = StateGraph(GraphState)

    # 1. Add Nodes
    builder.add_node('guardrail_routing', GuardrailRoutingNode())
    builder.add_node('generate_query', GenerateQueryNode())
    builder.add_node('retrieve_document', RetrieveDocumentNode())
    builder.add_node('generate_answer', GenerateAnswerNode())
    builder.add_node('grade_generation', FinalCheckNode())
    builder.add_node('refuse_answer', RefuseAnswerNode())

    # 2. Set Entry Point
    builder.set_entry_point('guardrail_routing')

    # 3. Add Edges & Conditional Routing
    # Guardrail routing validates safety/domain and routes to retrieval or refusal.
    # All passing queries go through retrieval (generate_query).
    builder.add_conditional_edges(
        'guardrail_routing',
        lambda state: 'refuse_answer' if state.guardrail_passed is False else 'generate_query',
    )

    # Retrieval flow sequence
    builder.add_edge('generate_query', 'retrieve_document')
    builder.add_edge('retrieve_document', 'generate_answer')

    # Generation flow sequence
    builder.add_edge('generate_answer', 'grade_generation')

    # Final check self-correction condition
    builder.add_conditional_edges(
        'grade_generation',
        create_final_check_routing(max_attempts=3),
    )

    # Refusal node ends the graph
    builder.add_edge('refuse_answer', END)

    return builder.compile()
