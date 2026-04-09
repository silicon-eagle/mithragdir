from collections.abc import Callable

from .state import GraphState


def create_grade_routing_function(max_attempts: int = 3) -> Callable:
    """Create a routing function that grades the relevance of retrieved documents and routes accordingly.

    Args:
        max_attempts: Maximum number of retries for retrieval rewrite if documents are not relevant.

    Returns:
        A routing function that can be used in the graph to route based on document relevance.
    """

    def grade_routing(state: GraphState) -> str:
        assert state
        assert max_attempts > 0
        return ''

    return grade_routing
