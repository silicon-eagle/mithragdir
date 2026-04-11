from workflow.graph.state import GraphState
from workflow.nodes.route_query_node import RouteQueryNode


async def test_route_query_node_integration() -> None:
    node = RouteQueryNode()
    state = GraphState(query='Hello there!')

    result = await node(state)

    assert 'route' in result
    assert result['route'] in {'conversational', 'retrieval'}
    assert result['route'] == 'conversational'
