from workflow.graph.state import GraphState
from workflow.nodes.generate_query_node import GenerateQueryNode


async def test_generate_query_node_integration() -> None:
    node = GenerateQueryNode()
    state = GraphState(query='Who forged the One Ring?')

    result = await node(state)

    assert 'generated_query' in result
    assert 'retry_count' in result
    assert isinstance(result['generated_query'], str)
    assert result['generated_query'].strip()
    assert result['retry_count'] == 1
