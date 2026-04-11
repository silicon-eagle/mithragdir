from workflow.graph.state import GraphState
from workflow.nodes.final_check_node import FinalCheckNode


async def test_final_check_node_integration() -> None:
    node = FinalCheckNode()
    state = GraphState(
        query='Who is Gandalf?',
        generation='Gandalf is a wizard.',
    )

    result = await node(state)

    assert 'generation_grounded' in result
    assert 'generation_helpful' in result
    assert isinstance(result['generation_grounded'], bool)
    assert isinstance(result['generation_helpful'], bool)
    assert result['generation_grounded'] is False
    assert result['generation_helpful'] is False
