from workflow.graph.state import GraphState
from workflow.nodes.refuse_answer_node import RefuseAnswerNode


async def test_refuse_answer_node_integration() -> None:
    node = RefuseAnswerNode()
    state = GraphState(guardrail_passed=False)

    result = await node(state)

    assert 'refusal_reason' in result
    assert 'generation' in result
    assert isinstance(result['refusal_reason'], str)
    assert isinstance(result['generation'], str)
    assert result['refusal_reason'].strip()
    assert result['generation'].strip()
