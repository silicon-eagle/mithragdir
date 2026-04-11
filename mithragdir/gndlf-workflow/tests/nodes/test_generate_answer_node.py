from workflow.graph.state import GraphState
from workflow.nodes.generate_answer import GenerateAnswerNode


async def test_generate_answer_node_integration() -> None:
    node = GenerateAnswerNode()
    state = GraphState(
        query='Who is Gandalf?',
        generated_query='Gandalf identity',
    )

    result = await node(state)

    assert 'generation' in result
    assert isinstance(result['generation'], str)
    assert result['generation'].strip()
